package main

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"text/tabwriter"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The application's own database ("shop"), separate from the DBOS system
// database. The payment provider's ledger (schema "gateway") lives here too
// only to keep the demo to one Postgres server; the workflow never touches it
// directly, only through the provider's HTTP API.

type product struct {
	SKU          string
	Name         string
	Price        float64
	InitialStock int
}

var catalog = []product{
	{SKU: "KB-01", Name: "mechanical keyboard", Price: 129.00, InitialStock: 10},
	{SKU: "MON-4K", Name: "4K monitor", Price: 749.00, InitialStock: 5},
	{SKU: "HUB-C", Name: "usb-c hub", Price: 39.50, InitialStock: 20},
	{SKU: "DESK-S", Name: "standing desk", Price: 980.00, InitialStock: 3},
	{SKU: "GPU-X", Name: "graphics card", Price: 399.00, InitialStock: 0}, // sold out
}

func findProduct(sku string) (product, bool) {
	for _, p := range catalog {
		if p.SKU == sku {
			return p, true
		}
	}
	return product{}, false
}

const shopSchema = `
CREATE SCHEMA IF NOT EXISTS shop;
CREATE TABLE IF NOT EXISTS shop.inventory (
	sku   TEXT PRIMARY KEY,
	name  TEXT NOT NULL,
	price NUMERIC(10,2) NOT NULL,
	stock INT NOT NULL CHECK (stock >= 0)
);
CREATE TABLE IF NOT EXISTS shop.orders (
	order_id    TEXT PRIMARY KEY,
	sku         TEXT NOT NULL REFERENCES shop.inventory(sku),
	amount      NUMERIC(10,2) NOT NULL,
	status      TEXT NOT NULL,
	charge_id   TEXT,
	tracking_no TEXT,
	note        TEXT,
	updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE SCHEMA IF NOT EXISTS gateway;
CREATE TABLE IF NOT EXISTS gateway.charges (
	charge_id       TEXT PRIMARY KEY,
	idempotency_key TEXT NOT NULL UNIQUE,
	order_id        TEXT NOT NULL,
	amount          NUMERIC(10,2) NOT NULL,
	status          TEXT NOT NULL,
	created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);`

// openShopDB creates the shop database if needed, applies the schema, seeds
// the catalog, and returns a pool to it.
func openShopDB(ctx context.Context, systemURL, appURL string) (*pgxpool.Pool, error) {
	cfg, err := pgx.ParseConfig(appURL)
	if err != nil {
		return nil, fmt.Errorf("parse APP_DATABASE_URL: %w", err)
	}
	admin, err := pgx.Connect(ctx, systemURL)
	if err != nil {
		return nil, fmt.Errorf("connect to system database: %w", err)
	}
	defer admin.Close(ctx)
	var exists bool
	if err := admin.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, cfg.Database).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{cfg.Database}.Sanitize()); err != nil {
			return nil, fmt.Errorf("create database %s: %w", cfg.Database, err)
		}
		log.Printf("created application database %q", cfg.Database)
	}

	pool, err := pgxpool.New(ctx, appURL)
	if err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, shopSchema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply shop schema: %w", err)
	}
	for _, p := range catalog {
		if _, err := pool.Exec(ctx,
			`INSERT INTO shop.inventory (sku, name, price, stock) VALUES ($1, $2, $3, $4) ON CONFLICT (sku) DO NOTHING`,
			p.SKU, p.Name, p.Price, p.InitialStock); err != nil {
			pool.Close()
			return nil, fmt.Errorf("seed inventory: %w", err)
		}
	}
	return pool, nil
}

// --- Transactions run with dbos.RunAsTransaction -----------------------------
//
// Each function's writes commit atomically with a DBOS completion record, so
// even a crash right after COMMIT never makes them run a second time. The plain
// INSERTs (no ON CONFLICT) are deliberate: a double execution would fail loudly
// on the primary key instead of silently succeeding.

func reserveInventory(o Order) dbos.Txn[bool] {
	return func(ctx context.Context, tx dbos.Tx) (bool, error) {
		res, err := tx.Exec(ctx, `UPDATE shop.inventory SET stock = stock - 1 WHERE sku = $1 AND stock > 0`, o.SKU)
		if err != nil {
			return false, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return false, err
		}
		status := "reserved"
		if n == 0 {
			status = "out_of_stock"
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO shop.orders (order_id, sku, amount, status) VALUES ($1, $2, $3, $4)`,
			o.ID, o.SKU, o.Amount, status); err != nil {
			return false, err
		}
		log.Printf("[%s] 🗄️  TX reserve_inventory: %s -> %s", o.ID, o.SKU, status)
		return n == 1, nil
	}
}

func shipOrder(o Order, chargeID string) dbos.Txn[string] {
	return func(ctx context.Context, tx dbos.Tx) (string, error) {
		trackingNo := fmt.Sprintf("TRK-%06d", rand.IntN(1_000_000))
		if _, err := tx.Exec(ctx,
			`UPDATE shop.orders SET status = 'shipped', charge_id = $2, tracking_no = $3, updated_at = now() WHERE order_id = $1`,
			o.ID, chargeID, trackingNo); err != nil {
			return "", err
		}
		log.Printf("[%s] 🗄️  TX ship_order: tracking %s", o.ID, trackingNo)
		return trackingNo, nil
	}
}

func releaseInventory(o Order, chargeID, reason string) dbos.Txn[string] {
	return func(ctx context.Context, tx dbos.Tx) (string, error) {
		if _, err := tx.Exec(ctx, `UPDATE shop.inventory SET stock = stock + 1 WHERE sku = $1`, o.SKU); err != nil {
			return "", err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE shop.orders SET status = 'cancelled', charge_id = NULLIF($2, ''), note = $3, updated_at = now() WHERE order_id = $1`,
			o.ID, chargeID, reason); err != nil {
			return "", err
		}
		log.Printf("[%s] 🗄️  TX release_inventory: %s +1", o.ID, o.SKU)
		return "released", nil
	}
}

// --- report command ----------------------------------------------------------

// report prints the business data and checks the invariants a crash-unsafe
// implementation would break: no order charged twice, and stock that adds up.
func report(ctx context.Context, appURL string) error {
	pool, err := pgxpool.New(ctx, appURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	printRows := func(title, query string, cols int) error {
		rows, err := pool.Query(ctx, query)
		if err != nil {
			return fmt.Errorf("%s (has the worker been started once?): %w", title, err)
		}
		defer rows.Close()
		fmt.Fprintf(tw, "\n%s\n", title)
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				return err
			}
			for i := 0; i < cols; i++ {
				fmt.Fprintf(tw, "  %v\t", vals[i])
			}
			fmt.Fprintln(tw)
		}
		return rows.Err()
	}

	if err := printRows("INVENTORY (sku, name, stock)",
		`SELECT sku, name, stock FROM shop.inventory ORDER BY sku`, 3); err != nil {
		return err
	}
	if err := printRows("ORDERS (order, sku, status, charge, tracking, note)",
		`SELECT order_id, sku, status, coalesce(charge_id,'-'), coalesce(tracking_no,'-'), coalesce(note,'') FROM shop.orders ORDER BY order_id`, 6); err != nil {
		return err
	}
	if err := printRows("PAYMENT PROVIDER LEDGER (charge, order, amount, status)",
		`SELECT charge_id, order_id, amount::text, status FROM gateway.charges ORDER BY order_id`, 4); err != nil {
		return err
	}
	tw.Flush()

	fmt.Println("\nCHECKS")
	var doubleCharged int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM (SELECT order_id FROM gateway.charges GROUP BY order_id HAVING count(*) > 1) d`).Scan(&doubleCharged); err != nil {
		return err
	}
	check(doubleCharged == 0, fmt.Sprintf("no order charged more than once (%d double-charged)", doubleCharged))

	for _, p := range catalog {
		var stock, holding int
		if err := pool.QueryRow(ctx, `SELECT stock FROM shop.inventory WHERE sku = $1`, p.SKU).Scan(&stock); err != nil {
			return err
		}
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM shop.orders WHERE sku = $1 AND status IN ('reserved', 'shipped')`, p.SKU).Scan(&holding); err != nil {
			return err
		}
		check(stock+holding == p.InitialStock,
			fmt.Sprintf("%s stock adds up: %d in stock + %d reserved/shipped = %d initial", p.SKU, stock, holding, p.InitialStock))
	}
	return nil
}

func check(ok bool, msg string) {
	mark := "✅"
	if !ok {
		mark = "❌"
	}
	fmt.Printf("  %s %s\n", mark, msg)
}
