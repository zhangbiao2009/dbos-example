// Command dbos-example is an order-processing demo for DBOS Transact (Go).
//
// Each order reserves stock in a real Postgres database, charges a card through
// an (fake, flaky) HTTP payment provider, may wait for a human approval, sleeps
// durably, and records the shipment. Kill the worker at any point and restart
// it: `go run . report` then proves nothing was lost or done twice.
//
// Files:
//
//	workflow.go  the durable workflow (ProcessOrder) and its compensation
//	shop.go      the application database: schema, transactions, report
//	gateway.go   the fake third-party payment provider and its HTTP client
//	main.go      commands and wiring
//
// Usage:
//
//	go run . worker [--crash-after-charge]  run the worker (Ctrl-C or kill -9 it any time)
//	go run . submit                         enqueue the demo orders
//	go run . submit <order-id> <sku>        enqueue a custom order (sku from the catalog)
//	go run . approve|reject <order-id>      decide on an order awaiting approval
//	go run . status                         workflow progress and per-step checkpoints
//	go run . report                         business data + exactly-once checks
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

const (
	appName           = "dbos-order-demo"
	queueName         = "orders"
	workflowName      = "process-order"
	workerConcurrency = 3 // max orders processed concurrently by one worker

	defaultSystemDatabaseURL = "postgres://postgres:postgres@localhost:5433/dbos_demo?sslmode=disable"
	defaultAppDatabaseURL    = "postgres://postgres:postgres@localhost:5433/shop?sslmode=disable"
	defaultGatewayAddr       = "127.0.0.1:8089"
)

var demoOrders = []struct{ ID, SKU string }{
	{"order-1001", "KB-01"},
	{"order-1002", "MON-4K"}, // >= $500: needs approval
	{"order-1003", "HUB-C"},
	{"order-1004", "DESK-S"}, // >= $500: needs approval
	{"order-1005", "GPU-X"},  // sold out: cancelled without charging
}

func main() {
	log.SetFlags(log.Ltime)
	if len(os.Args) < 2 {
		usage()
	}

	var err error
	switch cmd, args := os.Args[1], os.Args[2:]; cmd {
	case "worker":
		crash := len(args) == 1 && args[0] == "--crash-after-charge"
		if len(args) > 0 && !crash {
			usage()
		}
		err = runWorker(crash)
	case "submit":
		err = withClient(func(c dbos.Client) error { return submit(c, args) })
	case "approve", "reject":
		if len(args) != 1 {
			usage()
		}
		err = withClient(func(c dbos.Client) error { return decide(c, args[0], cmd) })
	case "status":
		err = withClient(status)
	case "report":
		err = report(context.Background(), env("APP_DATABASE_URL", defaultAppDatabaseURL))
	default:
		usage()
	}
	if err != nil {
		log.Fatal(err)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  go run . worker [--crash-after-charge]  run the order worker
  go run . submit                         enqueue the demo orders
  go run . submit <order-id> <sku>        enqueue a custom order
  go run . approve <order-id>             approve an order awaiting review
  go run . reject <order-id>              reject an order awaiting review
  go run . status                         workflow progress and steps
  go run . report                         business data and exactly-once checks`)
	os.Exit(2)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func runWorker(crashAfterCharge bool) error {
	bg := context.Background()
	systemURL := env("DBOS_SYSTEM_DATABASE_URL", defaultSystemDatabaseURL)
	gatewayAddr := env("GATEWAY_ADDR", defaultGatewayAddr)

	appPool, err := openShopDB(bg, systemURL, env("APP_DATABASE_URL", defaultAppDatabaseURL))
	if err != nil {
		return err
	}
	defer appPool.Close()
	if err := startPaymentProvider(gatewayAddr, appPool); err != nil {
		return err
	}
	gateway = newGatewayClient(gatewayAddr, crashAfterCharge)

	ctx, err := dbos.NewContext(bg, dbos.Config{
		AppName:     appName,
		DatabaseURL: systemURL,
		// Pin the version so workflows started by an earlier build of this demo
		// are still recovered after you edit the code. In production, leave it
		// unset: DBOS then only recovers workflows with the code that started them.
		ApplicationVersion: "v1",
	})
	if err != nil {
		return fmt.Errorf("create DBOS context: %w", err)
	}

	// Lets RunAsTransaction commit shop-table writes and its checkpoint atomically.
	if shopDB, err = dbos.NewDataSource(ctx, appPool, dbos.WithDataSourceName("shop")); err != nil {
		return fmt.Errorf("create data source: %w", err)
	}
	dbos.RegisterWorkflow(ctx, ProcessOrder, dbos.WithWorkflowName(workflowName))
	if _, err := dbos.RegisterQueue(ctx, queueName, dbos.WithWorkerConcurrency(workerConcurrency)); err != nil {
		return fmt.Errorf("register queue: %w", err)
	}

	// Launch also recovers any workflows this executor left PENDING when it died.
	if err := dbos.Launch(ctx); err != nil {
		return fmt.Errorf("launch DBOS: %w", err)
	}
	log.Printf("worker running (queue %q, concurrency %d, payment provider on %s). Ctrl-C or kill -9 any time.",
		queueName, workerConcurrency, gatewayAddr)
	if crashAfterCharge {
		log.Printf("--crash-after-charge: the worker will die right after the first successful card charge")
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Printf("shutting down; in-flight orders will resume on next start")
	return dbos.Shutdown(ctx, 5*time.Second)
}

func withClient(fn func(dbos.Client) error) error {
	client, err := dbos.NewClient(context.Background(), dbos.ClientConfig{
		AppName:     appName,
		DatabaseURL: env("DBOS_SYSTEM_DATABASE_URL", defaultSystemDatabaseURL),
		// Keep the CLI output readable; every client call still returns its errors.
		Logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1})),
	})
	if err != nil {
		return fmt.Errorf("connect to DBOS system database (has the worker been started once?): %w", err)
	}
	defer dbos.Shutdown(client, 5*time.Second)
	return fn(client)
}

func submit(c dbos.Client, args []string) error {
	requested := demoOrders
	switch len(args) {
	case 0:
	case 2:
		requested = []struct{ ID, SKU string }{{args[0], args[1]}}
	default:
		usage()
	}

	for _, r := range requested {
		p, ok := findProduct(r.SKU)
		if !ok {
			return fmt.Errorf("unknown sku %q", r.SKU)
		}
		o := Order{ID: r.ID, SKU: p.SKU, Amount: p.Price}

		existing, err := dbos.ListWorkflows(c, dbos.WithFilterWorkflowIDs(o.ID))
		if err != nil {
			return err
		}
		// The order ID is the workflow ID, so enqueueing it again is a no-op
		// that returns the existing workflow: no duplicate orders, no double charges.
		if _, err := dbos.Enqueue[OrderResult](c, queueName, workflowName, o,
			dbos.WithEnqueueWorkflowID(o.ID)); err != nil {
			return fmt.Errorf("enqueue %s: %w", o.ID, err)
		}
		if len(existing) > 0 {
			fmt.Printf("%s already exists (%s), not duplicated\n", o.ID, existing[0].Status)
			continue
		}
		note := ""
		if o.Amount >= approvalThreshold {
			note = "  (needs approval)"
		}
		fmt.Printf("enqueued %s: %s $%.2f%s\n", o.ID, p.Name, o.Amount, note)
	}
	return nil
}

func decide(c dbos.Client, orderID, decision string) error {
	if err := dbos.Send(c, orderID, decision, approvalTopic); err != nil {
		return fmt.Errorf("send %s to %s: %w", decision, orderID, err)
	}
	fmt.Printf("sent %q to %s\n", decision, orderID)
	return nil
}

func status(c dbos.Client) error {
	wfs, err := dbos.ListWorkflows(c, dbos.WithFilterName(workflowName))
	if err != nil {
		return err
	}
	if len(wfs) == 0 {
		fmt.Println("no orders yet; run: go run . submit")
		return nil
	}

	for _, wf := range wfs {
		prog, err := dbos.GetEvent[string](c, wf.ID, progressKey, 50*time.Millisecond)
		if err != nil && !errors.Is(err, dbos.ErrTimeout) {
			return err
		}
		fmt.Printf("\n%s  status=%s  attempts=%d  progress=%q\n", wf.ID, wf.Status, wf.Attempts, prog)

		steps, err := dbos.GetWorkflowSteps(c, wf.ID, dbos.WithStepsLoadOutput(true))
		if err != nil {
			return err
		}
		tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		for _, s := range steps {
			out := ""
			if s.Output != nil {
				out = fmt.Sprint(s.Output)
			}
			if s.Error != nil {
				out = "error: " + s.Error.Error()
			}
			fmt.Fprintf(tw, "  #%d\t%s\t%s\n", s.StepID, prettyStepName(s.StepName), out)
		}
		tw.Flush()
	}
	return nil
}

// prettyStepName turns the Go function names DBOS records for transactions
// (e.g. "main.ProcessOrder.reserveInventory.func1") into "tx:reserveInventory".
func prettyStepName(name string) string {
	if !strings.HasPrefix(name, "main.") {
		return name
	}
	if i := strings.Index(name, ".func"); i > 0 {
		name = name[:i]
	}
	return "tx:" + name[strings.LastIndex(name, ".")+1:]
}
