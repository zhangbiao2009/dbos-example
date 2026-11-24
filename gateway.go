package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// A fake third-party payment provider, served over HTTP from inside the worker
// process for convenience. It behaves like real providers (Stripe etc.):
//
//   - it is flaky: the first 2 requests for each idempotency key fail with 503;
//   - it is idempotent: a repeated request with the same Idempotency-Key returns
//     the original charge instead of charging again.
//
// Its ledger lives in the gateway.charges table, which the workflow never
// touches directly.

type chargeRequest struct {
	IdempotencyKey string  `json:"idempotency_key"`
	OrderID        string  `json:"order_id"`
	Amount         float64 `json:"amount"`
}

type chargeResponse struct {
	ChargeID  string `json:"charge_id"`
	Duplicate bool   `json:"duplicate"`
}

type refundRequest struct {
	ChargeID string `json:"charge_id"`
}

type paymentProvider struct {
	pool     *pgxpool.Pool
	mu       sync.Mutex
	attempts map[string]int
}

func startPaymentProvider(addr string, pool *pgxpool.Pool) error {
	p := &paymentProvider{pool: pool, attempts: map[string]int{}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /charge", p.charge)
	mux.HandleFunc("POST /refund", p.refund)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("start payment provider on %s: %w", addr, err)
	}
	go func() {
		if err := http.Serve(ln, mux); err != nil {
			log.Printf("payment provider stopped: %v", err)
		}
	}()
	return nil
}

func (p *paymentProvider) charge(w http.ResponseWriter, r *http.Request) {
	var req chargeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.IdempotencyKey == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	p.mu.Lock()
	p.attempts[req.IdempotencyKey]++
	attempt := p.attempts[req.IdempotencyKey]
	p.mu.Unlock()
	if attempt <= 2 {
		log.Printf("[gateway] 503 for %s (simulated outage, attempt %d)", req.OrderID, attempt)
		http.Error(w, "gateway timeout", http.StatusServiceUnavailable)
		return
	}

	newID := fmt.Sprintf("ch_%08x", rand.Uint32())
	tag, err := p.pool.Exec(r.Context(),
		`INSERT INTO gateway.charges (charge_id, idempotency_key, order_id, amount, status)
		 VALUES ($1, $2, $3, $4, 'charged') ON CONFLICT (idempotency_key) DO NOTHING`,
		newID, req.IdempotencyKey, req.OrderID, req.Amount)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := chargeResponse{ChargeID: newID, Duplicate: tag.RowsAffected() == 0}
	if resp.Duplicate {
		if err := p.pool.QueryRow(r.Context(),
			`SELECT charge_id FROM gateway.charges WHERE idempotency_key = $1`, req.IdempotencyKey).Scan(&resp.ChargeID); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		log.Printf("[gateway] duplicate request for %s: returning existing %s, NOT charging again", req.OrderID, resp.ChargeID)
	} else {
		log.Printf("[gateway] 💳 charged $%.2f for %s -> %s", req.Amount, req.OrderID, resp.ChargeID)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (p *paymentProvider) refund(w http.ResponseWriter, r *http.Request) {
	var req refundRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ChargeID == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	// Refunding an already-refunded charge is a no-op, so retries are safe.
	tag, err := p.pool.Exec(r.Context(),
		`UPDATE gateway.charges SET status = 'refunded' WHERE charge_id = $1 AND status = 'charged'`, req.ChargeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if tag.RowsAffected() == 1 {
		log.Printf("[gateway] ↩️  refunded %s", req.ChargeID)
	}
	w.WriteHeader(http.StatusOK)
}

// gatewayClient is what the workflow's steps use to talk to the provider.
type gatewayClient struct {
	baseURL          string
	http             *http.Client
	crashAfterCharge bool // demo: die after a real charge, before DBOS checkpoints the step
}

func newGatewayClient(addr string, crashAfterCharge bool) *gatewayClient {
	return &gatewayClient{
		baseURL:          "http://" + addr,
		http:             &http.Client{Timeout: 5 * time.Second},
		crashAfterCharge: crashAfterCharge,
	}
}

func (g *gatewayClient) Charge(ctx context.Context, idempotencyKey string, o Order) (string, error) {
	var resp chargeResponse
	if err := g.post(ctx, "/charge", chargeRequest{IdempotencyKey: idempotencyKey, OrderID: o.ID, Amount: o.Amount}, &resp); err != nil {
		return "", err
	}
	if g.crashAfterCharge && !resp.Duplicate {
		log.Printf("[%s] 💥 CRASHING: card charged (%s) but the step has not returned, so DBOS has no checkpoint for it",
			o.ID, resp.ChargeID)
		os.Exit(1)
	}
	return resp.ChargeID, nil
}

func (g *gatewayClient) Refund(ctx context.Context, chargeID string) error {
	return g.post(ctx, "/refund", refundRequest{ChargeID: chargeID}, nil)
}

func (g *gatewayClient) post(ctx context.Context, path string, body, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.baseURL+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := g.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New("payment provider: " + resp.Status)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
