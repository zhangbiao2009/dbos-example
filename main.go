// Command dbos-example is an order-processing demo for DBOS Transact (Go).
//
// It shows, in one small program, the things that are hard to get right
// without a durable-execution engine:
//
//   - Crash recovery: kill the worker at any point (even kill -9) and the
//     in-flight orders resume from their last completed step on restart.
//   - Exactly-once steps: a payment that completed before a crash is never
//     charged again; its checkpointed output (the charge ID) is reused.
//   - Automatic step retries with backoff for a flaky payment gateway.
//   - Durable timers: a 20s "warehouse pickup" sleep survives restarts and only
//     waits for whatever time is left.
//   - Human-in-the-loop: large orders wait (durably, across restarts) for an
//     approve/reject message sent from a separate process.
//   - Compensation (saga): a rejected or unapproved order is refunded.
//   - Queues with concurrency limits: at most 2 orders are processed at once.
//   - Idempotency: submitting the same order ID twice never creates a duplicate.
//   - Introspection: progress events and the per-step checkpoint history.
//
// Usage:
//
//	go run . worker                   # run the order worker (Ctrl-C or kill -9 it any time)
//	go run . submit                   # enqueue the 4 demo orders
//	go run . submit <order-id> <usd>  # enqueue a custom order
//	go run . approve <order-id>       # approve an order waiting for review
//	go run . reject <order-id>        # reject an order waiting for review
//	go run . status                   # show every order, its progress, and its steps
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

const (
	appName       = "dbos-order-demo"
	queueName     = "orders"
	workflowName  = "process-order"
	approvalTopic = "approval"
	progressKey   = "progress"

	defaultDatabaseURL = "postgres://postgres:postgres@localhost:5433/dbos_demo?sslmode=disable"

	approvalThreshold = 500.0            // orders at or above this amount need a human decision
	approvalTimeout   = 5 * time.Minute  // auto-reject if nobody decides in time
	pickupDelay       = 20 * time.Second // durable sleep; a good window to kill the worker
	workerConcurrency = 2                // max orders processed concurrently by one worker
)

type Order struct {
	ID     string  `json:"id"`
	Item   string  `json:"item"`
	Amount float64 `json:"amount"`
}

type OrderResult struct {
	OrderID    string `json:"order_id"`
	Status     string `json:"status"` // "shipped" or "cancelled"
	ChargeID   string `json:"charge_id,omitempty"`
	TrackingNo string `json:"tracking_no,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

var demoOrders = []Order{
	{ID: "order-1001", Item: "mechanical keyboard", Amount: 129.00},
	{ID: "order-1002", Item: "4K monitor", Amount: 749.00},
	{ID: "order-1003", Item: "usb-c hub", Amount: 39.50},
	{ID: "order-1004", Item: "standing desk", Amount: 980.00},
}

// ---------------------------------------------------------------------------
// The workflow. Everything with side effects lives in a step, so DBOS can
// checkpoint its result and skip it on recovery.
// ---------------------------------------------------------------------------

func ProcessOrder(ctx dbos.Context, order Order) (OrderResult, error) {
	logf := func(format string, args ...any) {
		log.Printf("[%s] "+format, append([]any{order.ID}, args...)...)
	}
	progress := func(msg string) error {
		logf("progress: %s", msg)
		return dbos.SetEvent(ctx, progressKey, msg)
	}

	if err := progress("reserving inventory"); err != nil {
		return OrderResult{}, err
	}
	if _, err := dbos.RunAsStep(ctx, func(context.Context) (string, error) {
		logf("📦 EXECUTING reserve_inventory for %q", order.Item)
		return "reserved", nil
	}, dbos.WithStepName("reserve_inventory")); err != nil {
		return OrderResult{}, err
	}

	if err := progress("charging payment"); err != nil {
		return OrderResult{}, err
	}
	chargeID, err := dbos.RunAsStep(ctx, func(context.Context) (string, error) {
		return chargeCard(order, logf)
	},
		dbos.WithStepName("charge_payment"),
		dbos.WithStepMaxRetries(5),
		dbos.WithStepBaseInterval(time.Second),
		dbos.WithStepBackoffFactor(2),
	)
	if err != nil {
		return OrderResult{}, err
	}

	if order.Amount >= approvalThreshold {
		if err := progress(fmt.Sprintf("awaiting approval (run: go run . approve %s)", order.ID)); err != nil {
			return OrderResult{}, err
		}
		decision, err := dbos.Recv[string](ctx, approvalTopic, approvalTimeout)
		reason := ""
		switch {
		case errors.Is(err, dbos.ErrTimeout):
			reason = "no approval within " + approvalTimeout.String()
		case err != nil:
			return OrderResult{}, err
		case decision != "approve":
			reason = "rejected by reviewer"
		}
		if reason != "" {
			return compensate(ctx, order, chargeID, reason, progress, logf)
		}
		logf("✅ approved by reviewer")
	}

	if err := progress(fmt.Sprintf("waiting %s for warehouse pickup (durable sleep)", pickupDelay)); err != nil {
		return OrderResult{}, err
	}
	if _, err := dbos.Sleep(ctx, pickupDelay); err != nil {
		return OrderResult{}, err
	}

	trackingNo, err := dbos.RunAsStep(ctx, func(context.Context) (string, error) {
		tn := fmt.Sprintf("TRK-%06d", rand.IntN(1_000_000))
		logf("🚚 EXECUTING ship_order, tracking number %s", tn)
		return tn, nil
	}, dbos.WithStepName("ship_order"))
	if err != nil {
		return OrderResult{}, err
	}

	if err := progress("shipped " + trackingNo); err != nil {
		return OrderResult{}, err
	}
	return OrderResult{OrderID: order.ID, Status: "shipped", ChargeID: chargeID, TrackingNo: trackingNo}, nil
}

// compensate undoes the side effects of a cancelled order (saga pattern).
func compensate(ctx dbos.Context, order Order, chargeID, reason string,
	progress func(string) error, logf func(string, ...any)) (OrderResult, error) {
	if err := progress("cancelling: " + reason); err != nil {
		return OrderResult{}, err
	}
	if _, err := dbos.RunAsStep(ctx, func(context.Context) (string, error) {
		logf("↩️  EXECUTING refund_payment for charge %s ($%.2f)", chargeID, order.Amount)
		return "refunded", nil
	}, dbos.WithStepName("refund_payment")); err != nil {
		return OrderResult{}, err
	}
	if _, err := dbos.RunAsStep(ctx, func(context.Context) (string, error) {
		logf("↩️  EXECUTING release_inventory for %q", order.Item)
		return "released", nil
	}, dbos.WithStepName("release_inventory")); err != nil {
		return OrderResult{}, err
	}
	if err := progress("cancelled: " + reason); err != nil {
		return OrderResult{}, err
	}
	return OrderResult{OrderID: order.ID, Status: "cancelled", ChargeID: chargeID, Reason: reason}, nil
}

// chargeCard simulates a flaky payment gateway: the first two attempts for
// each order (per worker process) time out, then it succeeds. DBOS retries the
// step automatically, and once it succeeds the charge is never repeated.
var (
	gatewayMu       sync.Mutex
	gatewayAttempts = map[string]int{}
)

func chargeCard(order Order, logf func(string, ...any)) (string, error) {
	gatewayMu.Lock()
	gatewayAttempts[order.ID]++
	attempt := gatewayAttempts[order.ID]
	gatewayMu.Unlock()

	if attempt <= 2 {
		logf("💳 charge attempt %d: payment gateway timeout (DBOS will retry)", attempt)
		return "", fmt.Errorf("payment gateway timeout (attempt %d)", attempt)
	}
	chargeID := fmt.Sprintf("ch_%08x", rand.Uint32())
	logf("💳 EXECUTING charge_payment: charged $%.2f, charge id %s", order.Amount, chargeID)
	return chargeID, nil
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

func main() {
	log.SetFlags(log.Ltime)
	if len(os.Args) < 2 {
		usage()
	}

	var err error
	switch cmd, args := os.Args[1], os.Args[2:]; cmd {
	case "worker":
		err = runWorker()
	case "submit":
		err = withClient(func(c dbos.Client) error { return submit(c, args) })
	case "approve", "reject":
		if len(args) != 1 {
			usage()
		}
		err = withClient(func(c dbos.Client) error { return decide(c, args[0], cmd) })
	case "status":
		err = withClient(status)
	default:
		usage()
	}
	if err != nil {
		log.Fatal(err)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  go run . worker                   run the order worker
  go run . submit                   enqueue the demo orders
  go run . submit <order-id> <usd>  enqueue a custom order
  go run . approve <order-id>       approve an order awaiting review
  go run . reject <order-id>        reject an order awaiting review
  go run . status                   show orders, progress and steps`)
	os.Exit(2)
}

func databaseURL() string {
	if u := os.Getenv("DBOS_SYSTEM_DATABASE_URL"); u != "" {
		return u
	}
	return defaultDatabaseURL
}

func runWorker() error {
	ctx, err := dbos.NewContext(context.Background(), dbos.Config{
		AppName:     appName,
		DatabaseURL: databaseURL(),
		// Pin the version so workflows started by an earlier build of this demo
		// are still recovered after you edit the code.
		ApplicationVersion: "v1",
	})
	if err != nil {
		return fmt.Errorf("create DBOS context: %w", err)
	}

	dbos.RegisterWorkflow(ctx, ProcessOrder, dbos.WithWorkflowName(workflowName))
	if _, err := dbos.RegisterQueue(ctx, queueName, dbos.WithWorkerConcurrency(workerConcurrency)); err != nil {
		return fmt.Errorf("register queue: %w", err)
	}

	// Launch also recovers any workflows this executor left PENDING when it died.
	if err := dbos.Launch(ctx); err != nil {
		return fmt.Errorf("launch DBOS: %w", err)
	}
	log.Printf("worker running (queue %q, concurrency %d). Ctrl-C or kill -9 at any time; restart to resume.",
		queueName, workerConcurrency)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Printf("shutting down; in-flight orders will resume on next start")
	return dbos.Shutdown(ctx, 5*time.Second)
}

func withClient(fn func(dbos.Client) error) error {
	client, err := dbos.NewClient(context.Background(), dbos.ClientConfig{
		AppName:     appName,
		DatabaseURL: databaseURL(),
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
	orders := demoOrders
	switch len(args) {
	case 0:
	case 2:
		amount, err := strconv.ParseFloat(args[1], 64)
		if err != nil {
			return fmt.Errorf("invalid amount %q: %w", args[1], err)
		}
		orders = []Order{{ID: args[0], Item: "custom item", Amount: amount}}
	default:
		usage()
	}

	for _, o := range orders {
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
		fmt.Printf("enqueued %s: %s $%.2f%s\n", o.ID, o.Item, o.Amount, note)
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
			fmt.Fprintf(tw, "  #%d\t%s\t%s\n", s.StepID, s.StepName, out)
		}
		tw.Flush()
	}
	return nil
}
