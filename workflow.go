package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

const (
	approvalTopic = "approval"
	progressKey   = "progress"

	approvalThreshold = 500.0            // orders at or above this amount need a human decision
	approvalTimeout   = 5 * time.Minute  // auto-reject if nobody decides in time
	pickupDelay       = 20 * time.Second // durable sleep; a good window to kill the worker
)

type Order struct {
	ID     string  `json:"id"`
	SKU    string  `json:"sku"`
	Amount float64 `json:"amount"`
}

type OrderResult struct {
	OrderID    string `json:"order_id"`
	Status     string `json:"status"` // "shipped" or "cancelled"
	ChargeID   string `json:"charge_id,omitempty"`
	TrackingNo string `json:"tracking_no,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

// Set up by runWorker before DBOS is launched.
var (
	shopDB  *dbos.DataSource // the application's own database (inventory, orders)
	gateway *gatewayClient   // HTTP client for the external payment provider
)

// ProcessOrder is the durable workflow. It may be re-executed from the top
// after a crash; every interaction with the outside world goes through a DBOS
// call so that completed work is returned from its checkpoint, not redone:
//
//   - dbos.RunAsTransaction: writes to our own database. The writes and the
//     DBOS checkpoint commit in one transaction, so they happen exactly once.
//   - dbos.RunAsStep: calls to an external system (the payment provider).
//     The step re-runs only if it crashed before finishing, and the
//     idempotency key makes that re-run harmless.
//   - dbos.Recv / dbos.Sleep / dbos.SetEvent: durable input, timers, status.
func ProcessOrder(ctx dbos.Context, order Order) (OrderResult, error) {
	logf := func(format string, args ...any) {
		log.Printf("[%s] "+format, append([]any{order.ID}, args...)...)
	}
	progress := func(msg string) error {
		logf("progress: %s", msg)
		return dbos.SetEvent(ctx, progressKey, msg)
	}

	// 1. Reserve stock in our database (exactly once).
	if err := progress("reserving inventory"); err != nil {
		return OrderResult{}, err
	}
	reserved, err := dbos.RunAsTransaction(ctx, shopDB, reserveInventory(order))
	if err != nil {
		return OrderResult{}, err
	}
	if !reserved {
		if err := progress("cancelled: out of stock"); err != nil {
			return OrderResult{}, err
		}
		return OrderResult{OrderID: order.ID, Status: "cancelled", Reason: "out of stock"}, nil
	}

	// 2. Charge the card through the external payment API, retrying on failure.
	if err := progress("charging payment"); err != nil {
		return OrderResult{}, err
	}
	chargeID, err := dbos.RunAsStep(ctx, func(stepCtx context.Context) (string, error) {
		// Same key on every attempt and every re-run: the provider charges at most once.
		return gateway.Charge(stepCtx, order.ID+":charge", order)
	},
		dbos.WithStepName("charge_payment"),
		dbos.WithStepMaxRetries(5),
		dbos.WithStepBaseInterval(time.Second),
		dbos.WithStepBackoffFactor(2),
	)
	if err != nil {
		logf("payment failed permanently: %v", err)
		return cancelOrder(ctx, order, "", "payment failed", progress)
	}

	// 3. Big orders wait (durably) for a human decision sent from another process.
	if order.Amount >= approvalThreshold {
		if err := progress(fmt.Sprintf("awaiting approval (run: go run . approve %s)", order.ID)); err != nil {
			return OrderResult{}, err
		}
		decision, err := dbos.Recv[string](ctx, approvalTopic, approvalTimeout)
		switch {
		case errors.Is(err, dbos.ErrTimeout):
			return cancelOrder(ctx, order, chargeID, "no approval within "+approvalTimeout.String(), progress)
		case err != nil:
			return OrderResult{}, err
		case decision != "approve":
			return cancelOrder(ctx, order, chargeID, "rejected by reviewer", progress)
		}
		logf("✅ approved by reviewer")
	}

	// 4. Durable timer: survives restarts, resumes with only the remaining time.
	if err := progress(fmt.Sprintf("waiting %s for warehouse pickup (durable sleep)", pickupDelay)); err != nil {
		return OrderResult{}, err
	}
	if _, err := dbos.Sleep(ctx, pickupDelay); err != nil {
		return OrderResult{}, err
	}

	// 5. Record the shipment in our database (exactly once).
	trackingNo, err := dbos.RunAsTransaction(ctx, shopDB, shipOrder(order, chargeID))
	if err != nil {
		return OrderResult{}, err
	}
	if err := progress("shipped " + trackingNo); err != nil {
		return OrderResult{}, err
	}
	return OrderResult{OrderID: order.ID, Status: "shipped", ChargeID: chargeID, TrackingNo: trackingNo}, nil
}

// cancelOrder undoes what the order already did (saga compensation):
// refund the charge, if any, and put the stock back.
func cancelOrder(ctx dbos.Context, order Order, chargeID, reason string, progress func(string) error) (OrderResult, error) {
	if err := progress("cancelling: " + reason); err != nil {
		return OrderResult{}, err
	}
	if chargeID != "" {
		if _, err := dbos.RunAsStep(ctx, func(stepCtx context.Context) (string, error) {
			return "refunded", gateway.Refund(stepCtx, chargeID)
		}, dbos.WithStepName("refund_payment"), dbos.WithStepMaxRetries(5)); err != nil {
			return OrderResult{}, err
		}
	}
	if _, err := dbos.RunAsTransaction(ctx, shopDB, releaseInventory(order, chargeID, reason)); err != nil {
		return OrderResult{}, err
	}
	if err := progress("cancelled: " + reason); err != nil {
		return OrderResult{}, err
	}
	return OrderResult{OrderID: order.ID, Status: "cancelled", ChargeID: chargeID, Reason: reason}, nil
}
