package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	dbos "github.com/dbos-inc/dbos-transact-golang/dbos"
)

type DemoInput struct {
	Name string `json:"name"`
}

type DemoOutput struct {
	Message     string    `json:"message"`
	CompletedAt time.Time `json:"completed_at"`
}

var durableHello = dbos.Workflow[DemoInput, DemoOutput](DurableHello)

func DurableHello(ctx dbos.DBOSContext, input DemoInput) (DemoOutput, error) {
	workflowID, err := dbos.GetWorkflowID(ctx)
	if err != nil {
		return DemoOutput{}, err
	}

	_, err = dbos.RunAsStep(ctx, func(stepCtx context.Context) (string, error) {
		log.Printf("[workflow %s] step1: preparing greeting for %q", workflowID, input.Name)
		return "prepared", nil
	}, dbos.WithStepName("step1_prepare"))
	if err != nil {
		return DemoOutput{}, err
	}

	_, err = dbos.Sleep(ctx, 10*time.Second)
	if err != nil {
		return DemoOutput{}, err
	}

	message, err := dbos.RunAsStep(ctx, func(stepCtx context.Context) (string, error) {
		log.Printf("[workflow %s] step2: finalizing greeting", workflowID)
		return fmt.Sprintf("hello %s from a durable workflow", input.Name), nil
	}, dbos.WithStepName("step2_finalize"))
	if err != nil {
		return DemoOutput{}, err
	}

	return DemoOutput{
		Message:     message,
		CompletedAt: time.Now().UTC(),
	}, nil
}

func main() {
	databaseURL := os.Getenv("DBOS_SYSTEM_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@localhost:5432/dbos_demo?sslmode=disable"
	}

	dbosCtx, err := dbos.NewDBOSContext(context.Background(), dbos.Config{
		AppName:     "go-durable-demo",
		DatabaseURL: databaseURL,
	})
	if err != nil {
		log.Fatalf("failed to create DBOS context: %v", err)
	}

	dbos.RegisterWorkflow(dbosCtx, durableHello)

	if err := dbosCtx.Launch(); err != nil {
		log.Fatalf("failed to launch DBOS: %v", err)
	}
	defer dbosCtx.Shutdown(10 * time.Second)

	const workflowID = "durable-hello-demo"

	handle, err := dbos.RunWorkflow(
		dbosCtx,
		durableHello,
		DemoInput{Name: "DBOS"},
		dbos.WithWorkflowID(workflowID),
	)
	if err != nil {
		log.Fatalf("failed to start workflow: %v", err)
	}

	if os.Getenv("CRASH_ONCE") == "1" {
		log.Printf("workflow %s started. Crashing process in 3s; restart without CRASH_ONCE to watch recovery", workflowID)
		time.Sleep(3 * time.Second)
		os.Exit(99)
	}

	result, err := handle.GetResult(dbos.WithHandleTimeout(60 * time.Second))
	if err != nil {
		log.Fatalf("workflow failed: %v", err)
	}

	log.Printf("workflow completed: %s at %s", result.Message, result.CompletedAt.Format(time.RFC3339))
}
