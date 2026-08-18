<div align="center">
  <h1>🚀 SnerdMQ Go SDK (v1.0.5)</h1>
  <p>A zero-config, persistent background job queue for Go microservices. The official Go client for the SnerdMQ Rust daemon.</p>

  [![Go Reference](https://pkg.go.dev/badge/github.com/speed-nerd/snerdmq-go.svg)](https://pkg.go.dev/github.com/speed-nerd/snerdmq-go)
</div>

This is the official Go SDK wrapper for **SnerdMQ**. It handles all JSON-RPC communication and `os/exec` standard I/O orchestration so you can write lightning-fast background jobs in Go without blocking your application's main thread.

## ✨ Features
- **Ditch Redis**: The official SnerdMQ Go SDK gives your Goroutines persistent state, automatic retries, and dead-letter queues right out of the box.
- **Progress Streaming & Live Dashboard**: Handlers can stream progress updates to a built-in React UI dashboard served by the SDK.
- **Zero Rust Required**: Our CLI tool automatically downloads the pre-compiled C-speed Rust binary for your OS.
- **Concurrent Safe**: Uses strict `sync.RWMutex` locks and non-blocking I/O goroutines to handle massive scale.

## 📦 Installation

Installing the SDK is a simple two-step process:

**1. Install the module via go get:**
```bash
go get github.com/speed-nerd/snerdmq-go
```

**2. Download the Rust Engine:**
Because Go modules do not support automated post-install hooks, we provide a clean CLI tool. Run this immediately after installing to fetch the correct SnerdMQ binary for your operating system (macOS/Linux/Windows). It will securely place the binary into a `./bin` folder in your project directory:
```bash
go run github.com/speed-nerd/snerdmq-go/cmd/snerdmq-install@latest
```

---

## ⚡ Quickstart

Using the SDK is incredibly simple. Initialize the queue, register your handler functions, and start listening!

```go
package main

import (
	"context"
	"fmt"

	"github.com/speed-nerd/snerdmq-go"
)

func main() {
	// 1. Initialize the daemon in the background
	queue, err := snerdmq.NewSnerdQueue()
	if err != nil {
		panic(err)
	}

	// 2. Register your background job logic
	queue.RegisterHandler("send_email", func(ctx context.Context, data map[string]interface{}) error {
		to := data["to"].(string)
		subject := data["subject"].(string)
		fmt.Printf("Sending email to %s with subject: %s...\n", to, subject)
		// Return an error here to automatically trigger SnerdMQ's retry logic!
		return nil
	})

	// 3. Start the non-blocking goroutine listeners
	if err := queue.StartListening(); err != nil {
		panic(err)
	}
	fmt.Println("SnerdMQ Go SDK is listening for jobs...")

	// 4. Enqueue a job from anywhere in your codebase
	queue.Enqueue(
		"email-123",  // Unique Task ID
		"send_email", // Task Type
		map[string]interface{}{"to": "john@wick.com", "subject": "Continental Update"}, // Payload
		3,            // Max Retries
		0.5,          // Retry After Hours (wait 30 minutes before retrying)
		"email_api",  // Rate Limit Group
		100,          // Max Per Minute
		nil,          // Auto Dedupe
		nil,          // Urgency Score
		nil,          // Execute At
		nil,          // Cron
		nil,          // Webhook URL
		nil,          // Max Execution Seconds
	)

	// 5. Need scheduling, deduplication, or serverless execution? All
	// orchestration options are opt-in — combine only what you need:
	autoDedupe := true
	urgencyScore := 0.99
	cronStr := "0 8 * * *"               // Run every day at 08:00
	webhookUrl := "https://api.example.com/webhook"
	maxExecutionSeconds := 300

	queue.Enqueue(
		"email-digest-1",
		"send_email",
		map[string]interface{}{"to": "john@wick.com", "subject": "Daily Digest"},
		3,
		0.0,
		"",            // No rate limit group
		0,             // No max-per-minute cap
		&autoDedupe,   // Drop identical pending payloads
		&urgencyScore, // Float to the front of the queue
		nil,
		&cronStr,      // Cron schedule
		&webhookUrl,   // Execute via HTTP instead of local handlers
		&maxExecutionSeconds,
	)

	// Keep main thread alive
	queue.Wait()
}
```

### ⚙️ Advanced Task Configuration (v1.0.5)
To power complex workflows, tasks can now be configured with advanced orchestration parameters via the `Enqueue` positional arguments:

* **`AutoDedupe` (`bool`)**: If set to `true`, the daemon computes a cryptographic hash of the task type and data. If an identical payload is pending execution, this new task is silently dropped.
* **`UrgencyScore` (`float64`)**: A value (e.g. `0.99`) used to bypass the standard FIFO queue. SnerdMQ uses a Binary Max-Heap to continually float tasks with the highest urgency score to the front. Standard tasks default to `0.0`.
* **`RateLimitGroup` (`string`)** & **`MaxPerMinute` (`int`)**: If the queue processes more tasks in this group than the allowed limit within a 60-second window, further tasks are temporarily paused.
* **`ExecuteAt` (`string` | `time.Time`)**: A timestamp of when the job should be executed in the future.
* **`RetryAfterHours` (`float64`)**: Backoff in **hours** before a failed job is retried (default `0.0`). See *Cron Jobs vs. Retryable Jobs* below.
* **`Cron` (`string`)**: A cron expression (e.g. `"0 * * * *"`) for recurring jobs. Shorthands like `"2h"` or `"10m"` are also supported.
* **`WebhookUrl` (`string`)**: By providing a webhook URL, SnerdMQ will completely bypass your local Go handlers and dispatch the task payload via an HTTP POST request directly to the specified URL.
* **`MaxExecutionSeconds` (`int`)**: Optional hard timeout in seconds. If execution takes longer, it's marked as failed via a context timeout.

### Note on Hard Timeouts (`MaxExecutionSeconds`)
When `MaxExecutionSeconds` is provided, the Go SDK executes your handler with a `context.WithTimeout`. If the task takes longer than the timeout, the context is cancelled, and if your handler respects the context cancellation, it will terminate early and the task is marked as failed. In addition, the background Rust daemon will forcefully time out the IPC channel if it takes too long.

### 🌐 HTTP Webhooks (Serverless Execution)
You can configure a task to execute externally via an HTTP POST request. By setting a `WebhookUrl`, the internal background processor will skip any registered handlers (`queue.RegisterHandler`) and directly invoke the HTTP endpoint.

If the HTTP endpoint returns a non-200 status code, it triggers a retry. If it permanently fails (reaches `maxRetries`), the Dead Letter Queue event is automatically fired via a final HTTP POST to the same `WebhookUrl` but with the header `X-SnerdMQ-Event: MaxRetriesReached`.

### 🕒 Cron Jobs vs. Retryable Jobs
> - **A Cron Job** is a *Repeatable Job* that executes again **only after a success**, on a fixed schedule.
> - **A Retryable Job** is a *Recovery Job* that executes again **only after a failure**, attempting to recover using the `retryAfterHours` backoff.
> - **Combined:** If a Cron Job fails, it uses `retryAfterHours` to retry until it recovers, then goes back to its standard cron schedule!
### ☠️ Dead Letter Queue (Handling Permanent Failures)

When a task fails repeatedly and exhausts its `maxRetries`, the SnerdMQ daemon permanently moves it to the Dead Letter Queue. You can hook into this event to alert your team, update your database, or send a Slack message by registering a Max Retry Handler.

```go
// 5. Catch tasks that have permanently failed (Dead Letter Queue)
queue.RegisterMaxRetryHandler("send_email", func(ctx context.Context, data map[string]interface{}) error {
    fmt.Printf("Email task failed after all retries! Data: %v\n", data)
    return nil
})
```

---

## 📊 Live Dashboard

SnerdMQ ships with a built-in **React UI dashboard** served directly by the SDK — no extra services or ports to manage in your infrastructure. It gives you a real-time window into your queue:

- **Live stats**: total enqueued, processed, and failed jobs
- **Recent Jobs table**: per-task status (`queued`, `active`, `completed`, `failed`, `dead_letter`), retry counts, and badges showing which features a task uses (cron / webhook / timeout)
- **Real-time Progress Stream**: live output from `YieldProgress` calls in your handlers

```go
queue, _ := snerdmq.NewSnerdQueue()

// Start the built-in dashboard on http://localhost:9090
queue.StartDashboard(9090)

// ... register handlers, start listening, enqueue jobs ...
```

Then open **http://localhost:9090** in your browser. Updates are pushed to the page over WebSocket the moment jobs change state, and the dashboard also exposes a small JSON API (`/api/stats`, `/api/tasks`, `/api/progress`) if you want to build your own tooling on top.

> **Note:** The dashboard serves its `static/` assets relative to your process's working directory, so run your binary from the directory that contains the `static/` folder (the SDK bundles one in its repo). `StartDashboard` only serves the UI — your jobs keep running whether or not the dashboard is open.

---

## 📡 Progress Reporting

Long-running handlers can stream live updates to the Dashboard's Progress Stream (ideal for streaming LLM tokens or multi-step ETL work):

```go
queue.RegisterHandler("generate_report", func(ctx context.Context, data map[string]interface{}) error {
    for step := 1; step <= 10; step++ {
        doWork(step)
        queue.YieldProgress(ctx, fmt.Sprintf("Step %d/10 complete", step))
    }
    return nil
})
```

> `YieldProgress` must be called with the task's `context.Context` **inside a task handler** — the context is how the SDK knows which job the update belongs to.

---

## 🌍 Advanced: Distributed Scaling

By default, the SDK spins up the Rust daemon which writes the queue to a local file (`.snerdata/tasks/tasks.log`). 

If you have multiple Go microservices running behind a load balancer and want them to share the exact same queue, simply mount a **Shared Network Drive** (like AWS EFS or NFS) to all of your servers and pass the shared path into the `SnerdQueueConfig`:

```go
import "github.com/speed-nerd/snerdmq-go"

// All of your Go servers point to the exact same shared file!
// SnerdMQ's native OS file-locking guarantees zero data corruption.
queue, _ := snerdmq.NewSnerdQueue(snerdmq.SnerdQueueConfig{
	StoragePath: "/mnt/aws-efs-shared-drive/snerd_tasks.log",
})
```

*Built with ❤️ for John Wick tier engineering.*
