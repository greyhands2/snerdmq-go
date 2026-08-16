<div align="center">
  <h1>🚀 SnerdMQ Go SDK (v1.0.2)</h1>
  <p>A zero-config, persistent background job queue for Go microservices. The official Go client for the SnerdMQ Rust daemon.</p>

  [![Go Reference](https://pkg.go.dev/badge/github.com/greyhands2/snerdmq-go.svg)](https://pkg.go.dev/github.com/greyhands2/snerdmq-go)
</div>

This is the official Go SDK wrapper for **SnerdMQ**. It handles all JSON-RPC communication and `os/exec` standard I/O orchestration so you can write lightning-fast background jobs in Go without blocking your application's main thread.

## ✨ Features
- **Ditch Redis**: The official SnerdMQ Go SDK gives your Goroutines persistent state, automatic retries, and dead-letter queues right out of the box.
- **Zero Rust Required**: Our CLI tool automatically downloads the pre-compiled C-speed Rust binary for your OS.
- **Concurrent Safe**: Uses strict `sync.RWMutex` locks and non-blocking I/O goroutines to handle massive scale.

## 📦 Installation

Installing the SDK is a simple two-step process:

**1. Install the module via go get:**
```bash
go get github.com/greyhands2/snerdmq-go
```

**2. Download the Rust Engine:**
Because Go modules do not support automated post-install hooks, we provide a clean CLI tool. Run this immediately after installing to fetch the correct SnerdMQ binary for your operating system (macOS/Linux/Windows). It will securely place the binary into a `./bin` folder in your project directory:
```bash
go run github.com/greyhands2/snerdmq-go/cmd/snerdmq-install@latest
```

---

## ⚡ Quickstart

Using the SDK is incredibly simple. Initialize the queue, register your handler functions, and start listening!

```go
package main

import (
	"fmt"
	"time"

	"github.com/greyhands2/snerdmq-go"
)

func main() {
	// 1. Initialize the daemon in the background
	queue, err := snerdmq.NewSnerdQueue()
	if err != nil {
		panic(err)
	}

	// 2. Register your background job logic
	queue.RegisterHandler("send_email", func(data map[string]interface{}) error {
		to := data["to"].(string)
		subject := data["subject"].(string)
		fmt.Printf("Sending email to %s with subject: %s...\n", to, subject)
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
		0.0,          // Retry After Hours
		snerdmq.EnqueueOpts{
			AutoDedupe:   true,
			UrgencyScore: 0.99,
			Cron:         "1h", // Runs every 1 hour!
		},
	)

	// Keep main thread alive
	queue.Wait()
}
```

### ⚙️ Advanced Task Configuration (v1.0.2)
To power complex workflows, tasks can now be configured with advanced orchestration parameters via `EnqueueOpts`:

* **`AutoDedupe` (`bool`)**: If set to `true`, the daemon computes a cryptographic hash of the task type and data. If an identical payload is pending execution, this new task is silently dropped.
* **`UrgencyScore` (`float64`)**: A value (e.g. `0.99`) used to bypass the standard FIFO queue. SnerdMQ uses a Binary Max-Heap to continually float tasks with the highest urgency score to the front. Standard tasks default to `0.0`.
* **`RateLimitGroup` (`string`)** & **`MaxPerMinute` (`int`)**: If the queue processes more tasks in this group than the allowed limit within a 60-second window, further tasks are temporarily paused.
* **`ExecuteAt` (`string` | `time.Time`)**: A timestamp of when the job should be executed in the future.
* **`Cron` (`string`)**: A cron expression (e.g. `"0 * * * *"`) for recurring jobs. Shorthands like `"2h"` or `"10m"` are also supported.

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

## 🌍 Advanced: Distributed Scaling

By default, the SDK spins up the Rust daemon which writes the queue to a local file (`.snerdata/tasks/tasks.log`). 

If you have multiple Go microservices running behind a load balancer and want them to share the exact same queue, simply mount a **Shared Network Drive** (like AWS EFS or NFS) to all of your servers and pass the shared path into the `SnerdQueueConfig`:

```go
import "github.com/greyhands2/snerdmq-go"

// All of your Go servers point to the exact same shared file!
// SnerdMQ's native OS file-locking guarantees zero data corruption.
queue, _ := snerdmq.NewSnerdQueue(snerdmq.SnerdQueueConfig{
	StoragePath: "/mnt/aws-efs-shared-drive/snerd_tasks.log",
})
```

*Built with ❤️ for John Wick tier engineering.*
