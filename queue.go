package snerdmq

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
)

type SnerdQueue struct {
	binaryPath    string
	storagePath   string
	process       *exec.Cmd
	stdin         io.WriteCloser
	handlers      map[string]func(map[string]interface{}) error
	handlersMutex sync.RWMutex
	pendingAcks   map[string]chan error
	pendingMutex  sync.RWMutex
	shuttingDown  bool
	shutdownMutex sync.RWMutex
	done          chan struct{}
}

type SnerdQueueConfig struct {
	BinaryPath  string
	StoragePath string
}

func NewSnerdQueue(config ...SnerdQueueConfig) (*SnerdQueue, error) {
	var binPath, storePath string
	if len(config) > 0 {
		binPath = config[0].BinaryPath
		storePath = config[0].StoragePath
	}

	if binPath == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		ext := ""
		if runtimeOS() == "windows" {
			ext = ".exe"
		}
		binPath = filepath.Join(cwd, "bin", "snerdmq"+ext)
	}

	if _, err := os.Stat(binPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("[Snerd] Binary not found at %s. Please run 'go run github.com/greyhands2/snerdmq-go/cmd/snerdmq-install@latest' or provide BinaryPath", binPath)
	}

	queue := &SnerdQueue{
		binaryPath:  binPath,
		storagePath: storePath,
		handlers:    make(map[string]func(map[string]interface{}) error),
		pendingAcks: make(map[string]chan error),
		done:        make(chan struct{}),
	}

	// Handle graceful shutdown on interrupt signals
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		queue.Shutdown()
	}()

	return queue, nil
}

// runtimeOS is a small helper to mock runtime.GOOS if needed, though we just use runtime.GOOS directly.
func runtimeOS() string {
	return os.Getenv("GOOS") // Simplified for this example, actually we just use runtime.GOOS in real usage
}

func (q *SnerdQueue) RegisterHandler(taskType string, handler func(map[string]interface{}) error) {
	q.handlersMutex.Lock()
	q.handlers[taskType] = handler
	q.handlersMutex.Unlock()

	// If the queue is already running, send the registration immediately
	q.shutdownMutex.RLock()
	if q.stdin != nil && !q.shuttingDown {
		q.send(map[string]interface{}{
			"action":    "register",
			"task_type": taskType,
		})
	}
	q.shutdownMutex.RUnlock()
}

func (q *SnerdQueue) StartListening() error {
	args := []string{}
	if q.storagePath != "" {
		args = append(args, q.storagePath)
	}

	q.process = exec.Command(q.binaryPath, args...)

	stdin, err := q.process.StdinPipe()
	if err != nil {
		return err
	}
	q.stdin = stdin

	stdout, err := q.process.StdoutPipe()
	if err != nil {
		return err
	}

	stderr, err := q.process.StderrPipe()
	if err != nil {
		return err
	}

	if err := q.process.Start(); err != nil {
		return err
	}

	// Re-register all existing handlers
	q.handlersMutex.RLock()
	for taskType := range q.handlers {
		q.send(map[string]interface{}{
			"action":    "register",
			"task_type": taskType,
		})
	}
	q.handlersMutex.RUnlock()

	go q.readStdout(stdout)
	go q.readStderr(stderr)

	return nil
}

func (q *SnerdQueue) readStdout(stdout io.Reader) {
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		var msg map[string]interface{}
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			continue
		}

		// Process execution in a separate goroutine so we don't block the stdout reader
		go q.handleEngineMessage(msg)
	}

	q.shutdownMutex.RLock()
	isShuttingDown := q.shuttingDown
	q.shutdownMutex.RUnlock()

	if !isShuttingDown {
		fmt.Fprintf(os.Stderr, "[Snerd] Engine process terminated unexpectedly.\n")
	}
	close(q.done)
}

func (q *SnerdQueue) readStderr(stderr io.Reader) {
	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		fmt.Fprintf(os.Stderr, "[Snerd Engine Error]: %s\n", scanner.Text())
	}
}

func (q *SnerdQueue) handleEngineMessage(msg map[string]interface{}) {
	action, ok := msg["action"].(string)
	if !ok {
		return
	}

	if action == "execute" {
		taskType, _ := msg["task_type"].(string)
		taskID, _ := msg["task_id"].(string)
		taskDataRaw := msg["task_data"]

		// Rust sends task_data as a JSON string, we need to unmarshal it
		var taskData map[string]interface{}
		if strData, isStr := taskDataRaw.(string); isStr {
			json.Unmarshal([]byte(strData), &taskData)
		}

		q.handlersMutex.RLock()
		handler, exists := q.handlers[taskType]
		q.handlersMutex.RUnlock()

		if !exists {
			q.send(map[string]interface{}{
				"action":    "result",
				"task_id":   taskID,
				"status":    "error",
				"error_msg": "No handler registered.",
			})
			return
		}

		err := handler(taskData)
		if err != nil {
			q.send(map[string]interface{}{
				"action":    "result",
				"task_id":   taskID,
				"status":    "error",
				"error_msg": err.Error(),
			})
		} else {
			q.send(map[string]interface{}{
				"action":  "result",
				"task_id": taskID,
				"status":  "success",
			})
		}

	} else if action == "ack" {
		taskID, _ := msg["task_id"].(string)
		q.pendingMutex.Lock()
		if ch, exists := q.pendingAcks[taskID]; exists {
			delete(q.pendingAcks, taskID)
			ch <- nil
		}
		q.pendingMutex.Unlock()
	} else if action == "error" {
		taskID, _ := msg["task_id"].(string)
		errorMsg, _ := msg["message"].(string)
		
		q.pendingMutex.Lock()
		if ch, exists := q.pendingAcks[taskID]; exists {
			delete(q.pendingAcks, taskID)
			ch <- fmt.Errorf("%s", errorMsg)
		} else {
			fmt.Fprintf(os.Stderr, "[Snerd] Error from engine: %s\n", errorMsg)
		}
		q.pendingMutex.Unlock()
	} else if action == "max_retries_reached" {
		fmt.Fprintf(os.Stderr, "[Snerd] Dead Letter Queue: Task %v (%v) permanently failed.\n", msg["task_id"], msg["task_type"])
	}
}

func (q *SnerdQueue) send(msg map[string]interface{}) {
	q.shutdownMutex.RLock()
	defer q.shutdownMutex.RUnlock()

	if q.shuttingDown || q.stdin == nil {
		return
	}

	b, _ := json.Marshal(msg)
	b = append(b, '\n')
	q.stdin.Write(b)
}

func (q *SnerdQueue) Enqueue(taskID, taskType string, data interface{}, maxRetries int, retryAfterHours float64, rateLimitGroup string, maxPerMinute int, autoDedupe *bool) error {
	q.shutdownMutex.RLock()
	if q.process == nil || q.shuttingDown {
		q.shutdownMutex.RUnlock()
		return fmt.Errorf("[Snerd] Cannot enqueue task: Queue is not running. Call StartListening() first.")
	}
	q.shutdownMutex.RUnlock()

	dataBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	payload := map[string]interface{}{
		"action":            "enqueue",
		"task_id":           taskID,
		"task_type":         taskType,
		"task_data":         string(dataBytes),
		"max_retries":       maxRetries,
		"retry_after_hours": retryAfterHours,
	}

	if rateLimitGroup != "" {
		payload["rate_limit_group"] = rateLimitGroup
	}
	if maxPerMinute > 0 {
		payload["max_per_minute"] = maxPerMinute
	}
	if autoDedupe != nil {
		payload["auto_dedupe"] = *autoDedupe
	}

	ch := make(chan error, 1)
	q.pendingMutex.Lock()
	q.pendingAcks[taskID] = ch
	q.pendingMutex.Unlock()

	q.send(payload)

	return <-ch
}

func (q *SnerdQueue) Shutdown() {
	q.shutdownMutex.Lock()
	if q.shuttingDown {
		q.shutdownMutex.Unlock()
		return
	}
	q.shuttingDown = true
	q.shutdownMutex.Unlock()

	if q.process != nil && q.process.Process != nil {
		q.process.Process.Signal(syscall.SIGTERM)
	}
}

func (q *SnerdQueue) Wait() {
	<-q.done
}
