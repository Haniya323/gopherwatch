package alerting

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"
	"time"

	pb "gopherwatch/proto"

	_ "github.com/go-sql-driver/mysql"
)

// ============================================
// ALERT RULE STRUCT
// Defines when to trigger an alert
// ============================================

type AlertRule struct {
	MetricType string  // "cpu" or "memory"
	Threshold  float64 // the limit e.g. 90.0
	Message    string  // what to say when triggered
}

// ============================================
// EVALUATOR INTERFACE
// An interface is a contract — anything that
// implements Evaluate() is an Evaluator
// ============================================

type Evaluator interface {
	Evaluate(report *pb.MetricReport) (bool, string)
}

// CPUEvaluator checks CPU threshold
type CPUEvaluator struct {
	Rule AlertRule
}

func (e *CPUEvaluator) Evaluate(report *pb.MetricReport) (bool, string) {
	if report.Cpu > e.Rule.Threshold {
		return true, e.Rule.Message
	}
	return false, ""
}

// MemoryEvaluator checks Memory threshold
type MemoryEvaluator struct {
	Rule AlertRule
}

func (e *MemoryEvaluator) Evaluate(report *pb.MetricReport) (bool, string) {
	if report.Memory > e.Rule.Threshold {
		return true, e.Rule.Message
	}
	return false, ""
}

// ============================================
// ALERT ENGINE
// ============================================

type AlertEngine struct {
	evaluators []Evaluator           // list of rules to check
	dispatcher chan *pb.MetricReport // the fan-out channel
	state      *sync.Map             // current health of all services
	db         *sql.DB               // database connection
	wg         sync.WaitGroup
}

// Create new alert engine with rules
func NewAlertEngine(state *sync.Map, db *sql.DB) *AlertEngine {
	return &AlertEngine{
		dispatcher: make(chan *pb.MetricReport, 500),
		state:      state,
		db:         db,
		evaluators: []Evaluator{
			&CPUEvaluator{
				Rule: AlertRule{
					MetricType: "cpu",
					Threshold:  90.0,
					Message:    "CPU usage is critically high!",
				},
			},
			&MemoryEvaluator{
				Rule: AlertRule{
					MetricType: "memory",
					Threshold:  85.0,
					Message:    "Memory usage is critically high!",
				},
			},
		},
	}
}

// Dispatch sends a metric into the fan-out channel
// Called by gRPC server every time a metric arrives
func (e *AlertEngine) Dispatch(report *pb.MetricReport) {
	e.dispatcher <- report
}

// StartWorkerPool launches N workers that all
// pull from the same dispatcher channel
func (e *AlertEngine) StartWorkerPool(workerCount int) {
	slog.Info("Starting worker pool", "workers", workerCount)

	for i := 0; i < workerCount; i++ {
		e.wg.Add(1)
		go e.worker(i)
	}
}

// Each worker pulls metrics from channel and evaluates them
func (e *AlertEngine) worker(id int) {
	defer e.wg.Done()

	slog.Info("Worker started", "worker_id", id)

	// Keep pulling from channel until it's closed
	for report := range e.dispatcher {
		// Check every evaluator (rule) against this metric
		for _, evaluator := range e.evaluators {
			triggered, message := evaluator.Evaluate(report)

			if triggered {
				// Log critical alert
				slog.Warn("CRITICAL ALERT",
					"worker_id", id,
					"service_id", report.ServiceId,
					"cpu", report.Cpu,
					"memory", report.Memory,
					"message", message,
				)

				// Save alert to database
				go e.saveAlert(report, message)
			}
		}
	}
}

// Save triggered alert to MySQL
func (e *AlertEngine) saveAlert(report *pb.MetricReport, message string) {
	// context.WithTimeout prevents hanging if DB is slow
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := e.db.ExecContext(ctx,
		`INSERT INTO alerts 
		(service_name, metric_name, value, message) 
		VALUES (?, ?, ?, ?)`,
		report.ServiceId,
		"cpu_or_memory",
		report.Cpu,
		message,
	)
	if err != nil {
		slog.Error("Failed to save alert", "error", err)
		return
	}

	slog.Info("Alert saved to database", "service_id", report.ServiceId)
}

// Stop closes the dispatcher channel
// workers will finish remaining items then exit
func (e *AlertEngine) Stop() {
	close(e.dispatcher)
	e.wg.Wait()
	slog.Info("Alert engine stopped")
}
