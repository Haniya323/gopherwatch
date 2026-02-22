package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
)

// ============================================
// TYPES
// ============================================

type AlertHistory struct {
	ID          int       `json:"id"`
	ServiceName string    `json:"service_name"`
	MetricName  string    `json:"metric_name"`
	Value       float64   `json:"value"`
	Message     string    `json:"message"`
	Timestamp   time.Time `json:"timestamp"`
}

type ServiceStatus struct {
	ServiceID    string    `json:"service_id"`
	CPU          float64   `json:"cpu"`
	Memory       float64   `json:"memory"`
	RequestCount int64     `json:"request_count"`
	LastSeen     time.Time `json:"last_seen"`
	Status       string    `json:"status"`
}

// ============================================
// API SERVER
// ============================================

type APIServer struct {
	db         *sql.DB
	state      *sync.Map
	httpServer *http.Server
}

func NewAPIServer(db *sql.DB, state *sync.Map) *APIServer {
	return &APIServer{
		db:    db,
		state: state,
	}
}

// GET /status — returns current health of all services
func (a *APIServer) getStatus(w http.ResponseWriter, r *http.Request) {
	services := []ServiceStatus{}

	// Range over sync.Map and collect all service states
	a.state.Range(func(key, value interface{}) bool {
		if status, ok := value.(ServiceStatus); ok {
			services = append(services, status)
		}
		return true // continue ranging
	})

	// Return proper HTTP 200 with JSON
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(services)
}

// GET /alerts/history — returns all alerts from database
func (a *APIServer) getAlertHistory(w http.ResponseWriter, r *http.Request) {
	// context.WithTimeout — give DB query 5 seconds max
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	rows, err := a.db.QueryContext(ctx,
		`SELECT id, service_name, metric_name, value, message, timestamp 
		 FROM alerts 
		 ORDER BY timestamp DESC`,
	)
	if err != nil {
		slog.Error("Failed to query alerts", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Failed to fetch alerts",
		})
		return
	}
	defer rows.Close()

	alerts := []AlertHistory{}
	for rows.Next() {
		var a AlertHistory
		rows.Scan(&a.ID, &a.ServiceName, &a.MetricName, &a.Value, &a.Message, &a.Timestamp)
		alerts = append(alerts, a)
	}

	// Return proper HTTP 200 with JSON
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(alerts)
}

// Start the API server
func (a *APIServer) Start() {
	// Create Gorilla Mux router
	router := mux.NewRouter()

	// Register routes
	router.HandleFunc("/status", a.getStatus).Methods("GET")
	router.HandleFunc("/alerts/history", a.getAlertHistory).Methods("GET")

	// Create http server
	a.httpServer = &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	slog.Info("REST API running", "port", 8080)
	slog.Info("Routes available:")
	slog.Info("  GET http://localhost:8080/status")
	slog.Info("  GET http://localhost:8080/alerts/history")

	if err := a.httpServer.ListenAndServe(); err != http.ErrServerClosed {
		slog.Error("API server error", "error", err)
	}
}

// Graceful shutdown — cleanly stop the API server
func (a *APIServer) Stop() {
	slog.Info("Shutting down API server...")

	// Give active requests 5 seconds to finish
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := a.httpServer.Shutdown(ctx); err != nil {
		slog.Error("API shutdown error", "error", err)
	}

	slog.Info("API server stopped cleanly")
}
