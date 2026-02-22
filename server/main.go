package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"gopherwatch/alerting"
	"gopherwatch/api"
	pb "gopherwatch/proto"

	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var serviceState sync.Map
var db *sql.DB

type metricsServer struct {
	pb.UnimplementedMetricsServiceServer
	alertEngine *alerting.AlertEngine
}

func (s *metricsServer) SendMetrics(stream pb.MetricsService_SendMetricsServer) error {
	md, ok := metadata.FromIncomingContext(stream.Context())
	serviceID := "unknown"
	if ok {
		ids := md.Get("service-id")
		if len(ids) > 0 {
			serviceID = ids[0]
		}
	}

	slog.Info("Agent connected", "service_id", serviceID)
	totalReceived := 0

	for {
		report, err := stream.Recv()

		if err == io.EOF {
			slog.Info("Agent finished streaming",
				"service_id", serviceID,
				"total", totalReceived,
			)
			return stream.SendAndClose(&pb.Summary{
				Message:       fmt.Sprintf("Received %d metrics from %s", totalReceived, serviceID),
				TotalReceived: int32(totalReceived),
			})
		}

		if err != nil {
			slog.Error("Stream error", "service_id", serviceID, "error", err)
			return err
		}

		totalReceived++
		serviceState.Store(serviceID, api.ServiceStatus{
			ServiceID:    serviceID,
			CPU:          report.Cpu,
			Memory:       report.Memory,
			RequestCount: report.RequestCount,
			LastSeen:     time.Now(),
			Status:       "healthy",
		})
		s.alertEngine.Dispatch(report)

		slog.Info("Metric received",
			"service_id", serviceID,
			"cpu", report.Cpu,
			"memory", report.Memory,
		)
	}
}

func loggingInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	serviceID := "unknown"
	if ok {
		ids := md.Get("service-id")
		if len(ids) > 0 {
			serviceID = ids[0]
		}
	}
	slog.Info("Interceptor", "method", info.FullMethod, "service_id", serviceID)
	return handler(ctx, req)
}

func streamInterceptor(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	md, ok := metadata.FromIncomingContext(ss.Context())
	serviceID := "unknown"
	if ok {
		ids := md.Get("service-id")
		if len(ids) > 0 {
			serviceID = ids[0]
		}
	}
	slog.Info("Stream interceptor", "method", info.FullMethod, "service_id", serviceID)
	return handler(srv, ss)
}

func main() {
	slog.Info("Starting GopherWatch...")

	// Connect to database
	var err error
	db, err = sql.Open("mysql",
		"root:Infoblox@12345@tcp(127.0.0.1:3306)/monitoring_system?parseTime=true")
	if err != nil {
		log.Fatal("DB connection failed:", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatal("DB not responding:", err)
	}
	slog.Info("Connected to MySQL!")
	defer db.Close()

	// Start alert engine
	alertEngine := alerting.NewAlertEngine(&serviceState, db)
	alertEngine.StartWorkerPool(5)

	// Start REST API in background
	apiServer := api.NewAPIServer(db, &serviceState)
	go apiServer.Start()

	// Start gRPC server
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal("Failed to listen:", err)
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(loggingInterceptor),
		grpc.StreamInterceptor(streamInterceptor),
	)

	pb.RegisterMetricsServiceServer(grpcServer, &metricsServer{
		alertEngine: alertEngine,
	})

	// Start gRPC in background
	go func() {
		slog.Info("gRPC Server listening", "port", 50051)
		if err := grpcServer.Serve(listener); err != nil {
			slog.Error("gRPC error", "error", err)
		}
	}()

	// ============================================
	// GRACEFUL SHUTDOWN
	// Wait for Ctrl+C then cleanly stop everything
	// ============================================
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Block here until Ctrl+C is pressed
	<-quit

	slog.Info("Shutdown signal received...")

	// Stop everything in order
	grpcServer.GracefulStop() // finish active streams then stop
	alertEngine.Stop()        // finish processing alerts then stop
	apiServer.Stop()          // finish active requests then stop

	slog.Info("GopherWatch stopped cleanly. Goodbye!")
}
