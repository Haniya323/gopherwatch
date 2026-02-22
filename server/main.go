package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	pb "gopherwatch/proto"
)

// ============================================
// STATE MANAGEMENT
// sync.Map = thread safe map, multiple goroutines
// can read/write safely at the same time
// ============================================

var serviceState sync.Map

// This stores the latest metric for each service
type ServiceHealth struct {
	ServiceID    string
	CPU          float64
	Memory       float64
	RequestCount int64
	Status       string
}

// ============================================
// gRPC SERVER
// ============================================

type metricsServer struct {
	pb.UnimplementedMetricsServiceServer
}

// This runs every time an agent connects and starts streaming
func (s *metricsServer) SendMetrics(stream pb.MetricsService_SendMetricsServer) error {
	// Read Service-ID from metadata (sent by the agent)
	// Metadata is like HTTP headers — extra info sent with the request
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

	// Keep receiving metrics until agent disconnects
	for {
		// Wait for next metric from agent
		report, err := stream.Recv()

		// io.EOF means agent finished streaming — normal ending
		if err == io.EOF {
			slog.Info("Agent finished streaming", "service_id", serviceID, "total", totalReceived)

			// Send summary back to agent
			return stream.SendAndClose(&pb.Summary{
				Message:       fmt.Sprintf("Received %d metrics from %s", totalReceived, serviceID),
				TotalReceived: int32(totalReceived),
			})
		}

		// Any other error means something went wrong
		if err != nil {
			slog.Error("Stream error", "service_id", serviceID, "error", err)
			return err
		}

		totalReceived++

		// Update the service state with latest metrics
		serviceState.Store(serviceID, ServiceHealth{
			ServiceID:    serviceID,
			CPU:          report.Cpu,
			Memory:       report.Memory,
			RequestCount: report.RequestCount,
			Status:       "healthy",
		})

		slog.Info("Metric received",
			"service_id", serviceID,
			"cpu", report.Cpu,
			"memory", report.Memory,
			"requests", report.RequestCount,
		)
	}
}

// ============================================
// INTERCEPTOR (MIDDLEWARE)
// Runs before every gRPC call — like a security
// guard that checks everyone coming in
// ============================================

func loggingInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	// Extract service-id from metadata
	md, ok := metadata.FromIncomingContext(ctx)
	serviceID := "unknown"
	if ok {
		ids := md.Get("service-id")
		if len(ids) > 0 {
			serviceID = ids[0]
		}
	}

	slog.Info("Interceptor caught request",
		"method", info.FullMethod,
		"service_id", serviceID,
	)

	// Pass the request through to the actual handler
	return handler(ctx, req)
}

// Stream interceptor — same but for streaming calls
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

	slog.Info("Stream interceptor",
		"method", info.FullMethod,
		"service_id", serviceID,
	)

	return handler(srv, ss)
}

// ============================================
// MAIN
// ============================================

func main() {
	slog.Info("Starting GopherWatch gRPC Server...")

	// Listen on port 50051
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal("Failed to listen:", err)
	}

	// Create gRPC server WITH interceptors attached
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(loggingInterceptor), // for regular calls
		grpc.StreamInterceptor(streamInterceptor), // for streaming calls
	)

	// Register our metrics service
	pb.RegisterMetricsServiceServer(grpcServer, &metricsServer{})

	slog.Info("gRPC Server listening", "port", 50051)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatal("Failed to serve:", err)
	}
}
