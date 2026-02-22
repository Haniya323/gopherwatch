package main

import (
	"context"
	"log"
	"log/slog"
	"math/rand"
	"time"

	pb "gopherwatch/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func main() {
	// The agent's unique identity
	serviceID := "web-server-01"

	slog.Info("Agent starting", "service_id", serviceID)

	// Connect to gRPC server
	conn, err := grpc.Dial("localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal("Could not connect:", err)
	}
	defer conn.Close()

	client := pb.NewMetricsServiceClient(conn)

	// Attach service-id to metadata
	// This goes in the "envelope" so server knows who we are
	ctx := metadata.AppendToOutgoingContext(
		context.Background(),
		"service-id", serviceID,
	)

	// Start streaming to server
	stream, err := client.SendMetrics(ctx)
	if err != nil {
		log.Fatal("Could not start stream:", err)
	}

	slog.Info("Connected to server! Streaming metrics...")

	// Send 10 metrics then stop
	for i := 0; i < 10; i++ {
		report := &pb.MetricReport{
			ServiceId:    serviceID,
			Cpu:          rand.Float64() * 100,
			Memory:       rand.Float64() * 100,
			RequestCount: int64(rand.Intn(1000)),
			Timestamp:    time.Now().Unix(),
		}

		// Send metric to server
		if err := stream.Send(report); err != nil {
			log.Fatal("Failed to send:", err)
		}

		slog.Info("Metric sent",
			"cpu", report.Cpu,
			"memory", report.Memory,
			"requests", report.RequestCount,
		)

		// Wait 2 seconds between metrics
		time.Sleep(2 * time.Second)
	}

	// Tell server we are done — this triggers io.EOF on server side
	summary, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatal("Failed to receive summary:", err)
	}

	slog.Info("Stream complete!",
		"message", summary.Message,
		"total_received", summary.TotalReceived,
	)
}
