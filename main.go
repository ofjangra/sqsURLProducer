package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/ofjangra/sqsURLProducer/app"
	"github.com/ofjangra/sqsURLProducer/models"
	"gorm.io/gorm"
)

const (
	BatchSize       = 10
	RetryAttempts   = 3
	RetryBackoff    = 2 * time.Second
	DatabaseLimit   = 100
	PollingInterval = 10 * time.Second
)

func main() {
	app.InitApp()

	queueURL := getEnv("SQS_URL")
	accessKeyID := getEnv("IAM_ACCESS_KEY")
	secretAccessKey := getEnv("IAM_SECRET")
	region := getEnv("AWS_REGION")
	port := getEnv("PORT")

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, "")),
	)
	if err != nil {
		log.Fatalf("Failed to load AWS configuration: %v", err)
	}

	sqsClient := sqs.NewFromConfig(cfg)
	db := app.GetDB()

	// Graceful shutdown handling
	ctx, cancel := context.WithCancel(context.Background())
	go handleShutdown(cancel)

	log.Println("Starting SQS Producer...")

	messageCount := 0

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Println("Shutting down producer...")
				return
			default:
				processURLs(ctx, db, sqsClient, queueURL, &messageCount)
			}

			time.Sleep(PollingInterval)
		}
	}()

	// Start a simple HTTP server to keep the application running and provide a status endpoint
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("SQS Producer is running"))
	})

	log.Println("Starting HTTP server on port", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
}

func processURLs(ctx context.Context, db *gorm.DB, sqsClient *sqs.Client, queueURL string, messageCount *int) {
	var urls []models.URLs
	result := db.Limit(DatabaseLimit).Where("processed = ?", false).Find(&urls)
	if result.Error != nil {
		log.Printf("Database query failed: %v", result.Error)
		return
	}

	if len(urls) == 0 {
		log.Println("No URLs found, sleeping...")
		return
	}

	log.Printf("Processing %d URLs...", len(urls))

	// Prepare batches
	var batch []types.SendMessageBatchRequestEntry
	for i, url := range urls {
		*messageCount++
		batch = append(batch, types.SendMessageBatchRequestEntry{
			Id:             aws.String(fmt.Sprintf("msg-%d", *messageCount)),
			MessageBody:    aws.String(url.URL),
			MessageGroupId: aws.String(fmt.Sprintf("group-%d", *messageCount)),
		})

		// Send when batch size is reached or it's the last message
		if len(batch) == BatchSize || i == len(urls)-1 {
			if err := sendBatch(ctx, sqsClient, queueURL, batch); err != nil {
				log.Printf("Failed to send batch: %v", err)
			} else {
				// Update processed status for URLs in the batch
				for _, entry := range batch {
					db.Model(&models.URLs{}).Where("url = ?", *entry.MessageBody).Update("processed", true)
				}
			}
			batch = nil // Reset batch
		}
	}
}

func sendBatch(ctx context.Context, sqsClient *sqs.Client, queueURL string, batch []types.SendMessageBatchRequestEntry) error {
	for attempt := 0; attempt < RetryAttempts; attempt++ {
		_, err := sqsClient.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
			QueueUrl: aws.String(queueURL),
			Entries:  batch,
		})
		if err == nil {
			log.Printf("Successfully sent batch of %d messages.", len(batch))
			return nil
		}

		log.Printf("Send batch attempt %d failed: %v", attempt+1, err)
		time.Sleep(RetryBackoff * time.Duration(attempt+1))
	}

	return fmt.Errorf("failed to send batch after %d attempts", RetryAttempts)
}

func handleShutdown(cancel context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	cancel()
}

func getEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("Environment variable %s is not set", key)
	}
	return value
}
