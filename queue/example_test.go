package queue_test

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/nabeken/aws-go-sqs/v4/queue"
	"github.com/nabeken/aws-go-sqs/v4/queue/option"
)

func ExampleQueue_SendMessage() {
	// Create SQS instance
	s := sqs.New(sqs.Options{})

	// Create Queue instance
	q, err := queue.New(context.Background(), s, "example-queue-name")
	if err != nil {
		log.Fatal(err)
	}

	// MessageAttributes
	attrs := map[string]interface{}{
		"ATTR1": "STRING!!",
		"ATTR2": 12345,
	}

	if _, err := q.SendMessage(context.Background(), "MESSAGE BODY", option.MessageAttributes(attrs)); err != nil {
		log.Fatal(err)
	}

	log.Print("successed!")
}

func ExampleQueue_SendMessageBatch() {
	// Create SQS instance
	s := sqs.New(sqs.Options{})

	// Create Queue instance
	q, err := queue.New(context.Background(), s, "example-queue-name")
	if err != nil {
		log.Fatal(err)
	}

	// MessageAttributes
	attrs := map[string]interface{}{
		"ATTR1": "STRING!!",
	}

	// Create messages for batch operation
	batchMessages := []queue.BatchMessage{
		{
			Body: "success",
		},
		{
			Body:    "failed",
			Options: []option.SendMessageInput{option.MessageAttributes(attrs)},
		},
	}

	err = q.SendMessageBatch(context.Background(), batchMessages...)
	if err != nil {
		batchErrors, ok := queue.IsBatchError(err)
		if !ok {
			log.Fatal(err)
		}
		for _, e := range batchErrors {
			if e.SenderFault {
				// Continue if the failure is on the client side.
				log.Print(e)
				continue
			}
			// Retry if the failure is on the server side
			// You can use e.Index to identify the message
			// failedMessage := batchMessages[e.Index]
		}
	}
}
