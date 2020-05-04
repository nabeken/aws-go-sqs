package queue_test

import (
	"log"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/nabeken/aws-go-sqs/v3/queue"
	"github.com/nabeken/aws-go-sqs/v3/queue/option"
)

func ExampleQueue_SendMessage() {
	// Create SQS instance
	s := sqs.New(session.Must(session.NewSession()))

	// Create Queue instance
	q, err := queue.New(s, "example-queue-name")
	if err != nil {
		log.Fatal(err)
	}

	// MessageAttributes
	attrs := map[string]interface{}{
		"ATTR1": "STRING!!",
		"ATTR2": 12345,
	}

	if _, err := q.SendMessage("MESSAGE BODY", option.MessageAttributes(attrs)); err != nil {
		log.Fatal(err)
	}

	log.Print("successed!")
}

func ExampleQueue_SendMessageBatch() {
	// Create SQS instance
	s := sqs.New(session.Must(session.NewSession()))

	// Create Queue instance
	q, err := queue.New(s, "example-queue-name")
	if err != nil {
		log.Fatal(err)
	}

	// MessageAttributes
	attrs := map[string]interface{}{
		"ATTR1": "STRING!!",
	}

	// Create messages for batch operation
	batchMessages := []queue.BatchMessage{
		queue.BatchMessage{
			Body: "success",
		},
		queue.BatchMessage{
			Body:    "failed",
			Options: []option.SendMessageInput{option.MessageAttributes(attrs)},
		},
	}

	err = q.SendMessageBatch(batchMessages...)
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
