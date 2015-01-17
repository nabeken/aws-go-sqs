package queue_test

import (
	"log"

	"github.com/stripe/aws-go/aws"
	"github.com/stripe/aws-go/gen/sqs"

	"github.com/nabeken/aws-go-sqs/queue"
	"github.com/nabeken/aws-go-sqs/queue/option"
)

func ExampleQueue_SendMessage() {
	creds := aws.DetectCreds("", "", "")

	// Create SQS instance
	s := sqs.New(creds, "ap-northeast-1", nil)

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

	if err := q.SendMessage("MESSAGE BODY", option.MessageAttributes(attrs)); err != nil {
		log.Fatal(err)
	}

	log.Print("successed!")
}

func ExampleQueue_SendMessageBatch() {
	creds := aws.DetectCreds("", "", "")

	// Create SQS instance
	s := sqs.New(creds, "ap-northeast-1", nil)

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
			Options: []option.SendMessageRequest{option.MessageAttributes(attrs)},
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
