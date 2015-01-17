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
