package queue

import (
	"fmt"

	"github.com/hashicorp/go-multierror"
	"github.com/stripe/aws-go/aws"
	"github.com/stripe/aws-go/gen/sqs"

	"github.com/nabeken/aws-go-sqs/queue/option"
)

type Queue struct {
	*sqs.SQS
	URL aws.StringValue
}

func New(s *sqs.SQS, name string) (*Queue, error) {
	u, err := GetQueueURL(s, name)
	if err != nil {
		return nil, err
	}

	return &Queue{
		SQS: s,
		URL: u,
	}, nil
}

func (q *Queue) SendMessage(body string, opts ...option.SendMessageRequest) error {
	req := &sqs.SendMessageRequest{
		MessageBody: aws.String(body),
		QueueURL:    q.URL,
	}

	for _, f := range opts {
		f(req)
	}

	_, err := q.SQS.SendMessage(req)
	return err
}

type BatchMessage struct {
	Body    string
	Options []option.SendMessageRequest
}

type BatchError struct {
	Entry sqs.BatchResultErrorEntry
}

func (e *BatchError) Error() string {
	return fmt.Sprintf("sqs: id: %s, code: %s, is_sender_failt: %s: %s",
		e.Entry.ID,
		e.Entry.Code,
		e.Entry.SenderFault,
		e.Entry.Message,
	)
}

func (q *Queue) SendMessageBatch(messages ...BatchMessage) error {
	entries := make([]sqs.SendMessageBatchRequestEntry, len(messages))
	for i, bm := range messages {
		req_ := &sqs.SendMessageRequest{}
		for _, f := range bm.Options {
			f(req_)
		}

		entries[i] = sqs.SendMessageBatchRequestEntry{
			DelaySeconds:      req_.DelaySeconds,
			MessageAttributes: req_.MessageAttributes,
			MessageBody:       aws.String(bm.Body),
			ID:                aws.String(fmt.Sprintf("msg-%d", i)),
		}
	}

	req := &sqs.SendMessageBatchRequest{
		Entries:  entries,
		QueueURL: q.URL,
	}

	resp, err := q.SQS.SendMessageBatch(req)
	if err != nil {
		return err
	}

	var result error
	for _, entry := range resp.Failed {
		result = multierror.Append(result, &BatchError{Entry: entry})
	}

	return result
}

func (q *Queue) ReceiveMessage(opts ...option.ReceiveMessageRequest) ([]sqs.Message, error) {
	req := &sqs.ReceiveMessageRequest{
		QueueURL: q.URL,
	}

	for _, f := range opts {
		f(req)
	}

	resp, err := q.SQS.ReceiveMessage(req)
	if err != nil {
		return nil, err
	}
	return resp.Messages, nil
}

func (q *Queue) DeleteMessage(receiptHandle aws.StringValue) error {
	return q.SQS.DeleteMessage(&sqs.DeleteMessageRequest{
		QueueURL:      q.URL,
		ReceiptHandle: receiptHandle,
	})
}

func (q *Queue) PurgeQueue() error {
	return q.SQS.PurgeQueue(&sqs.PurgeQueueRequest{
		QueueURL: q.URL,
	})
}

func GetQueueURL(s *sqs.SQS, name string) (aws.StringValue, error) {
	req := &sqs.GetQueueURLRequest{
		QueueName: aws.String(name),
	}

	resp, err := s.GetQueueURL(req)
	if err != nil {
		return nil, err
	}
	return resp.QueueURL, nil
}
