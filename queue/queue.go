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

func (q *Queue) ChangeMessageVisibility(receiptHandle aws.StringValue, visibilityTimeout int) error {
	req := &sqs.ChangeMessageVisibilityRequest{
		ReceiptHandle:     receiptHandle,
		VisibilityTimeout: aws.Integer(visibilityTimeout),
		QueueURL:          q.URL,
	}
	return q.SQS.ChangeMessageVisibility(req)
}

type BatchChangeMessageVisibility struct {
	ReceiptHandle     aws.StringValue
	VisibilityTimeout int
}

func (q *Queue) ChangeMessageVisibilityBatch(opts ...BatchChangeMessageVisibility) error {
	entries := make([]sqs.ChangeMessageVisibilityBatchRequestEntry, len(opts))
	id2index := make(map[string]int)
	for i, b := range opts {
		id := aws.String(fmt.Sprintf("msg-%d", i))
		entries[i] = sqs.ChangeMessageVisibilityBatchRequestEntry{
			ID:                id,
			ReceiptHandle:     b.ReceiptHandle,
			VisibilityTimeout: aws.Integer(b.VisibilityTimeout),
		}
		id2index[*id] = i
	}

	req := &sqs.ChangeMessageVisibilityBatchRequest{
		Entries:  entries,
		QueueURL: q.URL,
	}

	resp, err := q.SQS.ChangeMessageVisibilityBatch(req)
	if err != nil {
		return err
	}
	return newBatchError(id2index, resp.Failed)
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
	Index       int
	Code        string
	Message     string
	SenderFault bool
}

func newBatchError(id2index map[string]int, errors []sqs.BatchResultErrorEntry) error {
	var result error
	for _, entry := range errors {
		err := &BatchError{
			Index:       id2index[*entry.ID],
			Code:        *entry.Code,
			Message:     *entry.Message,
			SenderFault: *entry.SenderFault,
		}
		result = multierror.Append(result, err)
	}
	return result
}

func (e *BatchError) Error() string {
	return fmt.Sprintf("sqs: index: %s, code: %s, is_sender_failt: %s: %s",
		e.Index,
		e.Code,
		e.SenderFault,
		e.Message,
	)
}

func (q *Queue) SendMessageBatch(messages ...BatchMessage) error {
	entries := make([]sqs.SendMessageBatchRequestEntry, len(messages))
	id2index := make(map[string]int)
	for i, bm := range messages {
		req_ := &sqs.SendMessageRequest{}
		for _, f := range bm.Options {
			f(req_)
		}

		id := aws.String(fmt.Sprintf("msg-%d", i))
		entries[i] = sqs.SendMessageBatchRequestEntry{
			DelaySeconds:      req_.DelaySeconds,
			MessageAttributes: req_.MessageAttributes,
			MessageBody:       aws.String(bm.Body),
			ID:                id,
		}
		id2index[*id] = i
	}

	req := &sqs.SendMessageBatchRequest{
		Entries:  entries,
		QueueURL: q.URL,
	}

	resp, err := q.SQS.SendMessageBatch(req)
	if err != nil {
		return err
	}
	return newBatchError(id2index, resp.Failed)
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

func (q *Queue) DeleteMessageBatch(receiptHandles ...aws.StringValue) error {
	entries := make([]sqs.DeleteMessageBatchRequestEntry, len(receiptHandles))
	id2index := make(map[string]int)
	for i, rh := range receiptHandles {
		id := aws.String(fmt.Sprintf("msg-%d", i))
		entries[i] = sqs.DeleteMessageBatchRequestEntry{
			ID:            id,
			ReceiptHandle: rh,
		}
		id2index[*id] = i
	}

	req := &sqs.DeleteMessageBatchRequest{
		Entries:  entries,
		QueueURL: q.URL,
	}

	resp, err := q.SQS.DeleteMessageBatch(req)
	if err != nil {
		return err
	}
	return newBatchError(id2index, resp.Failed)
}

func (q *Queue) DeleteQueue() error {
	return q.SQS.DeleteQueue(&sqs.DeleteQueueRequest{
		QueueURL: q.URL,
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
