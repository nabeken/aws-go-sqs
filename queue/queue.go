package queue

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/nabeken/aws-go-sqs/v3/queue/option"
)

// A Queue is an SQS queue which holds queue url in URL.
// Queue allows you to call actions without queue url for every call.
type Queue struct {
	SQS sqsiface.SQSAPI
	URL *string
}

// New initializes Queue with name.
func New(s sqsiface.SQSAPI, name string) (*Queue, error) {
	u, err := GetQueueURL(s, name)
	if err != nil {
		return nil, err
	}

	return &Queue{
		SQS: s,
		URL: u,
	}, nil
}

// MustNew initializes Queue with name.
// It will panic when it fails to initialize a queue.
func MustNew(s sqsiface.SQSAPI, name string) *Queue {
	q, err := New(s, name)
	if err != nil {
		panic(err)
	}
	return q
}

// ChangeMessageVisibility wraps ChangeMessageVisibilityWithContext using context.Background.
func (q *Queue) ChangeMessageVisibility(receiptHandle *string, visibilityTimeout int64) error {
	return q.ChangeMessageVisibilityWithContext(context.Background(), receiptHandle, visibilityTimeout)
}

// ChangeMessageVisibilityWithContext changes a message visibiliy timeout.
func (q *Queue) ChangeMessageVisibilityWithContext(ctx context.Context, receiptHandle *string, visibilityTimeout int64) error {
	req := &sqs.ChangeMessageVisibilityInput{
		ReceiptHandle:     receiptHandle,
		VisibilityTimeout: aws.Int64(visibilityTimeout),
		QueueUrl:          q.URL,
	}
	_, err := q.SQS.ChangeMessageVisibilityWithContext(ctx, req)
	return err
}

// A BatchChangeMessageVisibility represents each request to
// change a visibility timeout.
type BatchChangeMessageVisibility struct {
	ReceiptHandle     *string
	VisibilityTimeout int64
}

// ChangeMessageVisibilityBatch wraps ChangeMessageVisibilityBatchWithContext using context.Background.
func (q *Queue) ChangeMessageVisibilityBatch(opts ...BatchChangeMessageVisibility) error {
	return q.ChangeMessageVisibilityBatchWithContext(context.Background(), opts...)
}

// ChangeMessageVisibilityBatchWithContext changes a visibility timeout for each message in opts.
func (q *Queue) ChangeMessageVisibilityBatchWithContext(ctx context.Context, opts ...BatchChangeMessageVisibility) error {
	entries := make([]*sqs.ChangeMessageVisibilityBatchRequestEntry, len(opts))
	id2index := make(map[string]int)
	for i, b := range opts {
		id := aws.String(fmt.Sprintf("msg-%d", i))
		entries[i] = &sqs.ChangeMessageVisibilityBatchRequestEntry{
			Id:                id,
			ReceiptHandle:     b.ReceiptHandle,
			VisibilityTimeout: aws.Int64(b.VisibilityTimeout),
		}
		id2index[*id] = i
	}

	req := &sqs.ChangeMessageVisibilityBatchInput{
		Entries:  entries,
		QueueUrl: q.URL,
	}

	resp, err := q.SQS.ChangeMessageVisibilityBatchWithContext(ctx, req)
	if err != nil {
		return err
	}
	return NewBatchError(id2index, resp.Failed)
}

// SendMessage wraps SendMessageWithContext using context.Background.
func (q *Queue) SendMessage(body string, opts ...option.SendMessageInput) (*sqs.SendMessageOutput, error) {
	return q.SendMessageWithContext(context.Background(), body, opts...)
}

// SendMessageWithContext sends a message to an SQS queue. opts are used to change parameters for a message.
func (q *Queue) SendMessageWithContext(ctx context.Context, body string, opts ...option.SendMessageInput) (*sqs.SendMessageOutput, error) {
	req := &sqs.SendMessageInput{
		MessageBody: aws.String(body),
		QueueUrl:    q.URL,
	}

	for _, f := range opts {
		f(req)
	}

	return q.SQS.SendMessageWithContext(ctx, req)
}

// A BatchMessage represents each request to send a message.
// Options are used to change parameters for the message.
type BatchMessage struct {
	Body    string
	Options []option.SendMessageInput
}

// A BatchError represents an error for batch operations such as SendMessageBatch and ChangeMessageVisibilityBatch.
// Index can be used to identify a message causing the error.
// See SendMessageBatch how to handle an error in batch operation.
type BatchError struct {
	Index       int
	Code        string
	Message     string
	SenderFault bool
}

// NewBatchError composes an error from errors if available.
func NewBatchError(id2index map[string]int, errors []*sqs.BatchResultErrorEntry) error {
	var result error
	for _, entry := range errors {
		err := &BatchError{
			Index:       id2index[*entry.Id],
			Code:        *entry.Code,
			Message:     *entry.Message,
			SenderFault: *entry.SenderFault,
		}
		result = multierror.Append(result, err)
	}
	return result
}

func (e *BatchError) Error() string {
	return fmt.Sprintf("sqs: index: %d, code: %s, is_sender_fault: %v: message: %s",
		e.Index,
		e.Code,
		e.SenderFault,
		e.Message,
	)
}

// IsBatchError checks that err contains BatchError.
// If err contains BatchError, it returns []*BatchError, true.
// If not, it returns nil, false.
func IsBatchError(err error) (errors []*BatchError, ok bool) {
	merr, mok := err.(*multierror.Error)
	if !mok {
		return nil, false
	}

	for _, e := range merr.Errors {
		berr, ok := e.(*BatchError)
		if ok {
			errors = append(errors, berr)
		}
	}
	return errors, len(errors) > 0
}

// SendMessageBatch wraps SendMessageBatchWithContext using context.Background.
func (q *Queue) SendMessageBatch(messages ...BatchMessage) error {
	return q.SendMessageBatchWithContext(context.Background(), messages...)
}

// SendMessageBatch sends messages to SQS queue.
func (q *Queue) SendMessageBatchWithContext(ctx context.Context, messages ...BatchMessage) error {
	entries, id2index := BuildBatchRequestEntry(messages...)

	req := &sqs.SendMessageBatchInput{
		Entries:  entries,
		QueueUrl: q.URL,
	}

	resp, err := q.SQS.SendMessageBatchWithContext(ctx, req)
	if err != nil {
		return err
	}
	return NewBatchError(id2index, resp.Failed)
}

// ReceiveMessage wraps ReceiveMessageWithContext using context.Background.
func (q *Queue) ReceiveMessage(opts ...option.ReceiveMessageInput) ([]*sqs.Message, error) {
	return q.ReceiveMessageWithContext(context.Background(), opts...)
}

// ReceiveMessage receives messages from SQS queue.
// opts are used to change parameters for a request.
func (q *Queue) ReceiveMessageWithContext(ctx context.Context, opts ...option.ReceiveMessageInput) ([]*sqs.Message, error) {
	req := &sqs.ReceiveMessageInput{
		QueueUrl: q.URL,
	}

	for _, f := range opts {
		f(req)
	}

	resp, err := q.SQS.ReceiveMessageWithContext(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.Messages, nil
}

// DeleteMessage wraps DeleteMessageWithContext using context.Background.
func (q *Queue) DeleteMessage(receiptHandle *string) error {
	return q.DeleteMessageWithContext(context.Background(), receiptHandle)
}

// DeleteMessage deletes a message from SQS queue.
func (q *Queue) DeleteMessageWithContext(ctx context.Context, receiptHandle *string) error {
	_, err := q.SQS.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      q.URL,
		ReceiptHandle: receiptHandle,
	})
	return err
}

// DeleteMessageBatch wraps DeleteMessageBatchWithContext using context.Background.
func (q *Queue) DeleteMessageBatch(receiptHandles ...*string) error {
	return q.DeleteMessageBatchWithContext(context.Background(), receiptHandles...)
}

// DeleteMessageBatchWithContext deletes messages from SQS queue.
func (q *Queue) DeleteMessageBatchWithContext(ctx context.Context, receiptHandles ...*string) error {
	entries := make([]*sqs.DeleteMessageBatchRequestEntry, len(receiptHandles))
	id2index := make(map[string]int)
	for i, rh := range receiptHandles {
		id := aws.String(fmt.Sprintf("msg-%d", i))
		entries[i] = &sqs.DeleteMessageBatchRequestEntry{
			Id:            id,
			ReceiptHandle: rh,
		}
		id2index[*id] = i
	}

	req := &sqs.DeleteMessageBatchInput{
		Entries:  entries,
		QueueUrl: q.URL,
	}

	resp, err := q.SQS.DeleteMessageBatchWithContext(ctx, req)
	if err != nil {
		return err
	}
	return NewBatchError(id2index, resp.Failed)
}

// DeleteQueue wraps DeleteQueueWithContext using context.Background.
func (q *Queue) DeleteQueue() error {
	return q.DeleteQueueWithContext(context.Background())
}

// DeleteQueue deletes a queue in SQS.
func (q *Queue) DeleteQueueWithContext(ctx context.Context) error {
	_, err := q.SQS.DeleteQueueWithContext(ctx, &sqs.DeleteQueueInput{
		QueueUrl: q.URL,
	})
	return err
}

// PurgeQueue wraps PurgeQueueWithContext using context.Background.
func (q *Queue) PurgeQueue() error {
	return q.PurgeQueueWithContext(context.Background())
}

// PurgeQueue purges messages in SQS queue.
// It deletes all messages in SQS queue.
func (q *Queue) PurgeQueueWithContext(ctx context.Context) error {
	_, err := q.SQS.PurgeQueueWithContext(ctx, &sqs.PurgeQueueInput{
		QueueUrl: q.URL,
	})
	return err
}

// GetQueueURL wraps GetQueueURLWithContext using context.Background.
func GetQueueURL(s sqsiface.SQSAPI, name string) (*string, error) {
	return GetQueueURLWithContext(context.Background(), s, name)
}

// GetQueueURLWithContext returns a URL for the given queue name.
func GetQueueURLWithContext(ctx context.Context, s sqsiface.SQSAPI, name string) (*string, error) {
	req := &sqs.GetQueueUrlInput{
		QueueName: aws.String(name),
	}

	resp, err := s.GetQueueUrlWithContext(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.QueueUrl, nil
}

// BuildBatchRequestEntry builds batch entries and id2index map.
func BuildBatchRequestEntry(messages ...BatchMessage) ([]*sqs.SendMessageBatchRequestEntry, map[string]int) {
	entries := make([]*sqs.SendMessageBatchRequestEntry, len(messages))
	id2index := make(map[string]int)
	for i, bm := range messages {
		req := &sqs.SendMessageInput{}
		for _, f := range bm.Options {
			f(req)
		}

		id := aws.String(fmt.Sprintf("msg-%d", i))
		entries[i] = &sqs.SendMessageBatchRequestEntry{
			DelaySeconds:      req.DelaySeconds,
			MessageAttributes: req.MessageAttributes,
			MessageBody:       aws.String(bm.Body),
			Id:                id,
		}
		id2index[*id] = i
	}

	return entries, id2index
}
