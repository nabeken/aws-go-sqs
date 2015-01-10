package queue

import (
	"github.com/nabeken/aws-go-sqs/queue/option"

	"github.com/stripe/aws-go/aws"
	"github.com/stripe/aws-go/gen/sqs"
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
