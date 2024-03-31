package queue

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/nabeken/aws-go-sqs/v4/queue/option"
	"github.com/stretchr/testify/suite"
)

func testSQSQueue(name string) (*Queue, error) {
	return New(context.Background(), sqs.New(sqs.Options{}), name)
}

type SendMessageBatchSuite struct {
	suite.Suite

	queue *Queue
}

func (s *SendMessageBatchSuite) SetupSuite() {
	name := os.Getenv("TEST_SQS_QUEUE_NAME")
	if len(name) == 0 {
		s.T().Skip("TEST_SQS_QUEUE_NAME must be set")
	}

	q, err := testSQSQueue(name)
	if err != nil {
		s.T().Fatal(err)
	}

	s.queue = q
}

func (s *SendMessageBatchSuite) SetupTest() {
}

func (s *SendMessageBatchSuite) TearDownTest() {
}

func (s *SendMessageBatchSuite) TearDownSuite() {
	// don't care of the result but logs it
	if err := s.queue.PurgeQueue(context.Background()); err != nil {
		s.T().Log(err)
	}
}

func (s *SendMessageBatchSuite) TestSendMessageBatch() {
	attrs := map[string]interface{}{
		"ATTR1": "STRING!!",
		"ATTR2": 12345,
	}

	batchMessages := []BatchMessage{
		BatchMessage{
			Body:    "body1",
			Options: []option.SendMessageInput{option.MessageAttributes(attrs)},
		},
		BatchMessage{
			Body:    "body2",
			Options: []option.SendMessageInput{option.MessageAttributes(attrs)},
		},
	}

	if err := s.queue.SendMessageBatch(context.Background(), batchMessages...); !s.NoError(err) {
		return
	}

	messages, err := s.queue.ReceiveMessage(
		context.Background(),
		option.MaxNumberOfMessages(5),
		option.UseAllAttribute(),
	)
	if !s.NoError(err) {
		return
	}

	s.Len(messages, 2)

	for i, m := range messages {
		s.Len(m.MessageAttributes, 2)
		for k, a := range m.MessageAttributes {
			mav := option.MessageAttributeValue(attrs[k])
			s.Equal(mav.StringValue, a.StringValue)
		}
		s.Equal(batchMessages[i].Body, *m.Body)
		s.queue.DeleteMessage(context.Background(), m.ReceiptHandle)
	}
}

func (s *SendMessageBatchSuite) TestSendMessageBatchError() {
	attrs := map[string]interface{}{
		"error": "",
	}

	batchMessages := []BatchMessage{
		BatchMessage{
			Body: "success",
		},
		BatchMessage{
			Body:    "failed",
			Options: []option.SendMessageInput{option.MessageAttributes(attrs)},
		},
	}

	if err := s.queue.SendMessageBatch(context.Background(), batchMessages...); s.Error(err) {
		if berrs, ok := IsBatchError(err); s.True(ok, "error must contain *BatchError") {
			s.Len(berrs, 1)

			s.Equal(1, berrs[0].Index, "batchMessages[1] must be error")
			s.Equal("InvalidParameterValue", berrs[0].Code)
			s.Equal(true, berrs[0].SenderFault)
		}
	}

	messages, err := s.queue.ReceiveMessage(
		context.Background(),
		option.MaxNumberOfMessages(5),
		option.UseAllAttribute(),
	)
	if !s.NoError(err) {
		return
	}

	s.Len(messages, 1)
	for _, m := range messages {
		s.queue.DeleteMessage(context.Background(), m.ReceiptHandle)
	}
}

func TestSendMessageBatchSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test")
	}

	suite.Run(t, new(SendMessageBatchSuite))
}
