package queue

import (
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/stripe/aws-go/aws"
	"github.com/stripe/aws-go/gen/sqs"

	"github.com/nabeken/aws-go-sqs/queue/option"
)

func testSQSQueue(name string) (*Queue, error) {
	return New(
		sqs.New(aws.DetectCreds("", "", ""), "ap-northeast-1", nil),
		name,
	)
}

type SendMessageBatchSuite struct {
	suite.Suite

	queue *Queue
}

func (s *SendMessageBatchSuite) SetupSuite() {
	name := os.Getenv("TEST_SQS_QUEUE_NAME")
	if len(name) == 0 {
		s.T().Fatal("TEST_SQS_QUEUE_NAME must be set")
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
	// don't care of the result
	s.queue.PurgeQueue()
}

func (s *SendMessageBatchSuite) TearDownSuite() {
}

func (s *SendMessageBatchSuite) TestSendMessageBatch() {
	attrs := map[string]interface{}{
		"ATTR1": "STRING!!",
		"ATTR2": 12345,
	}

	batchMessages := []BatchMessage{
		BatchMessage{
			Body:    "body1",
			Options: []option.SendMessageRequest{option.MessageAttributes(attrs)},
		},
		BatchMessage{
			Body:    "body2",
			Options: []option.SendMessageRequest{option.MessageAttributes(attrs)},
		},
	}

	if err := s.queue.SendMessageBatch(batchMessages...); !s.NoError(err) {
		return
	}

	messages, err := s.queue.ReceiveMessage(
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
		s.queue.DeleteMessage(m.ReceiptHandle)
	}
}

func TestSendMessageBatchSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test")
	}

	suite.Run(t, new(SendMessageBatchSuite))
}
