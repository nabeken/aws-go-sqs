package queue_test

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/nabeken/aws-go-sqs/v4/queue"
	"github.com/nabeken/aws-go-sqs/v4/queue/option"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type realSQSTestEnv struct {
	queue *queue.Queue
}

func setupRealSQSTestenv(t *testing.T) *realSQSTestEnv {
	name := os.Getenv("TEST_SQS_QUEUE_NAME")
	if len(name) == 0 {
		t.Skip("TEST_SQS_QUEUE_NAME must be set")
	}

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		t.Fatalf("loading AWS config: %s", err.Error())
	}

	q, err := queue.New(context.TODO(), sqs.NewFromConfig(cfg), name)
	if err != nil {
		t.Fatal(err)
	}

	env := &realSQSTestEnv{
		queue: q,
	}

	t.Cleanup(func() {
		t.Log("purging the queue...")

		if err := env.queue.PurgeQueue(context.TODO()); err != nil {
			t.Log(err)
		}
	})

	return env
}

func TestSendMessageBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test")
	}

	env := setupRealSQSTestenv(t)

	t.Run("OK", func(t *testing.T) {
		attrs := map[string]interface{}{
			"ATTR1": "STRING!!",
			"ATTR2": 12345,
		}

		batchMessages := []queue.BatchMessage{
			{
				Body:    "body1",
				Options: []option.SendMessageInput{option.MessageAttributes(attrs)},
			},
			{
				Body:    "body2",
				Options: []option.SendMessageInput{option.MessageAttributes(attrs)},
			},
		}

		err := env.queue.SendMessageBatch(context.Background(), batchMessages...)
		require.NoError(t, err)

		messages, err := env.queue.ReceiveMessage(
			context.Background(),
			option.MaxNumberOfMessages(5),
			option.UseAllAttribute(),
		)

		require.NoError(t, err)
		assert.Len(t, messages, 2)

		for i, m := range messages {
			assert.Len(t, m.MessageAttributes, 2)
			for k, a := range m.MessageAttributes {
				mav := option.MessageAttributeValue(attrs[k])
				assert.Equal(t, mav.StringValue, a.StringValue)
			}
			assert.Equal(t, batchMessages[i].Body, *m.Body)
			env.queue.DeleteMessage(context.TODO(), m.ReceiptHandle)
		}
	})

	t.Run("Error", func(t *testing.T) {
		attrs := map[string]interface{}{
			"error": "",
		}

		batchMessages := []queue.BatchMessage{
			{
				Body: "success",
			},
			{
				Body:    "failed",
				Options: []option.SendMessageInput{option.MessageAttributes(attrs)},
			},
		}

		err := env.queue.SendMessageBatch(context.Background(), batchMessages...)
		require.Error(t, err)

		berrs, ok := queue.IsBatchError(err)

		require.True(t, ok, "error must contain *BatchError")

		assert.Len(t, berrs, 1)
		assert.Equal(t, 1, berrs[0].Index, "batchMessages[1] must be error")
		assert.Equal(t, "InvalidParameterValue", berrs[0].Code)
		assert.Equal(t, true, berrs[0].SenderFault)

		messages, err := env.queue.ReceiveMessage(
			context.Background(),
			option.MaxNumberOfMessages(5),
			option.UseAllAttribute(),
		)

		require.NoError(t, err)

		assert.Len(t, messages, 1)
		for _, m := range messages {
			env.queue.DeleteMessage(context.TODO(), m.ReceiptHandle)
		}
	})
}
