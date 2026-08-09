package queue_test

import (
	"context"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/nabeken/aws-go-sqs/v4/queue"
	"github.com/nabeken/aws-go-sqs/v4/queue/option"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const envTestSQSQueueName = "TEST_SQS_QUEUE_NAME"

type realSQSTestEnv struct {
	queue *queue.Queue
}

// setupRealSQSTestEnv sets up a realSQSTestEnv backed by the queue named in
// the envVar environment variable. It skips the test if envVar is unset.
func setupRealSQSTestEnv(t *testing.T, envVar string) *realSQSTestEnv {
	t.Helper()

	name := os.Getenv(envVar)
	if len(name) == 0 {
		t.Skipf("%s must be set", envVar)
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

// setupStandardQueueTestEnv sets up a realSQSTestEnv for the standard queue
// exercised by TestStandardQueue.
func setupStandardQueueTestEnv(t *testing.T) *realSQSTestEnv {
	t.Helper()
	return setupRealSQSTestEnv(t, envTestSQSQueueName)
}

func TestBuildBatchRequestEntryWithMessageGroupId(t *testing.T) {
	entries, _ := queue.BuildBatchRequestEntry(
		queue.BatchMessage{
			Body: "body1",
		},
		queue.BatchMessage{
			Body:    "body2",
			Options: []option.SendMessageInput{option.MessageGroupId("tenant-123")},
		},
	)

	require.Len(t, entries, 2)
	assert.Nil(t, entries[0].MessageGroupId)
	require.NotNil(t, entries[1].MessageGroupId)
	assert.Equal(t, "tenant-123", *entries[1].MessageGroupId)
}

// TestStandardQueue exercises queue operations against a real standard SQS
// queue named by TEST_SQS_QUEUE_NAME.
func TestStandardQueue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test")
	}

	env := setupStandardQueueTestEnv(t)

	t.Run("SendMessageWithMessageGroupId", func(t *testing.T) {
		groupID := "tenant-123"

		_, err := env.queue.SendMessage(
			context.Background(),
			"body",
			option.MessageGroupId(groupID),
		)
		require.NoError(t, err)

		messages, err := env.queue.ReceiveMessage(
			context.Background(),
			option.WaitTimeSeconds(1),
			option.MaxNumberOfMessages(1),
			option.UseAttributes("All"),
		)
		require.NoError(t, err)
		require.Len(t, messages, 1)

		assert.Equal(t, groupID, messages[0].Attributes["MessageGroupId"])

		env.queue.DeleteMessage(context.TODO(), messages[0].ReceiptHandle)
	})

	t.Run("SendMessageBatch", func(t *testing.T) {
		t.Run("OK", func(t *testing.T) {
			groupIDs := []string{
				"tenant-1",
				"tenant-2",
			}

			attrs := map[string]interface{}{
				"ATTR1": "STRING!!",
				"ATTR2": 12345,
			}

			batchMessages := []queue.BatchMessage{
				{
					Body: "body1",
					Options: []option.SendMessageInput{
						option.MessageAttributes(attrs),
						option.MessageGroupId(groupIDs[0]),
					},
				},
				{
					Body: "body2",
					Options: []option.SendMessageInput{
						option.MessageAttributes(attrs),
						option.MessageGroupId(groupIDs[1]),
					},
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

			slices.SortFunc(messages, func(a, b types.Message) int {
				return strings.Compare(*a.Body, *b.Body)
			})

			for i, m := range messages {
				assert.Len(t, m.MessageAttributes, 2)
				for k, a := range m.MessageAttributes {
					mav := option.MessageAttributeValue(attrs[k])
					assert.Equal(t, mav.StringValue, a.StringValue)
				}
				assert.Equal(t, batchMessages[i].Body, *m.Body)
				assert.Equal(t, groupIDs[i], m.Attributes["MessageGroupId"])
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
	})
}
