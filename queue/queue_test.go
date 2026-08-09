package queue_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	"github.com/nabeken/aws-go-sqs/v4/queue"
	"github.com/nabeken/aws-go-sqs/v4/queue/option"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const envTestSQSQueueName = "TEST_SQS_QUEUE_NAME"

type realSQSTestEnv struct {
	queue *queue.Queue
}

// lookupTestQueueName returns the queue name in TEST_SQS_QUEUE_NAME,
// skipping the test if it is unset.
func lookupTestQueueName(t *testing.T) string {
	t.Helper()

	name := os.Getenv(envTestSQSQueueName)
	if len(name) == 0 {
		t.Skipf("%s must be set", envTestSQSQueueName)
	}
	return name
}

// setupRealSQSTestEnv sets up a realSQSTestEnv backed by the queue named
// name.
func setupRealSQSTestEnv(t *testing.T, name string) *realSQSTestEnv {
	t.Helper()

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
	return setupRealSQSTestEnv(t, lookupTestQueueName(t))
}

// setupFIFOQueueTestEnv sets up a realSQSTestEnv for the FIFO queue
// exercised by TestFIFOQueue. The FIFO queue name is derived from
// TEST_SQS_QUEUE_NAME by appending the required ".fifo" suffix, so a single
// environment variable configures both the standard and FIFO test queues.
func setupFIFOQueueTestEnv(t *testing.T) *realSQSTestEnv {
	t.Helper()
	return setupRealSQSTestEnv(t, lookupTestQueueName(t)+".fifo")
}

// newDeduplicationID returns a MessageDeduplicationId that is unique to the
// running test, since FIFO queues suppress duplicate sends of the same ID
// within a 5 minute window.
func newDeduplicationID(t *testing.T, suffix string) string {
	t.Helper()
	return fmt.Sprintf("%s-%s-%d", t.Name(), suffix, time.Now().UnixNano())
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

// TestFIFOQueue exercises queue operations against a real FIFO SQS queue
// named by TEST_SQS_QUEUE_NAME + ".fifo". FIFO queues require MessageGroupId
// on every send and either MessageDeduplicationId or content-based
// deduplication, so every SendMessage/SendMessageBatch call below sets both
// explicitly.
func TestFIFOQueue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test")
	}

	env := setupFIFOQueueTestEnv(t)

	t.Run("SendMessageWithMessageGroupId", func(t *testing.T) {
		groupID := "tenant-123"

		_, err := env.queue.SendMessage(
			context.Background(),
			"body",
			option.MessageGroupId(groupID),
			option.MessageDeduplicationId(newDeduplicationID(t, "single")),
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

	t.Run("Deduplication", func(t *testing.T) {
		groupID := "tenant-dedup"
		dedupID := newDeduplicationID(t, "dup")

		_, err := env.queue.SendMessage(
			context.Background(),
			"first",
			option.MessageGroupId(groupID),
			option.MessageDeduplicationId(dedupID),
		)
		require.NoError(t, err)

		_, err = env.queue.SendMessage(
			context.Background(),
			"second",
			option.MessageGroupId(groupID),
			option.MessageDeduplicationId(dedupID),
		)
		require.NoError(t, err)

		messages, err := env.queue.ReceiveMessage(
			context.Background(),
			option.WaitTimeSeconds(1),
			option.MaxNumberOfMessages(5),
			option.UseAllAttribute(),
		)
		require.NoError(t, err)
		require.Len(t, messages, 1, "the second send must be suppressed as a duplicate")

		assert.Equal(t, "first", *messages[0].Body)

		env.queue.DeleteMessage(context.TODO(), messages[0].ReceiptHandle)
	})

	t.Run("SendMessageBatch", func(t *testing.T) {
		t.Run("OK", func(t *testing.T) {
			// All messages share a MessageGroupId so FIFO ordering applies
			// and the receive order must match the send order.
			groupID := "tenant-batch"

			attrs := map[string]interface{}{
				"ATTR1": "STRING!!",
				"ATTR2": 12345,
			}

			batchMessages := []queue.BatchMessage{
				{
					Body: "body1",
					Options: []option.SendMessageInput{
						option.MessageAttributes(attrs),
						option.MessageGroupId(groupID),
						option.MessageDeduplicationId(newDeduplicationID(t, "batch-1")),
					},
				},
				{
					Body: "body2",
					Options: []option.SendMessageInput{
						option.MessageAttributes(attrs),
						option.MessageGroupId(groupID),
						option.MessageDeduplicationId(newDeduplicationID(t, "batch-2")),
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
			require.Len(t, messages, 2)

			for i, m := range messages {
				assert.Len(t, m.MessageAttributes, 2)
				for k, a := range m.MessageAttributes {
					mav := option.MessageAttributeValue(attrs[k])
					assert.Equal(t, mav.StringValue, a.StringValue)
				}
				assert.Equal(t, batchMessages[i].Body, *m.Body, "FIFO queue must preserve send order within a message group")
				assert.Equal(t, groupID, m.Attributes["MessageGroupId"])
				env.queue.DeleteMessage(context.TODO(), m.ReceiptHandle)
			}
		})

		t.Run("Error", func(t *testing.T) {
			batchMessages := []queue.BatchMessage{
				{
					Body: "success",
					Options: []option.SendMessageInput{
						option.MessageGroupId("group-1"),
						option.MessageDeduplicationId(newDeduplicationID(t, "error-success")),
					},
				},
				{
					Body: "failed",
					Options: []option.SendMessageInput{
						option.MessageDeduplicationId(newDeduplicationID(t, "error-failed")),
					},
				},
			}

			// With FIFO queue, the validation error is returned as the whole response, not a batch result error

			err := env.queue.SendMessageBatch(context.Background(), batchMessages...)
			require.Error(t, err)

			var ae smithy.APIError
			assert.True(t, errors.As(err, &ae))
			assert.Equal(t, "InvalidParameterValue", ae.ErrorCode())
		})
	})
}
