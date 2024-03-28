package multiqueue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/mercari/go-circuitbreaker"
	"github.com/nabeken/aws-go-sqs/v4/queue"
	"github.com/stretchr/testify/assert"
)

func TestDispatcher(t *testing.T) {
	// dummy queue
	q := NewQueue(testDummyQueue("dummy"))
	opts := []circuitbreaker.BreakerOption{
		circuitbreaker.WithCounterResetInterval(5 * time.Minute),
		circuitbreaker.WithOpenTimeout(5 * time.Second),
		circuitbreaker.WithTripFunc(circuitbreaker.NewTripFuncConsecutiveFailures(1)),
	}

	d := New(opts, q)

	t.Run("initial state", func(t *testing.T) {
		assert := assert.New(t)

		if assert.Len(d.queues, 1) {
			assert.Equal(q, d.queues[0])
		}

		if assert.Len(d.avail, 1) {
			assert.Equal(q, d.avail[0])
		}

		assert.Equal(circuitbreaker.StateClosed, d.cb["dummy"].State())
	})

	t.Run("dispatch queue", func(t *testing.T) {
		assert := assert.New(t)

		var stateChanged bool
		d.WithOnStateChange(func(q *Queue, oldState, newState circuitbreaker.State) {
			stateChanged = true
		})

		{
			dq := d.Dispatch()
			assert.Equal(q, dq.Queue)
			assert.Equal(q, d.DispatchByRR().Queue)

			givenErr := errors.New("unknown error")
			_, err := dq.Do(context.TODO(), func() (interface{}, error) {
				return nil, givenErr
			})

			assert.EqualError(err, givenErr.Error())
		}

		t.Log("waiting for the state change")
		time.Sleep(2 * time.Second)

		assert.True(stateChanged, "the state should be changed")
		assert.Equal(circuitbreaker.StateOpen, d.cb["dummy"].State())

		{
			assert.Len(d.queues, 1)
			assert.Len(d.avail, 0, "there should be no available queues")

			// while the all of the circuit opened, dispatcher returns a queue from the slice
			dq := d.Dispatch()
			assert.Equal(q, dq.Queue)
			assert.Equal(q, d.DispatchByRR().Queue)
		}

		t.Log("waiting for the circuit to be half-open")
		time.Sleep(5 * time.Second)
		assert.Equal(circuitbreaker.StateHalfOpen, d.cb["dummy"].State())
	})
}

func TestDispatcher_DispatchByRR(t *testing.T) {
	opts := []circuitbreaker.BreakerOption{
		circuitbreaker.WithCounterResetInterval(5 * time.Minute),
		circuitbreaker.WithOpenTimeout(5 * time.Second),
		circuitbreaker.WithTripFunc(circuitbreaker.NewTripFuncConsecutiveFailures(1)),
	}
	d := New(
		opts,
		NewQueue(testDummyQueue("dummy1")),
		NewQueue(testDummyQueue("dummy2")),
	)

	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Log("waiting for the state change")
	time.Sleep(1 * time.Second)

	assert := assert.New(t)
	assert.Equal(d.queues[0], d.DispatchByRR().Queue)
	assert.Equal(d.queues[1], d.DispatchByRR().Queue)
	assert.Equal(d.queues[0], d.DispatchByRR().Queue)
	assert.Equal(d.queues[1], d.DispatchByRR().Queue)
}

func TestMaxWeight(t *testing.T) {
	assert.Equal(t, 5, maxWeight([]*Queue{&Queue{w: 1}, &Queue{w: 5}, &Queue{w: 2}}))
}

func TestWeightedQueues(t *testing.T) {
	wq := NewWeightedQueues([]*Queue{
		{w: 1, Queue: testDummyQueue("dummy1")},
		{w: 5, Queue: testDummyQueue("dummy2")},
		{w: 2, Queue: testDummyQueue("dummy3")},
	})

	for i := 0; i < 3; i++ {
		for _, expected := range []string{
			// round 0
			"dummy1",
			"dummy2",
			"dummy3",

			// round 1
			"dummy2",
			"dummy3",

			// round 2
			"dummy2",

			// round 3
			"dummy2",

			// round 4
			"dummy2",
		} {
			assertQueue(t, expected, wq.Next())
		}
	}
}

func testDummyQueue(qn string) *queue.Queue {
	return &queue.Queue{URL: aws.String(qn)}
}

func assertQueue(t *testing.T, expected string, q *Queue) bool {
	return assert.Equal(t, expected, *q.URL)
}
