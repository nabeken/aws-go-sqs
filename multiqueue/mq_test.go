package multiqueue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/mercari/go-circuitbreaker"
	"github.com/nabeken/aws-go-sqs/v3/queue"
	"github.com/stretchr/testify/assert"
)

func TestDispatcher(t *testing.T) {
	// dummy queue
	q := &queue.Queue{URL: aws.String("dummy")}
	opts := &circuitbreaker.Options{
		Interval:    5 * time.Minute,
		OpenTimeout: 5 * time.Second,
		ShouldTrip:  circuitbreaker.NewTripFuncConsecutiveFailures(1),
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

		assert.Equal(circuitbreaker.StateClosed, d.monitor.curState["dummy"])
	})

	t.Run("dispatch queue", func(t *testing.T) {
		assert := assert.New(t)

		var stateChanged bool
		d.WithOnStateChange(func(q *queue.Queue, oldState, newState circuitbreaker.State) {
			stateChanged = true
		})

		go func() { d.StartStateMonitor(context.TODO()) }()

		t.Log("waiting for monitor")
		time.Sleep(1 * time.Second)

		{
			dq := d.Dispatch()
			assert.Equal(q, dq.Queue)

			givenErr := errors.New("unknown error")
			_, err := dq.Do(context.TODO(), func() (interface{}, error) {
				return nil, givenErr
			})

			assert.EqualError(err, givenErr.Error())
		}

		t.Log("waiting for monitor to detect the state change")
		time.Sleep(2 * time.Second)

		assert.True(stateChanged, "the state should be changed")
		assert.Equal(circuitbreaker.StateOpen, d.monitor.curState["dummy"])

		{
			assert.Len(d.queues, 1)
			assert.Len(d.avail, 0, "there should be no available queues")

			// while the all of the circuit opened, dispatcher returns a queue from the slice
			dq := d.Dispatch()
			assert.Equal(q, dq.Queue)
		}

		t.Log("waiting for the circuit to be half-open")
		time.Sleep(5 * time.Second)
		assert.Equal(circuitbreaker.StateHalfOpen, d.monitor.curState["dummy"])
	})
}
