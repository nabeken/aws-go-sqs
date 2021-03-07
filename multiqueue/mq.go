package multiqueue

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/mercari/go-circuitbreaker"
	"github.com/nabeken/aws-go-sqs/v3/queue"
)

type Queue struct {
	*queue.Queue

	// weight
	w int
}

func NewQueue(q *queue.Queue) *Queue {
	return &Queue{Queue: q}
}

func (q *Queue) Weight(w int) *Queue {
	q.w = w
	return q
}

// Dispatcher manages multiple *Queue instances with circit breaker and dispatches it by random or round-robin.
// Circuit breaker is installed per queue. Dispatcher doesn't dispatch a queue while the circuit breaker is open.
type Dispatcher struct {
	// circuit breaker for each queue
	cb            map[string]*circuitbreaker.CircuitBreaker
	onStateChange func(q *Queue, oldState, newState circuitbreaker.State)

	rand *rand.Rand

	// protect queues
	mu sync.Mutex
	// all of the registered queues
	queues []*Queue
	// queues believed to be available
	avail []*Queue
	wq    *WeightedQueues
}

// WithOnStateChange installs a hook which will be invoked when the state of the circuit breaker is changed.
func (d *Dispatcher) WithOnStateChange(f func(*Queue, circuitbreaker.State, circuitbreaker.State)) *Dispatcher {
	d.onStateChange = f
	return d
}

// New creates a dispatcher with mercari/go-circuitbreaker enabled per queue.
func New(cbOpts *circuitbreaker.Options, queues ...*Queue) *Dispatcher {
	if len(queues) == 0 {
		panic("at least one queue is required")
	}

	avail := make([]*Queue, len(queues))
	copy(avail, queues)

	d := &Dispatcher{
		queues: queues,
		avail:  avail,
		wq:     NewWeightedQueues(avail),
		rand:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	d.buildCircuitBreaker(cbOpts)

	return d
}

func (d *Dispatcher) GetExecutors() []*Executor {
	var execs []*Executor
	for i := range d.queues {
		execs = append(execs, d.dispatch(d.queues[i]))
	}
	return execs
}

func (d *Dispatcher) buildCircuitBreaker(opts *circuitbreaker.Options) {
	cb := map[string]*circuitbreaker.CircuitBreaker{}
	for i := range d.queues {
		q := d.queues[i]
		cb[*q.URL] = circuitbreaker.New(&circuitbreaker.Options{
			Interval:              opts.Interval,
			OpenTimeout:           opts.OpenTimeout,
			OpenBackOff:           opts.OpenBackOff,
			HalfOpenMaxSuccesses:  opts.HalfOpenMaxSuccesses,
			ShouldTrip:            opts.ShouldTrip,
			FailOnContextCancel:   opts.FailOnContextCancel,
			FailOnContextDeadline: opts.FailOnContextDeadline,
			OnStateChange: func(from, to circuitbreaker.State) {
				d.handleStateChange(q, from, to)
			},
		})
	}
	d.cb = cb
}

func (d *Dispatcher) markUnavailable(q *Queue) {
	d.mu.Lock()
	defer d.mu.Unlock()

	var newAvail []*Queue
	for i := range d.avail {
		if *q.URL != *d.avail[i].URL {
			newAvail = append(newAvail, d.avail[i])
		}
	}

	d.avail = newAvail
	d.wq = NewWeightedQueues(d.avail)
}

func (d *Dispatcher) markAvailable(q *Queue) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := range d.avail {
		if *q.URL == *d.avail[i].URL {
			// it exists
			return
		}
	}

	d.avail = append(d.avail, q)
	d.wq = NewWeightedQueues(d.avail)
}

func (d *Dispatcher) handleStateChange(q *Queue, prev, cur circuitbreaker.State) {
	if f := d.onStateChange; f != nil {
		f(q, prev, cur)
	}

	switch cur {
	case circuitbreaker.StateOpen:
		d.markUnavailable(q)
	case circuitbreaker.StateHalfOpen, circuitbreaker.StateClosed:
		d.markAvailable(q)
	default:
		panic(fmt.Sprintf("unknown state: %s -> %s", prev, cur))
	}
}

// DispatchByRR dispatches Executor by round-robin fasion.
func (d *Dispatcher) DispatchByRR() *Executor {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dispatch(d.dispatchByRR())
}

// caller of this must hold the lock
func (d *Dispatcher) dispatchByRR() *Queue {
	if len(d.avail) == 0 {
		return d.dispatchByRandom()
	}

	return d.wq.Next()
}

// caller of this must hold the lock
func (d *Dispatcher) dispatchByRandom() *Queue {
	// when there is no available queue, it will choose a queue from all of the registered queues
	if len(d.avail) > 0 {
		return d.avail[d.rand.Intn(len(d.avail))]
	}
	return d.queues[d.rand.Intn(len(d.queues))]
}

// Dispatch dispatches Executor by random.
func (d *Dispatcher) Dispatch() *Executor {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dispatch(d.dispatchByRandom())
}

func (d *Dispatcher) dispatch(q *Queue) *Executor {
	return &Executor{
		Queue: q,
		cb:    d.cb[*q.URL],
	}
}

// Executor is a wrapper of *Queue with the circuit breaker.
type Executor struct {
	*Queue

	cb *circuitbreaker.CircuitBreaker
}

// Do allows you to call req under the circuit breaker.
func (e *Executor) Do(ctx context.Context, req func() (interface{}, error)) (interface{}, error) {
	return e.cb.Do(ctx, req)
}

// maxWeight returns a maxium weight in the given queues.
func maxWeight(queues []*Queue) int {
	if len(queues) == 0 {
		return 0
	}

	w := queues[0].w
	for i := range queues[1:] {
		if nw := queues[i].w; nw > w {
			w = nw
		}
	}
	return w
}

// WeightedQueues represents the Interleaved Weighted Round-Robin algorithm.
// See https://en.wikipedia.org/wiki/Weighted_round_robin#Interleaved_WRR
type WeightedQueues struct {
	q []*Queue

	mu        sync.Mutex
	maxWeight int
	round     int
	nextIndex int
}

func NewWeightedQueues(q []*Queue) *WeightedQueues {
	max := maxWeight(q)
	if max == 0 {
		for i := range q {
			q[i].w = 1
		}
	}

	return &WeightedQueues{
		q: q,

		maxWeight: maxWeight(q),
	}
}

// Next returns a next queue and updates the internal state.
func (wq *WeightedQueues) Next() *Queue {
	wq.mu.Lock()
	defer wq.mu.Unlock()

AGAIN:
	for wq.round < wq.maxWeight {
		for wq.nextIndex < len(wq.q) {
			n := wq.nextIndex
			wq.nextIndex++

			// FIXME:
			//fmt.Printf("DEBUG: maxWeight: %d, round: %d, index: %d, weight[%d]: %d, nextIndex: %d\n",
			//	wq.maxWeight,
			//	wq.round,
			//	n,
			//	n,
			//	wq.q[n].w,
			//	wq.nextIndex,
			//)

			if wq.q[n].w > wq.round {
				return wq.q[n]
			}
		}
		if wq.nextIndex >= len(wq.q) {
			wq.round++
			wq.nextIndex = 0
		}
	}
	if wq.round >= wq.maxWeight {
		wq.round = 0
	}

	// go to the for-loop again since round is wrapped

	goto AGAIN

	panic("not reachable")
}
