package multiqueue

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/mercari/go-circuitbreaker"
	"github.com/nabeken/aws-go-sqs/queue"
)

// Dispatcher manages multiple *queue.Queue instances with circit breaker and dispatches it by random or round-robin.
// Circuit breaker is installed per queue. Dispatcher doesn't dispatch a queue while the circuit breaker is open.
type Dispatcher struct {
	// circuit breaker for each queue
	cb            map[string]*circuitbreaker.CircuitBreaker
	onStateChange func(q *queue.Queue, oldState, newState circuitbreaker.State)

	monitor *monitor

	rand *rand.Rand

	// protect queues
	mu sync.Mutex
	// all of the registered queues
	queues []*queue.Queue
	// queues believed to be available
	avail []*queue.Queue
	// index to a queue which will be dispatched next
	nextIndex int
}

// WithOnStateChange installs a hook which will be invoked when the state of the circuit breaker is changed.
func (d *Dispatcher) WithOnStateChange(f func(*queue.Queue, circuitbreaker.State, circuitbreaker.State)) *Dispatcher {
	d.onStateChange = f
	return d
}

// New creates a dispatcher with mercari/go-circuitbreaker enabled per queue.
func New(cbOpts *circuitbreaker.Options, queues ...*queue.Queue) *Dispatcher {
	if len(queues) == 0 {
		panic("at least one queue is required")
	}

	avail := make([]*queue.Queue, len(queues))
	copy(avail, queues)

	d := &Dispatcher{
		queues: queues,
		avail:  avail,
		rand:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	d.buildCircuitBreaker(cbOpts)
	d.buildMonitor()

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
		})
	}
	d.cb = cb
}

func (d *Dispatcher) buildMonitor() {
	mon := &monitor{d: d}
	mon.initState()
	d.monitor = mon
}

func (d *Dispatcher) markUnavailable(q *queue.Queue) {
	d.mu.Lock()
	defer d.mu.Unlock()

	var newAvail []*queue.Queue
	for i := range d.avail {
		if *q.URL != *d.avail[i].URL {
			newAvail = append(newAvail, d.avail[i])
		}
	}

	d.nextIndex = 0
	d.avail = newAvail
}

func (d *Dispatcher) markAvailable(q *queue.Queue) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := range d.avail {
		if *q.URL == *d.avail[i].URL {
			// it exists
			return
		}
	}

	d.nextIndex = 0
	d.avail = append(d.avail, q)
}

func (d *Dispatcher) handleStateChange(q *queue.Queue, prev, cur circuitbreaker.State) {
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
func (d *Dispatcher) dispatchByRR() *queue.Queue {
	if len(d.avail) == 0 {
		return d.dispatchByRandom()
	}

	if d.nextIndex >= len(d.avail) {
		d.nextIndex = 0
	}

	i := d.nextIndex
	d.nextIndex++
	return d.avail[i]
}

// caller of this must hold the lock
func (d *Dispatcher) dispatchByRandom() *queue.Queue {
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

func (d *Dispatcher) dispatch(q *queue.Queue) *Executor {
	return &Executor{
		Queue: q,
		cb:    d.cb[*q.URL],
	}
}

// StartStateMonitor starts the state monitor and it will be blocked until ctx is canceled.
func (d *Dispatcher) StartStateMonitor(ctx context.Context) {
	d.monitor.start(ctx)
}

type monitor struct {
	d *Dispatcher

	mu       sync.Mutex
	curState map[string]circuitbreaker.State
}

func (m *monitor) initState() {
	m.curState = make(map[string]circuitbreaker.State)

	for k, cb := range m.d.cb {
		m.curState[k] = cb.State()
		//log.Printf("%s: init %s", n, m.curState[n])
	}
}

func (m *monitor) start(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, q := range m.d.queues {
				m.checkState(q)
			}
		}
	}
}

func (m *monitor) checkState(q *queue.Queue) {
	m.mu.Lock()
	defer m.mu.Unlock()

	k := *q.URL
	prev := m.curState[k]
	cur := m.d.cb[k].State()

	if prev != cur {
		m.d.handleStateChange(q, prev, cur)
		m.curState[k] = cur
	}
}

// Executor is a wrapper of *queue.Queue with the circuit breaker.
type Executor struct {
	*queue.Queue

	cb *circuitbreaker.CircuitBreaker
}

// Do allows you to call req under the circuit breaker.
func (e *Executor) Do(ctx context.Context, req func() (interface{}, error)) (interface{}, error) {
	return e.cb.Do(ctx, req)
}
