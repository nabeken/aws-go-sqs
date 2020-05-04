package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/mercari/go-circuitbreaker"
	"github.com/nabeken/aws-go-sqs/multiqueue"
	"github.com/nabeken/aws-go-sqs/queue"
	"github.com/nabeken/aws-go-sqs/queue/option"
)

func main() {
	var queueName1 = flag.String("queue1", "", "specify SQS queue name 1")
	var queueName2 = flag.String("queue2", "", "specify SQS queue name 2")
	var drain = flag.Bool("drain", false, "drain")
	var concurrency = flag.Int("concurrency", 1, "specify concurrency")
	var count = flag.Int("count", 10000, "number of messages")

	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	if *queueName1 == "" || *queueName2 == "" {
		log.Fatal("Please specify queue name")
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sig := <-c
		log.Println("got signal:", sig)
		cancel()
	}()

	// Create SQS instance
	s := sqs.New(session.Must(session.NewSession()))

	// Create Queue instance
	q1 := queue.MustNew(s, *queueName1)
	q2 := queue.MustNew(s, *queueName2)

	// if we do not set OpenTimeout nor OpenBackOff, the default value of OpenBackOff will be used.
	cbOpts := &circuitbreaker.Options{
		Interval:   1 * time.Minute,
		ShouldTrip: circuitbreaker.NewTripFuncFailureRate(100, 0.7),
	}

	d := multiqueue.New(cbOpts, q1, q2).
		WithOnStateChange(func(q *queue.Queue, oldState, newState circuitbreaker.State) {
			log.Printf("%s: state has been changed from %s to %s", *q.URL, oldState, newState)
		})

	fss := &failureScenarioServer{
		q: q1,
	}
	go func() {
		log.Print("starting failure injection HTTP server...")
		http.ListenAndServe("127.0.0.1:9003", fss)
	}()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		d.StartStateMonitor(ctx)
	}()

	cntCh := make(chan int64, *concurrency)
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cntCh <- recv(ctx, d)
		}()
	}

	if !*drain {
		wg.Add(1)
		go func() {
			defer wg.Done()
			send(ctx, *count, *concurrency, d, fss)
		}()
	}

	wg.Wait()

	var total int64
	for i := 0; i < *concurrency; i++ {
		total += <-cntCh
	}

	log.Printf("done! total: %d", total)
}

func send(ctx context.Context, count, concurrency int, d *multiqueue.Dispatcher, fss *failureScenarioServer) {
	// MessageAttributes
	attrs := map[string]interface{}{
		"ATTR1": "STRING!!",
		"ATTR2": 12345,
	}

	sem := make(chan struct{}, concurrency)

LOOP:
	for i := 0; i < count; i++ {
		sem <- struct{}{}
		cnt := i + 1
		go func() {
			defer func() { <-sem }()
			for {
				exec := d.Dispatch()
				_, err := exec.Do(ctx, func() (interface{}, error) {
					if err := fss.failureScenario(exec.Queue); err != nil {
						return nil, err
					}

					return exec.SendMessage(fmt.Sprintf("MESSAGE BODY FROM MULTI-QUEUE %d", cnt), option.MessageAttributes(attrs))
				})

				if err != nil {
					log.Printf("%s: unable to send the message. will retry: %s", *exec.Queue.URL, err)
				} else {
					log.Printf("%s: the message has been sent (%d)", *exec.Queue.URL, cnt)
					break
				}
			}
		}()

		select {
		case <-ctx.Done():
			break LOOP
		default:
		}
	}

	log.Print("sent!")
}

func recv(ctx context.Context, d *multiqueue.Dispatcher) int64 {
	log.Print("starting receiver...")

	var cnt int64
	for {
		select {
		case <-ctx.Done():
			log.Printf("shutting down receiver... count:%d", cnt)
			return cnt
		default:
		}

		exec := d.DispatchByRR()
		resp, err := exec.ReceiveMessage(
			option.MaxNumberOfMessages(10),
			option.WaitTimeSeconds(0),
		)
		if err != nil {
			log.Printf("unable to receive message: %s", err)
		}

		for _, m := range resp {
			if err := exec.DeleteMessage(m.ReceiptHandle); err != nil {
				log.Printf("unable to delete message: %s", err)
			}

			cnt++
		}
	}
}

type failureScenarioServer struct {
	// queue going to be failed
	q *queue.Queue

	mu      sync.Mutex
	until   time.Time
	errRate float64
}

func (s *failureScenarioServer) failureScenario(q *queue.Queue) error {
	// q is not the queue we want to be failed
	if q != s.q {
		return nil
	}

	if time.Now().Before(s.until) {
		if rand.Float64() > s.errRate {
			return nil
		}
		return fmt.Errorf("this is a failure scenario until %s", s.until.Format(time.RFC3339))
	}

	return nil
}

func (s *failureScenarioServer) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if err := req.ParseForm(); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	dur, err := time.ParseDuration(req.Form.Get("duration"))
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	errRate, err := strconv.ParseFloat(req.Form.Get("error_rate"), 64)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.errRate = errRate
	s.until = time.Now().Add(dur)
}
