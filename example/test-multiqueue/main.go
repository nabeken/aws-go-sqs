package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/mercari/go-circuitbreaker"
	"github.com/nabeken/aws-go-sqs/v4/multiqueue"
	"github.com/nabeken/aws-go-sqs/v4/queue"
	"github.com/nabeken/aws-go-sqs/v4/queue/option"
)

var errGotSignal = errors.New("got a signal")

func main() {
	var queueName1 = flag.String("queue1", "", "specify SQS queue name 1")
	var region1 = flag.String("region1", "ap-northeast-1", "specify a region for queue1")
	var weight1 = flag.Int("weight1", 1, "specify a weight for queue1")

	var queueName2 = flag.String("queue2", "", "specify SQS queue name 2")
	var region2 = flag.String("region2", "ap-southeast-1", "specify a region for queue2")
	var weight2 = flag.Int("weight2", 1, "specify a weight for queue2")

	var drain = flag.Bool("drain", false, "drain")
	var concurrency = flag.Int("concurrency", 1, "specify concurrency")
	var count = flag.Int("count", 10000, "number of messages")

	flag.Parse()

	randSource := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(randSource)

	if *queueName1 == "" || *queueName2 == "" {
		log.Fatal("Please specify queue name")
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	ctx, cancel := context.WithCancelCause(context.Background())
	go func() {
		sig := <-c
		log.Println("got signal:", sig)
		cancel(errGotSignal)
	}()

	tr := &http.Transport{
		MaxIdleConns:        *concurrency * 2,
		MaxIdleConnsPerHost: *concurrency * 2,

		MaxConnsPerHost: *concurrency * 2,

		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,

		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,

		ForceAttemptHTTP2: true,
	}

	httpClient := &http.Client{
		Transport: tr,
		Timeout:   time.Minute,
	}

	// Create SQS instance for region1
	cfg, err := config.LoadDefaultConfig(ctx, config.WithHTTPClient(httpClient))
	if err != nil {
		log.Fatalf("loading AWS config: %s", err.Error())
	}

	s1 := sqs.NewFromConfig(cfg, func(opts *sqs.Options) {
		opts.Region = *region1
	})

	s2 := sqs.NewFromConfig(cfg, func(opts *sqs.Options) {
		opts.Region = *region2
	})

	// Create Queue instance
	q1 := multiqueue.NewQueue(queue.MustNew(ctx, s1, *queueName1)).Weight(*weight1)
	q2 := multiqueue.NewQueue(queue.MustNew(ctx, s2, *queueName2)).Weight(*weight2)

	// if we do not set OpenTimeout nor OpenBackOff, the default value of OpenBackOff will be used.
	cbOpts := []circuitbreaker.BreakerOption{
		circuitbreaker.WithCounterResetInterval(1 * time.Minute),
		circuitbreaker.WithTripFunc(circuitbreaker.NewTripFuncFailureRate(100, 0.7)),
	}

	d := multiqueue.New(cbOpts, q1, q2).
		WithOnStateChange(func(q *multiqueue.Queue, oldState, newState circuitbreaker.State) {
			log.Printf("%s: state has been changed from %s to %s", *q.URL, oldState, newState)
		})

	fss := &failureScenarioServer{
		scenario: []failureScenario{
			{URL: *q1.URL},
			{URL: *q2.URL},
		},
		rng: rng,
	}
	go func() {
		log.Print("starting failure injection HTTP server...")
		_ = http.ListenAndServe("127.0.0.1:9003", fss)
	}()

	var wg sync.WaitGroup

	execs := d.GetExecutors()
	messagesCh := make(chan []string, *concurrency*len(execs))

	for _, exec := range execs {
		for i := 0; i < *concurrency; i++ {
			wg.Add(1)
			go func(exec *multiqueue.Executor) {
				defer wg.Done()
				messagesCh <- recv(ctx, exec)
			}(exec)
		}
	}

	if !*drain {
		wg.Add(1)
		go func() {
			defer wg.Done()
			send(ctx, *count, *concurrency, d, fss)
		}()
	}

	wg.Wait()

	var messages []string
	for i := 0; i < *concurrency*len(execs); i++ {
		messages = append(messages, <-messagesCh...)
	}

	total := len(messages)
	uniqueTotal := len(uniq(messages))

	log.Printf("done! total: %d unique: %d", total, uniqueTotal)
}

func uniq(ss []string) []string {
	m := map[string]struct{}{}
	var ret []string
	for _, s := range ss {
		if _, found := m[s]; !found {
			m[s] = struct{}{}
			ret = append(ret, s)
		}
	}
	return ret
}

func send(ctx context.Context, count, concurrency int, d *multiqueue.Dispatcher, fss *failureScenarioServer) {
	// MessageAttributes
	attrs := map[string]interface{}{
		"ATTR1": "STRING!!",
		"ATTR2": 12345,
	}

	sem := make(chan struct{}, concurrency)

	for i := 0; i < count; i++ {
		sem <- struct{}{}
		cnt := i + 1
		go func() {
			defer func() { <-sem }()
			for {
				exec := d.DispatchByRR()
				_, err := exec.Do(ctx, func() (interface{}, error) {
					if err := fss.failureScenario(exec.Queue); err != nil {
						time.Sleep(100 * time.Millisecond)
						return nil, err
					}

					return exec.SendMessage(ctx, fmt.Sprintf("MESSAGE BODY FROM MULTI-QUEUE %d", cnt), option.MessageAttributes(attrs))
				})

				if err != nil {
					if ctxGotSignal(ctx) {
						log.Printf("got a signal")
						return
					}

					log.Printf("%s: unable to send the message. will retry: %s", *exec.Queue.URL, err)
					if err == circuitbreaker.ErrOpen {
						time.Sleep(time.Second)
					}
				} else {
					log.Printf("%s: the message has been sent (%d)", *exec.Queue.URL, cnt)
					break
				}
			}
		}()

		select {
		case <-ctx.Done():
			return
		default:
		}
	}

	log.Print("sent!")
}

func recv(ctx context.Context, exec *multiqueue.Executor) []string {
	log.Printf("%s: starting receiver...", *exec.URL)

	var messages []string

	for {
		select {
		case <-ctx.Done():
			log.Printf("shutting down receiver... count:%d", len(messages))
			goto END
		default:
		}

		resp, err := exec.ReceiveMessage(
			ctx,
			option.MaxNumberOfMessages(10),
		)
		if err != nil {
			log.Printf("unable to receive message: %s", err)
		}

		for _, m := range resp {
			if err := exec.DeleteMessage(ctx, m.ReceiptHandle); err != nil {
				log.Printf("unable to delete message: %s", err)
			}
			messages = append(messages, *m.Body)
		}
	}

END:

	return messages
}

type failureScenario struct {
	URL     string
	Until   time.Time
	ErrRate float64
}

type failureScenarioServer struct {
	mu       sync.Mutex
	scenario []failureScenario
	rng      *rand.Rand
}

func (s *failureScenarioServer) findScenario(q *multiqueue.Queue) (failureScenario, bool) {
	for _, sc := range s.scenario {
		if sc.URL == *q.URL {
			return sc, true
		}
	}
	return failureScenario{}, false
}

func (s *failureScenarioServer) failureScenario(q *multiqueue.Queue) error {
	sc, found := s.findScenario(q)
	if !found {
		return nil
	}

	if time.Now().Before(sc.Until) {
		if s.rng.Float64() > sc.ErrRate {
			return nil
		}
		return fmt.Errorf("this is a failure scenario until %s", sc.Until.Format(time.RFC3339))
	}

	return nil
}

func (s *failureScenarioServer) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if err := req.ParseForm(); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	index, err := strconv.ParseInt(req.Form.Get("index"), 10, 64)
	if err != nil {
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
	if index > int64(len(s.scenario))-1 {
		http.Error(rw, "index out of bound", http.StatusBadRequest)
		return
	}
	s.scenario[index].Until = time.Now().Add(dur)
	s.scenario[index].ErrRate = errRate

	_ = json.NewEncoder(rw).Encode(s.scenario)
}

func ctxGotSignal(ctx context.Context) bool {
	err := context.Cause(ctx)

	return err != nil && errors.Is(err, errGotSignal)
}
