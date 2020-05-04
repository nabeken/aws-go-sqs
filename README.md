# aws-go-sqs

[![GoDoc](http://img.shields.io/badge/godoc-reference-blue.svg)](http://godoc.org/github.com/nabeken/aws-go-sqs/queue)
[![Build Status](https://img.shields.io/travis/nabeken/aws-go-sqs/master.svg)](https://travis-ci.org/nabeken/aws-go-sqs)
[![MIT License](http://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/nabeken/aws-go-sqs/blob/master/LICENSE)

aws-go-sqs is a SQS library built with [aws/aws-sdk-go](https://github.com/aws/aws-sdk-go).

# Versions

From v3 train (and `master` branch), we no longer use `gopkg.in` but will tag each release by following semver.

Import path for v3:
```go
import "github.com/nabeken/aws-go-sqs/v3"
```

- We have [v1 branch](https://github.com/nabeken/aws-go-sqs/tree/v1) so you can import it from `gopkg.in/nabeken/aws-go-sqs.v1`.
- We have [v2 branch](https://github.com/nabeken/aws-go-sqs/tree/v2) so you can import it from `gopkg.in/nabeken/aws-go-sqs.v2`.

# Multi-queue implementation

v3 has [multiqueue](multiqueue/) package to address multi-queue (region) deployment of SQS. SQS is a crucial messaging component but it's still possible to become unavailable for several hours. We experienced that incident at [2020-04-20](https://status.aws.amazon.com/rss/sqs-ap-northeast-1.rss).

Since SQS is just a message bus between components, deploying SQS to the multiple regions for availability works even the system isn't fully deployed to the multiple regions.

## How `multiqueue` works

`multiqueue.Dispatcher` maintains:
- a slice of all of the `*queue.Queue`
- a slice of "available" `*queue.Queue`
- circuit breakers for each `*queue.Queue`

When the circuit state transitions to open, the dispatcher will remove the associated queue from the "available" slice so that it won't be used. The circuit breaker will update the state to half-opened then the dispatcher will push it back to "available" slice. If there is no further error, the state will be closed.

When there is no queue in the "available" slice, the dispatcher returns a queue from all of the `*queue.Queue` by random.

Example code:
```go
// Create SQS instance
s := sqs.New(session.Must(session.NewSession()))

// Create Queue instances
q1 := queue.MustNew(s, "example-queue1")
q2 := queue.MustNew(s, "example-queue2")

// https://godoc.org/github.com/mercari/go-circuitbreaker#Options
cbOpts := &circuitbreaker.Options{
    Interval:   1 * time.Minute,
    ShouldTrip: circuitbreaker.NewTripFuncFailureRate(100, 0.7),
}

d := multiqueue.New(cbOpts, q1, q2).
    WithOnStateChange(func(q *queue.Queue, from, to circuitbreaker.State) {
        log.Printf("%s: state has been changed from %s to %s", *q.URL, from, to)
    })

// exec will be q1 or q2
exec := d.Dispatch()

// calling the existing method in *queue.Queue via Do involves the circuit breaker
_, err := exec.Do(ctx, func() (interface{}, error) {
  return exec.SendMessage("MESSAGE BODY)
})
...
```

You can find [example/test-multiqueue](example/test-multiqueue) for the full example.

## Design note

When it comes to multi-region deployment, you may think about *primary* and *secondary* and use secondary when the primary becomes unavailable. Such failover codepath won't be called until the primary becomes unavailable so you have a rare chance to get it tested in production. You may agree that such code may contain a bug and the bug will be triggered when you're on fire.

Let's use both queues all the time. Don't let it be failover.

## Status

Since I've add API that I needed to this library, it is not completed but still useful.

## Example

See examples in [GoDoc](http://godoc.org/github.com/nabeken/aws-go-sqs/queue#pkg-examples).

## Testing

We have some integration tests in `queue/queue_test.go`.

If you want to run the tests, please create a dedicated queue for the tests. The test suite will issue PurgeQueue API, which purge all messages, in the teardown.

You can specify the name in environment variable.

```sh
$ cd queue
$ export TEST_SQS_QUEUE_NAME=aws-go-sqs-test
$ go test -v
```
