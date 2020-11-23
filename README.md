# aws-go-sqs

[![PkgGoDev](https://pkg.go.dev/badge/github.com/nabeken/aws-go-sqs/v3)](https://pkg.go.dev/github.com/nabeken/aws-go-sqs/v3)
[![MIT License](http://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

`aws-go-sqs` is a SQS wrapper library for [aws/aws-sdk-go](https://github.com/aws/aws-sdk-go).

# Usage

`v3` and later requires Go Modules support to import.
```go
import "github.com/nabeken/aws-go-sqs/v3/queue"
```

From v3 train (and `master` branch), we no longer use `gopkg.in`.

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

When the circuit state transitions to open, the dispatcher will remove the associated queue from the "available" slice so that it won't be used. The circuit breaker will update the state to half-opened then the dispatcher will push it back to "available" slice. If there is no further error, the circuit will be closed.

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

## Consideration for ReceiveMessage via `multiqueue`

There is a case which causes the performance issue when there are no messages in one of the registered queues.

Let's say you have two queues: Queue A has 1000 messages and Queue B has no messages.

`Dispatcher.Dispatch()` will return one of the registered queues. The problem is when the B is returned. SQS will block until new message arrives for 20 seconds in default (WaitReceiveTime). You spend 20 seconds even you have 1000 messages in the A. If, unfortunately, the dispatcher returns B again, you will spend additional 20 seconds.

To avoid this situation, you should poll all the registered queueus in parallel. `Dispatcher.GetExecutors()` will return all of the registered queue wrapped with `Executor` so that you still can call the API over the circuit breaker.

## Design note

When it comes to multi-region deployment, you may think about *primary* and *secondary* and use secondary when the primary becomes unavailable. Such failover codepath won't be called until the primary becomes unavailable so you have a rare chance to get it tested in production. You may agree that such code may contain a bug or the secondary queue configuration may not be up-to-date since mostly the secondary queue is not used at all. Any hidden problems would be triggered when you're on fire.

Let's use all of the available queues all the time and stop sending requests to an unavailable queue until it recovers. You can have the confidence that your system always works with all of the available queues.

AWS recommends this patterns in Well-Architected Framework.
> https://wa.aws.amazon.com/wat.question.REL_11.en.html says:
>
> Use static stability to prevent bimodal behavior: Bimodal behavior is when your workload exhibits different behavior under normal and failure modes, for example, relying on launching new instances if an Availability Zone fails. You should instead build workloads that are statically stable and operate in only one mode. In this case, provision enough instances in each Availability Zone to handle the workload load if one AZ were removed and then use Elastic Load Balancing or Amazon Route 53 health checks to shift load away from the impaired instances.
>
> https://d1.awsstatic.com/whitepapers/architecture/AWS-Reliability-Pillar.pdf says:
>
> **Test disaster recovery implementation to validate the implementation**: Regularly test failover to DR to
ensure that RTO and RPO are met.
>
> A pattern to avoid is developing recovery paths that are rarely executed. For example, you might have a secondary data store that is used for read-only queries.
> When you write to a data store and the primary fails, you might want to fail over to the secondary data store.
> If you don’t frequently test this failover, you might find that your assumptions about the capabilities of the secondary data store are incorrect.
> The capacity of the secondary, which might have been sufficient when you last tested, may be no longer be able to tolerate the load under this scenario.
> Our experience has shown that the only error recovery that works is the path you test frequently. This is why having a small number of recovery paths is best.
> You can establish recovery patterns and regularly test them. If you have a complex or critical recovery path, you still need to regularly execute that failure in production to convince yourself that the recovery path works.
> In the example we just discussed, you should fail over to the standby regularly, regardless of need.

# Testing

We have some integration tests in `queue/queue_test.go`.

If you want to run the tests, please create a dedicated queue for the tests. The test suite will issue PurgeQueue API, which purge all messages, in the teardown.

You can specify the name in environment variable.

```sh
$ cd queue
$ export TEST_SQS_QUEUE_NAME=aws-go-sqs-test
$ go test -v
```
