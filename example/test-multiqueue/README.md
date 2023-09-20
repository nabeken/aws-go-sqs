# test-multiqueue - A command-line tool to emulate the circuit breaker state transition

We need a tool to be able to observe a real-time behavior of the `multiqueue` implementation and its performance. `test-multiqueue` command allows you to:
- run `SendMessage` and `ReceiveMessage` API under the circuit breaker.
- inject errors under the certain error rate and duration via HTTP
- show the total number of {unique,} messages (remember: SQS is at-least-once delivery)

```
go build && ./test-multiqueue -h
Usage of ./test-multiqueue:
  -concurrency int
    	specify concurrency (default 1)
  -count int
    	number of messages (default 10000)
  -drain
    	drain
  -queue1 string
    	specify SQS queue name 1
  -queue2 string
    	specify SQS queue name 2
  -region1 string
    	specify a region for queue1 (default "ap-northeast-1")
  -region2 string
    	specify a region for queue2 (default "ap-southeast-1")
```

## Example

Send 1,000,000 messages to queues in ap-northeast-1 and ap-southeast-1 and receive the messages from the both queues:
```sh
./test-multiqueue \
  -queue1 example-ap-northeast-1 \
  -queue2 example-ap-southeast-1 \
  -concurrency 50 \
  -count 1000000
```

While the tool is working, you can inject errors via HTTP:
```
# set 80% error rate for 5 minutes for queue1
curl "http://127.0.0.1:9003/?index=0&duration=5m&error_rate=0.8" | jq -r .
[
  {
    "URL": "https://sqs.ap-northeast-1.amazonaws.com/EXAMPLE/example-ap-northeast-1",
    "Until": "2020-05-07T23:52:13.096187+09:00",
    "ErrRate": 0.8
  },
  {
    "URL": "https://sqs.ap-southeast-1.amazonaws.com/EXAMPLE/example-ap-southeast-1",
    "Until": "0001-01-01T00:00:00Z",
    "ErrRate": 0
  }
]
```

# Test scenario

You can use `t.sh` script in the directory.

Happy testing.
