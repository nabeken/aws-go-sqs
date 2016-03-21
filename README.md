# aws-go-sqs

[![GoDoc](http://img.shields.io/badge/godoc-reference-blue.svg)](http://godoc.org/github.com/nabeken/aws-go-sqs/queue)
[![Build Status](https://img.shields.io/travis/nabeken/aws-go-sqs/v1.svg)](https://travis-ci.org/nabeken/aws-go-sqs)
[![MIT License](http://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/nabeken/aws-go-sqs/blob/master/LICENSE)

aws-go-sqs is a SQS library built with [aws/aws-sdk-go](https://github.com/aws/aws-sdk-go).

## Status

Since I've add API that I needed to this library, it is not completed but still useful.

## Example

See more examples in [GoDoc](http://godoc.org/github.com/nabeken/aws-go-sqs/queue#pkg-examples).

```go
import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/nabeken/aws-go-sqs/queue"
	"github.com/nabeken/aws-go-sqs/queue/option"
)

// Create SQS instance
s := sqs.New(&aws.Config{Region: aws.String("ap-northeast-1")})

// Create Queue instance
q, err := queue.New(s, "example-queue-name")
if err != nil {
    log.Fatal(err)
}

// MessageAttributes
attrs := map[string]interface{}{
    "ATTR1": "STRING!!",
    "ATTR2": 12345,
}

if err := q.SendMessage("MESSAGE BODY", option.MessageAttributes(attrs)); err != nil {
    log.Fatal(err)
}

log.Print("successed!")
```

## Testing

I've add some integration tests in `queue/queue_test.go`.

If you want to run the tests, you *MUST* create a decicated queue for the tests.
The test suite issues PurgeQueue in teardown.

You can specify the name in environment variable.

```sh
$ cd queue
$ export TEST_SQS_QUEUE_NAME=aws-go-sqs-test
$ go test -v -tags debug
```

If you specify a debug tag, you get all HTTP request and response in stdout.
