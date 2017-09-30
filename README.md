# aws-go-sqs

[![GoDoc](http://img.shields.io/badge/godoc-reference-blue.svg)](http://godoc.org/github.com/nabeken/aws-go-sqs/queue)
[![Build Status](https://img.shields.io/travis/nabeken/aws-go-sqs/master.svg)](https://travis-ci.org/nabeken/aws-go-sqs)
[![MIT License](http://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/nabeken/aws-go-sqs/blob/master/LICENSE)

aws-go-sqs is a SQS library built with [aws/aws-sdk-go](https://github.com/aws/aws-sdk-go).

# Versions

From v3 train (and `master` branch), we no longer use `gopkg.in` but will tag each release by following semver.

- We have [v1 branch](https://github.com/nabeken/aws-go-sqs/tree/v1) so you can import it from `gopkg.in/nabeken/aws-go-sqs.v1`.
- We have [v2 branch](https://github.com/nabeken/aws-go-sqs/tree/v2) so you can import it from `gopkg.in/nabeken/aws-go-sqs.v2`.

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
