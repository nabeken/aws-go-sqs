package option

import (
	"strconv"

	"github.com/stripe/aws-go/aws"
	"github.com/stripe/aws-go/gen/sqs"
)

const (
	DataTypeString = "String"
	DataTypeNumber = "Number"
	DataTypeBinary = "Binary"
)

type ReceiveMessageRequest func(req *sqs.ReceiveMessageRequest)

func VisibilityTimeout(timeout int) ReceiveMessageRequest {
	return func(req *sqs.ReceiveMessageRequest) {
		req.VisibilityTimeout = aws.Integer(timeout)
	}
}

func MaxNumberOfMessages(n int) ReceiveMessageRequest {
	return func(req *sqs.ReceiveMessageRequest) {
		req.MaxNumberOfMessages = aws.Integer(n)
	}
}

func UseAllAttribute() ReceiveMessageRequest {
	return UseAttributes("All")
}

func UseAttributes(attr ...string) ReceiveMessageRequest {
	return func(req *sqs.ReceiveMessageRequest) {
		req.AttributeNames = attr
		req.MessageAttributeNames = attr
	}
}

type SendMessageRequest func(req *sqs.SendMessageRequest)

func DelaySeconds(delay int) SendMessageRequest {
	return func(req *sqs.SendMessageRequest) {
		req.DelaySeconds = aws.Integer(delay)
	}
}

func MessageAttributes(attrs map[string]interface{}) SendMessageRequest {
	return func(req *sqs.SendMessageRequest) {
		if len(attrs) == 0 {
			return
		}

		ret := make(map[string]sqs.MessageAttributeValue)
		for n, v := range attrs {
			ret[n] = MessageAttributeValue(v)
		}
		req.MessageAttributes = ret
	}
}

func MessageAttributeValue(v interface{}) sqs.MessageAttributeValue {
	switch vv := v.(type) {
	case string:
		return sqs.MessageAttributeValue{
			DataType:    aws.String(DataTypeString),
			StringValue: aws.String(vv),
		}
	case []byte:
		return sqs.MessageAttributeValue{
			DataType:    aws.String(DataTypeBinary),
			BinaryValue: vv,
		}
	case int64:
		return sqs.MessageAttributeValue{
			DataType:    aws.String(DataTypeNumber),
			StringValue: aws.String(strconv.FormatInt(vv, 10)),
		}
	case int:
		return sqs.MessageAttributeValue{
			DataType:    aws.String(DataTypeNumber),
			StringValue: aws.String(strconv.FormatInt(int64(vv), 10)),
		}
	default:
		panic("sqs: unsupported type")
	}
}
