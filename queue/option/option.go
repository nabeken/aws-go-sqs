// Package option provides adapters to change a parameter in SQS request.
package option

import (
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// The DataType is a type of data used in Attributes and Message Attributes.
const (
	DataTypeString = "String"
	DataTypeNumber = "Number"
	DataTypeBinary = "Binary"
)

// The ReceiveMessageInput type is an adapter to change a parameter in
// sqs.ReceiveMessageInput.
type ReceiveMessageInput func(req *sqs.ReceiveMessageInput)

// VisibilityTimeout returns a ReceiveMessageInput that changes a message visibility timeout.
func VisibilityTimeout(timeout int32) ReceiveMessageInput {
	return func(req *sqs.ReceiveMessageInput) {
		req.VisibilityTimeout = timeout
	}
}

// MaxNumberOfMessages returns a ReceiveMessageInput that
// changes a max number of messages to receive to n.
func MaxNumberOfMessages(n int32) ReceiveMessageInput {
	return func(req *sqs.ReceiveMessageInput) {
		req.MaxNumberOfMessages = n
	}
}

// WaitTimeSeconds returns a ReceiveMessageInput that
// changes WaitTimeSeconds parameter.
func WaitTimeSeconds(n int32) ReceiveMessageInput {
	return func(req *sqs.ReceiveMessageInput) {
		req.WaitTimeSeconds = n
	}
}

// UseAllAttribute returns a ReceiveMessageInput that
// changes a parameter to receive all messages regardless of attributes.
func UseAllAttribute() ReceiveMessageInput {
	return UseAttributes("All")
}

// UseAttributes returns a ReceiveMessageInput that
// changes AttributeNames and MessageAttributeNames to attr.
func UseAttributes(attr ...string) ReceiveMessageInput {
	return func(req *sqs.ReceiveMessageInput) {
		for i := range attr {
			req.AttributeNames = append(req.AttributeNames, types.QueueAttributeName(attr[i]))
		}
		req.MessageAttributeNames = attr
	}
}

// The SendMessageInput type is an adapter to change a parameter in
// sqs.SendMessageInput.
type SendMessageInput func(req *sqs.SendMessageInput)

// DelaySeconds returns a SendMessageInput that changes DelaySeconds to delay in seconds.
func DelaySeconds(delay int32) SendMessageInput {
	return func(req *sqs.SendMessageInput) {
		req.DelaySeconds = delay
	}
}

// MessageAttributes returns a SendMessageInput that changes MessageAttributes to attrs.
// A string value in attrs sets to DataTypeString.
// A []byte value in attrs sets to DataTypeBinary.
// A int and int64 value in attrs sets to DataTypeNumber. Other types cause panicking.
func MessageAttributes(attrs map[string]interface{}) SendMessageInput {
	return func(req *sqs.SendMessageInput) {
		if len(attrs) == 0 {
			return
		}

		for n, v := range attrs {
			req.MessageAttributes[n] = MessageAttributeValue(v)
		}
	}
}

// MessageAttributeValue returns a appropriate sqs.MessageAttributeValue by type assersion of v.
// Types except string, []byte, int64 and int cause panicking.
func MessageAttributeValue(v interface{}) types.MessageAttributeValue {
	switch vv := v.(type) {
	case string:
		return types.MessageAttributeValue{
			DataType:    aws.String(DataTypeString),
			StringValue: aws.String(vv),
		}
	case []byte:
		return types.MessageAttributeValue{
			DataType:    aws.String(DataTypeBinary),
			BinaryValue: vv,
		}
	case int64:
		return types.MessageAttributeValue{
			DataType:    aws.String(DataTypeNumber),
			StringValue: aws.String(strconv.FormatInt(vv, 10)),
		}
	case int:
		return types.MessageAttributeValue{
			DataType:    aws.String(DataTypeNumber),
			StringValue: aws.String(strconv.FormatInt(int64(vv), 10)),
		}
	default:
		panic("sqs: unsupported type")
	}
}
