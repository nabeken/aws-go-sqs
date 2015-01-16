package option

import (
	"strconv"

	"github.com/stripe/aws-go/aws"
	"github.com/stripe/aws-go/gen/sqs"
)

// The DataType is a type of data used in Attributes and Message Attributes.
const (
	DataTypeString = "String"
	DataTypeNumber = "Number"
	DataTypeBinary = "Binary"
)

// The ReceiveMessageRequest type is an adapter to change a parameter in
// sqs.ReceiveMessageRequest.
type ReceiveMessageRequest func(req *sqs.ReceiveMessageRequest)

// VisibilityTimeout returns a ReceiveMessageRequest that changes a message visibility timeout.
func VisibilityTimeout(timeout int) ReceiveMessageRequest {
	return func(req *sqs.ReceiveMessageRequest) {
		req.VisibilityTimeout = aws.Integer(timeout)
	}
}

// MaxNumberOfMessages returns a ReceiveMessageRequest that
// changes a max number of messages to receive to n.
func MaxNumberOfMessages(n int) ReceiveMessageRequest {
	return func(req *sqs.ReceiveMessageRequest) {
		req.MaxNumberOfMessages = aws.Integer(n)
	}
}

// UseAllAttribute returns a ReceiveMessageRequest that
// changes a parameter to receive all messages regardless of attributes.
func UseAllAttribute() ReceiveMessageRequest {
	return UseAttributes("All")
}

// UseAttributes returns a ReceiveMessageRequest that
// changes AttributeNames and MessageAttributeNames to attr.
func UseAttributes(attr ...string) ReceiveMessageRequest {
	return func(req *sqs.ReceiveMessageRequest) {
		req.AttributeNames = attr
		req.MessageAttributeNames = attr
	}
}

// The SendMessageRequest type is an adapter to change a parameter in
// sqs.SendMessageRequest.
type SendMessageRequest func(req *sqs.SendMessageRequest)

// DelaySeconds returns a SendMessageRequest that changes DelaySeconds to delay in seconds.
func DelaySeconds(delay int) SendMessageRequest {
	return func(req *sqs.SendMessageRequest) {
		req.DelaySeconds = aws.Integer(delay)
	}
}

// MessageAttributes returns a SendMessageRequest that changes MessageAttributes to attrs.
// A string value in attrs sets to DataTypeString.
// A []byte value in attrs sets to DataTypeBinary.
// A int and int64 value in attrs sets to DataTypeNumber. Other types cause panicking.
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

// MessageAttributeValue returns a appropriate sqs.MessageAttributeValue by type assersion of v.
// Types except string, []byte, int64 and int cause panicking.
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
