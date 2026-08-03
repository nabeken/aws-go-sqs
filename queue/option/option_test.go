package option

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessageGroupId(t *testing.T) {
	req := &sqs.SendMessageInput{}

	MessageGroupId("tenant-123")(req)

	require.NotNil(t, req.MessageGroupId)
	assert.Equal(t, "tenant-123", *req.MessageGroupId)
}
