module github.com/nabeken/aws-go-sqs/v4

go 1.24

toolchain go1.26.6

require (
	github.com/aws/aws-sdk-go-v2 v1.47.5
	github.com/aws/aws-sdk-go-v2/config v1.33.12
	github.com/aws/aws-sdk-go-v2/service/sqs v1.52.7
	github.com/benbjohnson/clock v1.3.5 // indirect
	github.com/cenkalti/backoff/v3 v3.2.2 // indirect
	github.com/hashicorp/go-multierror v1.1.1
	github.com/mercari/go-circuitbreaker v0.0.2
	github.com/stretchr/testify v1.12.1
)

require github.com/aws/smithy-go v1.28.7

require (
	github.com/aws/aws-sdk-go-v2/credentials v1.20.12 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.20.5 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.8 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.8 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.5.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.23 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.14.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.12.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.39.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.44.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.51.6 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
)
