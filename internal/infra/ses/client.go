package ses

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	sesv1 "github.com/aws/aws-sdk-go-v2/service/ses"
	sestypes "github.com/aws/aws-sdk-go-v2/service/ses/types"
	sesv2 "github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/aws/smithy-go"
	"go.uber.org/zap"
)

type VerificationStatus string

const (
	StatusVerified VerificationStatus = "Success"
	StatusPending  VerificationStatus = "Pending"
	StatusFailed   VerificationStatus = "Failed"
)

// API abstracts the subset of SES v1 operations the workers use.
type API interface {
	GetIdentityVerificationAttributes(ctx context.Context, in *sesv1.GetIdentityVerificationAttributesInput, opts ...func(*sesv1.Options)) (*sesv1.GetIdentityVerificationAttributesOutput, error)
	SendEmail(ctx context.Context, in *sesv1.SendEmailInput, opts ...func(*sesv1.Options)) (*sesv1.SendEmailOutput, error)
}

// APIV2 abstracts SES v2 operations used for domain identity checks.
type APIV2 interface {
	GetEmailIdentity(ctx context.Context, in *sesv2.GetEmailIdentityInput, opts ...func(*sesv2.Options)) (*sesv2.GetEmailIdentityOutput, error)
}

type Client struct {
	api    API
	apiv2  APIV2
	logger *zap.Logger
}

// NewClient builds SES v1 + v2 clients using the standard AWS credential chain.
func NewClient(ctx context.Context, region string, logger *zap.Logger) (*Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	return &Client{
		api:    sesv1.NewFromConfig(cfg),
		apiv2:  sesv2.NewFromConfig(cfg),
		logger: logger,
	}, nil
}

// NewClientWithAPI wires a custom API (used by tests).
func NewClientWithAPI(api API, logger *zap.Logger) *Client {
	return &Client{api: api, logger: logger}
}

// CheckDomainVerification uses SES v2 GetEmailIdentity to check DKIM status.
// Domains created via SES v2 must be verified this way.
func (c *Client) CheckDomainVerification(ctx context.Context, domain string) (VerificationStatus, error) {
	out, err := c.apiv2.GetEmailIdentity(ctx, &sesv2.GetEmailIdentityInput{
		EmailIdentity: aws.String(domain),
	})
	if err != nil {
		return StatusFailed, fmt.Errorf("ses get verification: %w", err)
	}
	if out.DkimAttributes == nil {
		return StatusPending, nil
	}
	switch string(out.DkimAttributes.Status) {
	case "SUCCESS":
		return StatusVerified, nil
	case "FAILED", "TEMPORARY_FAILURE":
		return StatusFailed, nil
	default:
		return StatusPending, nil
	}
}

// SendEmailInput is the worker-facing send request.
type SendEmailInput struct {
	From     string
	FromName string
	To       []string
	ReplyTo  []string
	Subject  string
	HTMLBody string
	TextBody string
	Tags     map[string]string
}

// SendEmailOutput captures SES response.
type SendEmailOutput struct {
	MessageID string
}

// SendEmail issues a SES SendEmail call. Errors are returned as-is so callers
// can use IsPermanentError to classify retry behavior.
func (c *Client) SendEmail(ctx context.Context, in SendEmailInput) (SendEmailOutput, error) {
	if len(in.To) == 0 {
		return SendEmailOutput{}, errors.New("send: empty recipient list")
	}
	if in.From == "" {
		return SendEmailOutput{}, errors.New("send: empty from address")
	}

	source := in.From
	if in.FromName != "" {
		source = fmt.Sprintf("%s <%s>", in.FromName, in.From)
	}

	msg := &sestypes.Message{
		Subject: &sestypes.Content{
			Data:    aws.String(in.Subject),
			Charset: aws.String("UTF-8"),
		},
		Body: &sestypes.Body{},
	}
	if in.HTMLBody != "" {
		msg.Body.Html = &sestypes.Content{
			Data:    aws.String(in.HTMLBody),
			Charset: aws.String("UTF-8"),
		}
	}
	if in.TextBody != "" {
		msg.Body.Text = &sestypes.Content{
			Data:    aws.String(in.TextBody),
			Charset: aws.String("UTF-8"),
		}
	}

	var tags []sestypes.MessageTag
	for k, v := range in.Tags {
		tags = append(tags, sestypes.MessageTag{
			Name:  aws.String(k),
			Value: aws.String(v),
		})
	}

	out, err := c.api.SendEmail(ctx, &sesv1.SendEmailInput{
		Source:           aws.String(source),
		Destination:      &sestypes.Destination{ToAddresses: in.To},
		Message:          msg,
		ReplyToAddresses: in.ReplyTo,
		Tags:             tags,
	})
	if err != nil {
		return SendEmailOutput{}, err
	}
	return SendEmailOutput{MessageID: aws.ToString(out.MessageId)}, nil
}

// permanentErrorCodes are SES error codes that should never be retried.
var permanentErrorCodes = map[string]struct{}{
	"MessageRejected":                       {},
	"MailFromDomainNotVerifiedException":    {},
	"ConfigurationSetDoesNotExistException": {},
	"ConfigurationSetSendingPausedException": {},
	"AccountSendingPausedException":         {},
	"InvalidParameterValue":                 {},
	"InvalidRenderingParameter":             {},
	"AccessDeniedException":                 {},
	"ValidationException":                   {},
}

// IsPermanentError reports whether the given SES error is a non-retryable
// permanent failure (sender not verified, message rejected, validation error, etc.).
// Unknown errors are treated as transient (returns false).
func IsPermanentError(err error) bool {
	if err == nil {
		return false
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		_, ok := permanentErrorCodes[apiErr.ErrorCode()]
		return ok
	}
	return false
}
