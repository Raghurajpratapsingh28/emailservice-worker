package types

import "time"

// DomainVerifyPayload matches the domain.verify.poll subject contract.
type DomainVerifyPayload struct {
	DomainID    string `json:"domainId"`
	WorkspaceID string `json:"workspaceId"`
	Domain      string `json:"domain"`
}

// EmailAddress represents a single email recipient/sender.
type EmailAddress struct {
	Email string `json:"email"`
	Name  string `json:"name,omitempty"`
}

// TransactionalEmailPayload matches the email.send.transactional subject contract.
// This contract is fixed by the Fastify API and MUST NOT change.
type TransactionalEmailPayload struct {
	JobID       string            `json:"jobId"`
	WorkspaceID string            `json:"workspaceId"`
	SendID      string            `json:"sendId"`
	To          []EmailAddress    `json:"to"`
	From        EmailAddress      `json:"from"`
	ReplyTo     string            `json:"replyTo,omitempty"`
	Subject     string            `json:"subject"`
	HTML        string            `json:"html"`
	Text        string            `json:"text"`
	Tags        map[string]string `json:"tags,omitempty"`
	Provider    string            `json:"provider"`
}

// CampaignStartPayload matches the campaign.send.start subject contract.
// Fixed by the Fastify API. MUST NOT change.
type CampaignStartPayload struct {
	JobID       string       `json:"jobId"`
	WorkspaceID string       `json:"workspaceId"`
	CampaignID  string       `json:"campaignId"`
	SegmentID   string       `json:"segmentId"`
	Sender      EmailAddress `json:"sender"`
	ReplyTo     string       `json:"replyTo,omitempty"`
	Subject     string       `json:"subject"`
	HTML        string       `json:"html"`
	Text        string       `json:"text"`
}

// CampaignChunkRecipient is one entry of a chunk's recipient list.
type CampaignChunkRecipient struct {
	RecipientID string `json:"recipientId"` // campaign_recipients.id
	Email       string `json:"email"`
	Name        string `json:"name,omitempty"`
}

// CampaignChunkPayload is published by the campaign-start worker for each
// recipient batch and consumed by the campaign-chunk worker.
type CampaignChunkPayload struct {
	CampaignID  string                   `json:"campaignId"`
	WorkspaceID string                   `json:"workspaceId"`
	ChunkID     string                   `json:"chunkId"`
	Sender      EmailAddress             `json:"sender"`
	ReplyTo     string                   `json:"replyTo,omitempty"`
	Subject     string                   `json:"subject"`
	HTML        string                   `json:"html"`
	Text        string                   `json:"text"`
	Recipients  []CampaignChunkRecipient `json:"recipients"`
}

// EmailDeliveryEvent matches the email.delivery.events subject contract.
// Used by both transactional (with SendID) and campaign (with CampaignID +
// RecipientEmail) sends. Fields are mutually exclusive depending on origin.
type EmailDeliveryEvent struct {
	WorkspaceID       string    `json:"workspaceId"`
	SendID            string    `json:"sendId,omitempty"`
	CampaignID        string    `json:"campaignId,omitempty"`
	RecipientEmail    string    `json:"recipientEmail,omitempty"`
	ProviderMessageID string    `json:"providerMessageId,omitempty"`
	Status            string    `json:"status"` // sent | failed | bounced
	Reason            string    `json:"reason,omitempty"`
	Timestamp         time.Time `json:"timestamp"`
}

// CampaignDLQMessage is the payload published to campaign.send.dlq when a
// chunk exhausts retries.
type CampaignDLQMessage struct {
	Payload   *CampaignChunkPayload `json:"payload"`
	Reason    string                `json:"reason"`
	Attempts  int                   `json:"attempts"`
	Timestamp time.Time             `json:"timestamp"`
}

// RawEventPayload matches the events.raw.{workspaceId} subject contract.
// Fixed by the Fastify API. MUST NOT change.
type RawEventPayload struct {
	EventID     string                 `json:"eventId"`
	WorkspaceID string                 `json:"workspaceId"`
	APIKeyID    string                 `json:"apiKeyId"`
	EventType   string                 `json:"eventType"` // track | identify | page | screen | alias | group
	EventName   string                 `json:"eventName"`
	UserID      string                 `json:"userId,omitempty"`
	AnonymousID string                 `json:"anonymousId,omitempty"`
	GroupID     string                 `json:"groupId,omitempty"`
	Traits      map[string]interface{} `json:"traits"`
	Properties  map[string]interface{} `json:"properties"`
	Context     map[string]interface{} `json:"context"`
	ReceivedAt  time.Time              `json:"receivedAt"`
}

// WorkflowTriggerPayload matches the workflow.trigger subject contract.
// This contract is locked.
type WorkflowTriggerPayload struct {
	WorkspaceID string `json:"workspaceId"`
	WorkflowID  string `json:"workflowId,omitempty"`
	ContactID   string `json:"contactId"`
	EventName   string `json:"eventName"`
	EventID     string `json:"eventId"`
}

// WorkflowRegisterPayload matches the workflow.register subject contract.
type WorkflowRegisterPayload struct {
	WorkspaceID string `json:"workspaceId"`
	WorkflowID  string `json:"workflowId"`
}

// SegmentRefreshPayload matches the segment.refresh subject contract.
// Fixed by the Fastify API. MUST NOT change.
type SegmentRefreshPayload struct {
	WorkspaceID string `json:"workspaceId"`
	SegmentID   string `json:"segmentId"`
}
