package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type Client struct {
	Pool   *pgxpool.Pool
	logger *zap.Logger
}

func NewClient(ctx context.Context, databaseURL string, logger *zap.Logger) (*Client, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse db config: %w", err)
	}
	cfg.MaxConns = 10
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("db connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db ping: %w", err)
	}

	logger.Info("postgres connected")
	return &Client{Pool: pool, logger: logger}, nil
}

func (c *Client) Close() {
	c.Pool.Close()
}

// ============================================================
// domains table (domain verification worker)
// ============================================================

// GetDomainStatus returns the current status of a domain row.
// Returns ("", nil) when the row does not exist.
func (c *Client) GetDomainStatus(ctx context.Context, domainID, workspaceID string) (string, error) {
	const q = `SELECT status FROM domains WHERE id = $1 AND workspace_id = $2`
	var status string
	err := c.Pool.QueryRow(ctx, q, domainID, workspaceID).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	return status, nil
}

func (c *Client) UpdateDomainVerified(ctx context.Context, domainID, workspaceID string) error {
	const q = `UPDATE domains
		SET status = 'verified',
		    verified_at = COALESCE(verified_at, NOW()),
		    updated_at = NOW()
		WHERE id = $1 AND workspace_id = $2
		  AND deleted_at IS NULL
		  AND status NOT IN ('deleting', 'deleted')`
	_, err := c.Pool.Exec(ctx, q, domainID, workspaceID)
	return err
}

func (c *Client) UpdateDomainPending(ctx context.Context, domainID, workspaceID string, attempts int) error {
	const q = `UPDATE domains
		SET status = 'pending',
		    verification_attempts = $3,
		    last_verification_check_at = NOW(),
		    updated_at = NOW()
		WHERE id = $1 AND workspace_id = $2
		  AND deleted_at IS NULL
		  AND status NOT IN ('deleting', 'deleted')`
	_, err := c.Pool.Exec(ctx, q, domainID, workspaceID, attempts)
	return err
}

func (c *Client) UpdateDomainFailed(ctx context.Context, domainID, workspaceID string, attempts int) error {
	const q = `UPDATE domains
		SET status = 'failed',
		    verification_attempts = $3,
		    last_verification_check_at = NOW(),
		    updated_at = NOW()
		WHERE id = $1 AND workspace_id = $2
		  AND deleted_at IS NULL
		  AND status NOT IN ('deleting', 'deleted')`
	_, err := c.Pool.Exec(ctx, q, domainID, workspaceID, attempts)
	return err
}

// GetStaleVerifyingDomains returns domains that have been in pending/verifying
// status for longer than staleDuration and are not already deleted. Used by
// DomainCleanupScheduler to expire abandoned verifications.
func (c *Client) GetStaleVerifyingDomains(ctx context.Context, staleDuration time.Duration, limit int) ([]StaleVerifyingDomain, error) {
	const q = `
		SELECT d.id, d.workspace_id, d.domain,
		       u.email AS owner_email,
		       COALESCE(u.first_name, '') AS owner_name
		FROM domains d
		JOIN workspaces w ON w.id = d.workspace_id
		JOIN users u ON u.id = w.owner_user_id
		WHERE d.status IN ('pending', 'verifying')
		  AND d.deleted_at IS NULL
		  AND d.verification_started_at <= NOW() - ($1 * INTERVAL '1 second')
		ORDER BY d.verification_started_at
		LIMIT $2`
	rows, err := c.Pool.Query(ctx, q, staleDuration.Seconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("get stale verifying domains: %w", err)
	}
	defer rows.Close()

	var out []StaleVerifyingDomain
	for rows.Next() {
		var d StaleVerifyingDomain
		if err := rows.Scan(&d.ID, &d.WorkspaceID, &d.Domain, &d.OwnerEmail, &d.OwnerName); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// StaleVerifyingDomain is returned by GetStaleVerifyingDomains.
type StaleVerifyingDomain struct {
	ID          string
	WorkspaceID string
	Domain      string
	OwnerEmail  string
	OwnerName   string
}

// MarkDomainExpired transitions a domain from pending/verifying to failed
// with a note that it expired. Skips rows already in a terminal state.
func (c *Client) MarkDomainExpired(ctx context.Context, domainID, workspaceID string) error {
	const q = `UPDATE domains
		SET status = 'failed',
		    last_verification_check_at = NOW(),
		    updated_at = NOW()
		WHERE id = $1 AND workspace_id = $2
		  AND status IN ('pending', 'verifying')
		  AND deleted_at IS NULL`
	_, err := c.Pool.Exec(ctx, q, domainID, workspaceID)
	return err
}

// ============================================================
// email_sends table (transactional email worker)
// ============================================================

func (c *Client) GetEmailSendStatus(ctx context.Context, sendID, workspaceID string) (string, error) {
	const q = `SELECT status FROM email_sends WHERE send_id = $1 AND workspace_id = $2`
	var status string
	err := c.Pool.QueryRow(ctx, q, sendID, workspaceID).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	return status, nil
}

func (c *Client) UpdateEmailSendSending(ctx context.Context, sendID, workspaceID string) error {
	const q = `UPDATE email_sends
		SET status = 'sending', updated_at = NOW()
		WHERE send_id = $1 AND workspace_id = $2 AND status IN ('queued', 'failed', 'sending')`
	_, err := c.Pool.Exec(ctx, q, sendID, workspaceID)
	return err
}

func (c *Client) UpdateEmailSendSent(ctx context.Context, sendID, workspaceID, providerMessageID string) error {
	const q = `UPDATE email_sends
		SET status = 'sent',
		    provider_message_id = $3,
		    failure_reason = NULL,
		    updated_at = NOW()
		WHERE send_id = $1 AND workspace_id = $2 AND status NOT IN ('bounced')`
	_, err := c.Pool.Exec(ctx, q, sendID, workspaceID, providerMessageID)
	return err
}

func (c *Client) UpdateEmailSendFailed(ctx context.Context, sendID, workspaceID, reason string) error {
	const q = `UPDATE email_sends
		SET status = 'failed',
		    failure_reason = $3,
		    updated_at = NOW()
		WHERE send_id = $1 AND workspace_id = $2 AND status NOT IN ('sent', 'bounced')`
	_, err := c.Pool.Exec(ctx, q, sendID, workspaceID, reason)
	return err
}

// ============================================================
// segments + contacts (campaign batcher)
// ============================================================

// Contact is a row from the segment-resolved contact stream.
type Contact struct {
	ID    string
	Email string
	Name  string
}

// FetchSegmentContactsBatch returns contacts that belong to a segment, filtered
// by the active / non-suppressed / valid-email rules. The query is paginated by
// contact ID for keyset pagination — pass an empty afterID for the first call,
// then pass the last returned ID to fetch the next page.
//
// Returns an empty slice when no more contacts match.
func (c *Client) FetchSegmentContactsBatch(
	ctx context.Context,
	workspaceID, segmentID, afterID string,
	limit int,
) ([]Contact, error) {
	const q = `
		SELECT c.id, c.email, COALESCE(c.first_name, '')
		FROM contacts c
		INNER JOIN segment_memberships sm
		        ON sm.contact_id = c.id
		       AND sm.workspace_id = c.workspace_id
		WHERE c.workspace_id = $1
		  AND sm.segment_id = $2
		  AND c.status = 'active'
		  AND COALESCE(c.is_suppressed, false) = false
		  AND COALESCE(c.is_email_valid, true) = true
		  AND ($3 = '' OR c.id > $3::uuid)
		ORDER BY c.id
		LIMIT $4`
	rows, err := c.Pool.Query(ctx, q, workspaceID, segmentID, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("fetch segment contacts: %w", err)
	}
	defer rows.Close()

	var out []Contact
	for rows.Next() {
		var c Contact
		if err := rows.Scan(&c.ID, &c.Email, &c.Name); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// ============================================================
// campaigns table
// ============================================================

func (c *Client) GetCampaignStatus(ctx context.Context, campaignID, workspaceID string) (string, error) {
	const q = `SELECT status FROM campaigns WHERE id = $1 AND workspace_id = $2`
	var status string
	err := c.Pool.QueryRow(ctx, q, campaignID, workspaceID).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	return status, nil
}

// UpdateCampaignStarted moves a campaign from queued -> sending and records
// the total recipient count and start timestamp. The conditional WHERE makes
// this idempotent across duplicate start jobs.
func (c *Client) UpdateCampaignStarted(
	ctx context.Context,
	campaignID, workspaceID string,
	totalRecipients int,
) error {
	const q = `UPDATE campaigns
		SET status = 'sending',
		    total_recipients = $3,
		    started_at = COALESCE(started_at, NOW()),
		    updated_at = NOW()
		WHERE id = $1 AND workspace_id = $2 AND status IN ('queued', 'sending')`
	_, err := c.Pool.Exec(ctx, q, campaignID, workspaceID, totalRecipients)
	return err
}

// CampaignProgress is the response from IncrementCampaignCounts.
type CampaignProgress struct {
	SentCount       int
	FailedCount     int
	TotalRecipients int
}

// IncrementCampaignCounts atomically adds to sent_count and failed_count.
// Returns the post-update counters so the caller can detect completion.
func (c *Client) IncrementCampaignCounts(
	ctx context.Context,
	campaignID, workspaceID string,
	sentDelta, failedDelta int,
) (CampaignProgress, error) {
	const q = `UPDATE campaigns
		SET sent_count = sent_count + $3,
		    failed_count = failed_count + $4,
		    updated_at = NOW()
		WHERE id = $1 AND workspace_id = $2
		RETURNING sent_count, failed_count, total_recipients`
	var p CampaignProgress
	err := c.Pool.QueryRow(ctx, q, campaignID, workspaceID, sentDelta, failedDelta).
		Scan(&p.SentCount, &p.FailedCount, &p.TotalRecipients)
	return p, err
}

// MarkCampaignComplete sets status='completed' and completed_at if and only if
// all recipients have been processed AND the campaign hasn't already been
// marked complete. Returns true when this call performed the transition.
func (c *Client) MarkCampaignComplete(ctx context.Context, campaignID, workspaceID string) (bool, error) {
	const q = `UPDATE campaigns
		SET status = 'completed',
		    completed_at = NOW(),
		    updated_at = NOW()
		WHERE id = $1
		  AND workspace_id = $2
		  AND completed_at IS NULL
		  AND total_recipients > 0
		  AND sent_count + failed_count >= total_recipients`
	tag, err := c.Pool.Exec(ctx, q, campaignID, workspaceID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// MarkCampaignCompleteEmpty handles the zero-recipient case explicitly.
func (c *Client) MarkCampaignCompleteEmpty(ctx context.Context, campaignID, workspaceID string) error {
	const q = `UPDATE campaigns
		SET status = 'completed',
		    completed_at = NOW(),
		    total_recipients = 0,
		    started_at = COALESCE(started_at, NOW()),
		    updated_at = NOW()
		WHERE id = $1 AND workspace_id = $2 AND status NOT IN ('completed', 'failed')`
	_, err := c.Pool.Exec(ctx, q, campaignID, workspaceID)
	return err
}

func (c *Client) UpdateCampaignFailed(ctx context.Context, campaignID, workspaceID, reason string) error {
	const q = `UPDATE campaigns
		SET status = 'failed',
		    failure_reason = $3,
		    completed_at = NOW(),
		    updated_at = NOW()
		WHERE id = $1 AND workspace_id = $2 AND status NOT IN ('completed', 'failed')`
	_, err := c.Pool.Exec(ctx, q, campaignID, workspaceID, reason)
	return err
}

// ============================================================
// campaign_recipients table
// ============================================================

// CampaignRecipientRow is the persisted recipient as returned by the bulk insert.
type CampaignRecipientRow struct {
	ID    string
	Email string
	Name  string
}

// BulkInsertCampaignRecipients inserts (or no-ops on conflict) recipient rows
// for the given contacts and returns the persisted rows. Uses a unique
// constraint on (campaign_id, contact_id) for idempotent re-runs.
func (c *Client) BulkInsertCampaignRecipients(
	ctx context.Context,
	campaignID, workspaceID string,
	contacts []Contact,
) ([]CampaignRecipientRow, error) {
	if len(contacts) == 0 {
		return nil, nil
	}

	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Build a multi-row insert. ON CONFLICT preserves existing rows (idempotent).
	const q = `
		INSERT INTO campaign_recipients
		    (id, campaign_id, workspace_id, contact_id, email, name, status, created_at, updated_at)
		VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, 'queued', NOW(), NOW())
		ON CONFLICT (campaign_id, contact_id) DO UPDATE SET updated_at = NOW()
		RETURNING id, email, COALESCE(name, '')`

	out := make([]CampaignRecipientRow, 0, len(contacts))
	for _, ct := range contacts {
		var row CampaignRecipientRow
		if err := tx.QueryRow(ctx, q, campaignID, workspaceID, ct.ID, ct.Email, ct.Name).
			Scan(&row.ID, &row.Email, &row.Name); err != nil {
			return nil, fmt.Errorf("insert recipient %s: %w", ct.Email, err)
		}
		out = append(out, row)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}
	return out, nil
}

func (c *Client) GetCampaignRecipientStatus(ctx context.Context, recipientID, workspaceID string) (string, error) {
	const q = `SELECT status FROM campaign_recipients WHERE id = $1 AND workspace_id = $2`
	var status string
	err := c.Pool.QueryRow(ctx, q, recipientID, workspaceID).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	return status, nil
}

func (c *Client) MarkCampaignRecipientSending(ctx context.Context, recipientID, workspaceID string) error {
	const q = `UPDATE campaign_recipients
		SET status = 'sending', updated_at = NOW()
		WHERE id = $1 AND workspace_id = $2 AND status IN ('queued', 'failed', 'sending')`
	_, err := c.Pool.Exec(ctx, q, recipientID, workspaceID)
	return err
}

func (c *Client) MarkCampaignRecipientSent(
	ctx context.Context,
	recipientID, workspaceID, providerMessageID string,
) error {
	const q = `UPDATE campaign_recipients
		SET status = 'sent',
		    provider_message_id = $3,
		    failure_reason = NULL,
		    sent_at = COALESCE(sent_at, NOW()),
		    updated_at = NOW()
		WHERE id = $1 AND workspace_id = $2 AND status NOT IN ('bounced')`
	_, err := c.Pool.Exec(ctx, q, recipientID, workspaceID, providerMessageID)
	return err
}

func (c *Client) MarkCampaignRecipientFailed(
	ctx context.Context,
	recipientID, workspaceID, reason string,
) error {
	const q = `UPDATE campaign_recipients
		SET status = 'failed',
		    failure_reason = $3,
		    updated_at = NOW()
		WHERE id = $1 AND workspace_id = $2 AND status NOT IN ('sent', 'bounced')`
	_, err := c.Pool.Exec(ctx, q, recipientID, workspaceID, reason)
	return err
}

// ============================================================
// TTL cleanup queries
// ============================================================

// DeleteOldAuditLogs deletes up to limit audit_log rows older than retainFor.
// Returns the number of rows deleted.
func (c *Client) DeleteOldAuditLogs(ctx context.Context, retainFor time.Duration, limit int) (int64, error) {
	const q = `
		DELETE FROM audit_logs
		WHERE id IN (
			SELECT id FROM audit_logs
			WHERE created_at < NOW() - ($1 * INTERVAL '1 second')
			ORDER BY created_at
			LIMIT $2
		)`
	tag, err := c.Pool.Exec(ctx, q, retainFor.Seconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("delete old audit_logs: %w", err)
	}
	return tag.RowsAffected(), nil
}

// DeleteOldEvents deletes up to limit events_raw rows older than retainFor.
// Two categories are deleted:
//   - Terminal rows (processed/failed) older than retainFor — normal TTL cleanup.
//   - Stuck rows (pending/processing) older than retainFor — these were orphaned
//     by a worker crash and will never finish; keeping them wastes space forever.
//
// Cascades automatically to events_enriched and event_debug_logs via FK ON DELETE CASCADE.
// Returns the number of rows deleted.
func (c *Client) DeleteOldEvents(ctx context.Context, retainFor time.Duration, limit int) (int64, error) {
	const q = `
		DELETE FROM events_raw
		WHERE id IN (
			SELECT id FROM events_raw
			WHERE created_at < NOW() - ($1 * INTERVAL '1 second')
			ORDER BY created_at
			LIMIT $2
		)`
	tag, err := c.Pool.Exec(ctx, q, retainFor.Seconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("delete old events_raw: %w", err)
	}
	return tag.RowsAffected(), nil
}

// ============================================================
// events_raw table (event enrichment worker)
// ============================================================

// GetRawEventStatus returns the current processing status of a raw event.
// Returns "" if the row does not exist.
func (c *Client) GetRawEventStatus(ctx context.Context, eventID, workspaceID string) (string, error) {
	const q = `SELECT status FROM events_raw WHERE id = $1 AND workspace_id = $2`
	var status string
	err := c.Pool.QueryRow(ctx, q, eventID, workspaceID).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	return status, nil
}

// MarkRawEventProcessing transitions queued → processing. Idempotent.
func (c *Client) MarkRawEventProcessing(ctx context.Context, eventID, workspaceID string) error {
	const q = `UPDATE events_raw
		SET status = 'processing', updated_at = NOW()
		WHERE id = $1 AND workspace_id = $2 AND status IN ('queued', 'processing')`
	_, err := c.Pool.Exec(ctx, q, eventID, workspaceID)
	return err
}

// MarkRawEventProcessed transitions processing → processed.
func (c *Client) MarkRawEventProcessed(ctx context.Context, eventID, workspaceID string) error {
	const q = `UPDATE events_raw
		SET status = 'processed', processed_at = NOW(), updated_at = NOW()
		WHERE id = $1 AND workspace_id = $2`
	_, err := c.Pool.Exec(ctx, q, eventID, workspaceID)
	return err
}

// MarkRawEventFailed marks an event as permanently failed.
func (c *Client) MarkRawEventFailed(ctx context.Context, eventID, workspaceID, reason string) error {
	const q = `UPDATE events_raw
		SET status = 'failed', failure_reason = $3, updated_at = NOW()
		WHERE id = $1 AND workspace_id = $2`
	_, err := c.Pool.Exec(ctx, q, eventID, workspaceID, reason)
	return err
}

// IncrementRawEventAttempts increments the attempt counter and returns the new value.
func (c *Client) IncrementRawEventAttempts(ctx context.Context, eventID, workspaceID string) (int, error) {
	const q = `UPDATE events_raw
		SET attempts = COALESCE(attempts, 0) + 1, updated_at = NOW()
		WHERE id = $1 AND workspace_id = $2
		RETURNING attempts`
	var attempts int
	err := c.Pool.QueryRow(ctx, q, eventID, workspaceID).Scan(&attempts)
	return attempts, err
}

// ============================================================
// contacts / anonymous_profiles (identity resolution)
// ============================================================

// ContactRow is a resolved contact.
type ContactRow struct {
	ID          string
	WorkspaceID string
	UserID      string
	Email       string
	Name        string
	Traits      []byte // JSONB
}

// FindContactByUserID looks up a contact by userId within a workspace.
func (c *Client) FindContactByUserID(ctx context.Context, workspaceID, userID string) (*ContactRow, error) {
	const q = `SELECT id, workspace_id, user_id, COALESCE(email,''), COALESCE(first_name,''), COALESCE(traits,'{}')
		FROM contacts
		WHERE workspace_id = $1 AND user_id = $2
		LIMIT 1`
	row := &ContactRow{}
	err := c.Pool.QueryRow(ctx, q, workspaceID, userID).
		Scan(&row.ID, &row.WorkspaceID, &row.UserID, &row.Email, &row.Name, &row.Traits)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return row, nil
}

// FindContactByAnonymousID looks up a contact by anonymousId.
func (c *Client) FindContactByAnonymousID(ctx context.Context, workspaceID, anonymousID string) (*ContactRow, error) {
	const q = `SELECT id, workspace_id, COALESCE(user_id,''), COALESCE(email,''), COALESCE(first_name,''), COALESCE(traits,'{}')
		FROM contacts
		WHERE workspace_id = $1 AND anonymous_id = $2
		LIMIT 1`
	row := &ContactRow{}
	err := c.Pool.QueryRow(ctx, q, workspaceID, anonymousID).
		Scan(&row.ID, &row.WorkspaceID, &row.UserID, &row.Email, &row.Name, &row.Traits)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return row, nil
}

// UpsertContact creates or updates a contact by userId. Returns the contact ID.
// On conflict (workspace_id, user_id), merges traits via jsonb_strip_nulls.
func (c *Client) UpsertContact(
	ctx context.Context,
	workspaceID, userID, anonymousID string,
	traits []byte,
) (string, error) {
	const q = `INSERT INTO contacts (id, workspace_id, user_id, anonymous_id, traits, status, created_at, updated_at)
		VALUES (gen_random_uuid(), $1, $2, NULLIF($3,''), $4::jsonb, 'active', NOW(), NOW())
		ON CONFLICT (workspace_id, user_id) DO UPDATE
		SET traits = contacts.traits || EXCLUDED.traits,
		    anonymous_id = COALESCE(EXCLUDED.anonymous_id, contacts.anonymous_id),
		    updated_at = NOW()
		RETURNING id`
	var id string
	err := c.Pool.QueryRow(ctx, q, workspaceID, userID, anonymousID, traits).Scan(&id)
	return id, err
}

// UpsertAnonymousContact creates or updates a contact by anonymousId (no userId yet).
func (c *Client) UpsertAnonymousContact(
	ctx context.Context,
	workspaceID, anonymousID string,
	traits []byte,
) (string, error) {
	const q = `INSERT INTO contacts (id, workspace_id, anonymous_id, traits, status, created_at, updated_at)
		VALUES (gen_random_uuid(), $1, $2, $3::jsonb, 'active', NOW(), NOW())
		ON CONFLICT (workspace_id, anonymous_id) DO UPDATE
		SET traits = contacts.traits || EXCLUDED.traits,
		    updated_at = NOW()
		RETURNING id`
	var id string
	err := c.Pool.QueryRow(ctx, q, workspaceID, anonymousID, traits).Scan(&id)
	return id, err
}

// MergeAnonymousIntoContact links an anonymous profile to a known userId contact,
// transferring the anonymousId. Used for alias/identify events.
func (c *Client) MergeAnonymousIntoContact(
	ctx context.Context,
	workspaceID, contactID, anonymousID string,
) error {
	const q = `UPDATE contacts
		SET anonymous_id = $3, updated_at = NOW()
		WHERE id = $1 AND workspace_id = $2`
	_, err := c.Pool.Exec(ctx, q, contactID, workspaceID, anonymousID)
	return err
}

// ============================================================
// events_enriched table
// ============================================================

// EnrichedEventRow is the row written to events_enriched.
type EnrichedEventRow struct {
	WorkspaceID  string
	RawEventID   string
	ContactID    string
	UserID       string
	AnonymousID  string
	EventType    string
	EventName    string
	Properties   []byte // JSONB
	Traits       []byte // JSONB
	Context      []byte // JSONB
	EnrichedData []byte // JSONB
}

// InsertEnrichedEvent persists an enriched event. Idempotent via ON CONFLICT on raw_event_id.
func (c *Client) InsertEnrichedEvent(ctx context.Context, row EnrichedEventRow) error {
	const q = `INSERT INTO events_enriched
		(id, workspace_id, raw_event_id, contact_id, user_id, anonymous_id,
		 event_type, event_name, properties, traits, context, enriched_data, processed_at, created_at)
		VALUES (gen_random_uuid(), $1, $2, NULLIF($3,''), NULLIF($4,''), NULLIF($5,''),
		        $6, $7, $8::jsonb, $9::jsonb, $10::jsonb, $11::jsonb, NOW(), NOW())
		ON CONFLICT (raw_event_id) DO NOTHING`
	_, err := c.Pool.Exec(ctx, q,
		row.WorkspaceID, row.RawEventID, row.ContactID, row.UserID, row.AnonymousID,
		row.EventType, row.EventName, row.Properties, row.Traits, row.Context, row.EnrichedData,
	)
	return err
}

// ============================================================
// workflow_triggers table (MVP: simple rule matching)
// ============================================================

// WorkflowTriggerRule is a row from the workflow_triggers table.
type WorkflowTriggerRule struct {
	WorkflowID string
	EventName  string // empty = match all event names for this type
	EventType  string // track | identify | page | screen | alias | group
}

// FindMatchingWorkflowTriggers returns workflow trigger rules that match the
// given event type and name within a workspace.
func (c *Client) FindMatchingWorkflowTriggers(
	ctx context.Context,
	workspaceID, eventType, eventName string,
) ([]WorkflowTriggerRule, error) {
	const q = `SELECT workflow_id, COALESCE(event_name,''), COALESCE(event_type,'')
		FROM workflow_triggers
		WHERE workspace_id = $1
		  AND (event_type = $2 OR event_type IS NULL OR event_type = '')
		  AND (event_name = $3 OR event_name IS NULL OR event_name = '')
		  AND active = true`
	rows, err := c.Pool.Query(ctx, q, workspaceID, eventType, eventName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []WorkflowTriggerRule
	for rows.Next() {
		var r WorkflowTriggerRule
		if err := rows.Scan(&r.WorkflowID, &r.EventName, &r.EventType); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ============================================================
// segments table (segment computation worker)
// ============================================================

// SegmentRow is the loaded segment definition.
type SegmentRow struct {
	ID          string
	WorkspaceID string
	Name        string
	FilterTree  []byte // JSONB — the filter tree definition
	Status      string
}

// GetSegment loads a segment by ID within a workspace.
func (c *Client) GetSegment(ctx context.Context, segmentID, workspaceID string) (*SegmentRow, error) {
	const q = `SELECT id, workspace_id, name, COALESCE(filter_tree,'{}'), COALESCE(status,'queued')
		FROM segments
		WHERE id = $1 AND workspace_id = $2`
	row := &SegmentRow{}
	err := c.Pool.QueryRow(ctx, q, segmentID, workspaceID).
		Scan(&row.ID, &row.WorkspaceID, &row.Name, &row.FilterTree, &row.Status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return row, nil
}

// UpdateSegmentStatus sets the segment status and last_computed timestamp.
func (c *Client) UpdateSegmentStatus(ctx context.Context, segmentID, workspaceID, status string) error {
	const q = `UPDATE segments
		SET status = $3, last_computed = NOW(), updated_at = NOW()
		WHERE id = $1 AND workspace_id = $2`
	_, err := c.Pool.Exec(ctx, q, segmentID, workspaceID, status)
	return err
}

// UpdateSegmentReady sets status='ready' and contact_count atomically.
func (c *Client) UpdateSegmentReady(ctx context.Context, segmentID, workspaceID string, count int) error {
	const q = `UPDATE segments
		SET status = 'ready', contact_count = $3, last_computed = NOW(), updated_at = NOW()
		WHERE id = $1 AND workspace_id = $2`
	_, err := c.Pool.Exec(ctx, q, segmentID, workspaceID, count)
	return err
}

// ============================================================
// segment_memberships table
// ============================================================

// GetCurrentMemberIDs returns the set of contact IDs currently in the segment.
func (c *Client) GetCurrentMemberIDs(ctx context.Context, segmentID, workspaceID string) (map[string]struct{}, error) {
	const q = `SELECT contact_id FROM segment_memberships
		WHERE segment_id = $1 AND workspace_id = $2`
	rows, err := c.Pool.Query(ctx, q, segmentID, workspaceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]struct{})
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out[id] = struct{}{}
	}
	return out, rows.Err()
}

// InsertSegmentMembers bulk-inserts new segment memberships. Idempotent via ON CONFLICT.
func (c *Client) InsertSegmentMembers(ctx context.Context, segmentID, workspaceID string, contactIDs []string) error {
	if len(contactIDs) == 0 {
		return nil
	}
	const q = `INSERT INTO segment_memberships (segment_id, workspace_id, contact_id, created_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (segment_id, contact_id) DO NOTHING`
	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, id := range contactIDs {
		if _, err := tx.Exec(ctx, q, segmentID, workspaceID, id); err != nil {
			return fmt.Errorf("insert member %s: %w", id, err)
		}
	}
	return tx.Commit(ctx)
}

// DeleteSegmentMembers removes stale memberships.
func (c *Client) DeleteSegmentMembers(ctx context.Context, segmentID, workspaceID string, contactIDs []string) error {
	if len(contactIDs) == 0 {
		return nil
	}
	const q = `DELETE FROM segment_memberships
		WHERE segment_id = $1 AND workspace_id = $2 AND contact_id = $3`
	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, id := range contactIDs {
		if _, err := tx.Exec(ctx, q, segmentID, workspaceID, id); err != nil {
			return fmt.Errorf("delete member %s: %w", id, err)
		}
	}
	return tx.Commit(ctx)
}

// ============================================================
// contacts — segment filter evaluation
// ============================================================

// ContactForEval is a contact row used during filter evaluation.
type ContactForEval struct {
	ID             string
	WorkspaceID    string
	Email          string
	FirstName      string
	LastName       string
	Phone          string
	LifecycleStage string
	LeadScore      int
	Properties     []byte // JSONB
	CreatedAt      time.Time
}

// StreamContactsForEval streams all non-deleted contacts in a workspace in pages,
// calling fn for each page. Stops when fn returns an error or no more rows.
func (c *Client) StreamContactsForEval(
	ctx context.Context,
	workspaceID string,
	pageSize int,
	fn func([]ContactForEval) error,
) error {
	afterID := ""
	for {
		const q = `SELECT id, workspace_id, COALESCE(email,''),
			COALESCE(first_name,''), COALESCE(last_name,''), COALESCE(phone,''),
			COALESCE(lifecycle_stage,'lead'), COALESCE(lead_score,0),
			COALESCE(properties,'{}'), created_at
			FROM contacts
			WHERE workspace_id = $1
			  AND deleted_at IS NULL
			  AND ($2 = '' OR id > $2::uuid)
			ORDER BY id
			LIMIT $3`
		rows, err := c.Pool.Query(ctx, q, workspaceID, afterID, pageSize)
		if err != nil {
			return err
		}

		var page []ContactForEval
		for rows.Next() {
			var ct ContactForEval
			if err := rows.Scan(
				&ct.ID, &ct.WorkspaceID, &ct.Email,
				&ct.FirstName, &ct.LastName, &ct.Phone,
				&ct.LifecycleStage, &ct.LeadScore,
				&ct.Properties, &ct.CreatedAt,
			); err != nil {
				rows.Close()
				return err
			}
			page = append(page, ct)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(page) == 0 {
			break
		}
		if err := fn(page); err != nil {
			return err
		}
		afterID = page[len(page)-1].ID
		if len(page) < pageSize {
			break
		}
	}
	return nil
}

// ContactHasPerformedEvent returns true if the contact has at least one event
// matching the given event name within the workspace.
func (c *Client) ContactHasPerformedEvent(ctx context.Context, workspaceID, contactID, eventName string) (bool, error) {
	const q = `SELECT EXISTS(
		SELECT 1 FROM events_enriched
		WHERE workspace_id = $1 AND contact_id = $2 AND event_name = $3
		LIMIT 1
	)`
	var exists bool
	err := c.Pool.QueryRow(ctx, q, workspaceID, contactID, eventName).Scan(&exists)
	return exists, err
}

// ============================================================
// workflows table
// ============================================================

// WorkflowRow is a loaded workflow definition.
type WorkflowRow struct {
	ID          string
	WorkspaceID string
	Status      string // draft | published | archived
	Nodes       []byte // JSONB — ordered list of WorkflowNode
}

// WorkflowNode is a single node in the workflow definition.
type WorkflowNode struct {
	ID       string                 `json:"id"`
	Type     string                 `json:"type"` // trigger | email | delay | end
	Config   map[string]interface{} `json:"config"`
	NextNode string                 `json:"nextNode,omitempty"`
}

// GetWorkflow loads a workflow by ID within a workspace.
func (c *Client) GetWorkflow(ctx context.Context, workflowID, workspaceID string) (*WorkflowRow, error) {
	const q = `SELECT id, workspace_id, COALESCE(status,'draft'), COALESCE(nodes,'[]')
		FROM workflows
		WHERE id = $1 AND workspace_id = $2`
	row := &WorkflowRow{}
	err := c.Pool.QueryRow(ctx, q, workflowID, workspaceID).
		Scan(&row.ID, &row.WorkspaceID, &row.Status, &row.Nodes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return row, nil
}

// ============================================================
// workflow_executions table
// ============================================================

// ExecutionRow is a workflow execution instance.
type ExecutionRow struct {
	ID              string
	WorkspaceID     string
	WorkflowID      string
	ContactID       string
	Status          string // running | waiting | completed | failed
	CurrentNodeID   string
	NextRunAt       *time.Time
	RetryCount      int
	FailureReason   string
}

// CreateExecution inserts a new workflow execution. Returns the new execution ID.
// Idempotent via ON CONFLICT on (workflow_id, contact_id, trigger_event_id).
func (c *Client) CreateExecution(
	ctx context.Context,
	workflowID, workspaceID, contactID, triggerEventID string,
) (string, bool, error) {
	const q = `INSERT INTO workflow_executions
		(id, workflow_id, workspace_id, contact_id, trigger_event_id, status, created_at, updated_at)
		VALUES (gen_random_uuid(), $1, $2, $3, $4, 'running', NOW(), NOW())
		ON CONFLICT (workflow_id, contact_id, trigger_event_id) DO NOTHING
		RETURNING id`
	var id string
	err := c.Pool.QueryRow(ctx, q, workflowID, workspaceID, contactID, triggerEventID).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Conflict — execution already exists for this trigger.
			return "", false, nil
		}
		return "", false, err
	}
	return id, true, nil
}

// GetExecution loads an execution by ID.
func (c *Client) GetExecution(ctx context.Context, executionID string) (*ExecutionRow, error) {
	const q = `SELECT id, workspace_id, workflow_id, contact_id,
		COALESCE(status,'running'), COALESCE(current_node_id,''),
		next_run_at, COALESCE(retry_count,0), COALESCE(failure_reason,'')
		FROM workflow_executions WHERE id = $1`
	row := &ExecutionRow{}
	err := c.Pool.QueryRow(ctx, q, executionID).Scan(
		&row.ID, &row.WorkspaceID, &row.WorkflowID, &row.ContactID,
		&row.Status, &row.CurrentNodeID, &row.NextRunAt,
		&row.RetryCount, &row.FailureReason,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return row, nil
}

// AdvanceExecution updates the current node and status.
func (c *Client) AdvanceExecution(ctx context.Context, executionID, nodeID, status string) error {
	const q = `UPDATE workflow_executions
		SET current_node_id = $2, status = $3, updated_at = NOW()
		WHERE id = $1`
	_, err := c.Pool.Exec(ctx, q, executionID, nodeID, status)
	return err
}

// ScheduleDelay sets status='waiting' and next_run_at for a delay node.
func (c *Client) ScheduleDelay(ctx context.Context, executionID, nodeID string, nextRunAt time.Time) error {
	const q = `UPDATE workflow_executions
		SET status = 'waiting', current_node_id = $2, next_run_at = $3, updated_at = NOW()
		WHERE id = $1`
	_, err := c.Pool.Exec(ctx, q, executionID, nodeID, nextRunAt)
	return err
}

// CompleteExecution marks an execution as completed.
func (c *Client) CompleteExecution(ctx context.Context, executionID string) error {
	const q = `UPDATE workflow_executions
		SET status = 'completed', completed_at = NOW(), updated_at = NOW()
		WHERE id = $1`
	_, err := c.Pool.Exec(ctx, q, executionID)
	return err
}

// FailExecution marks an execution as failed.
func (c *Client) FailExecution(ctx context.Context, executionID, reason string) error {
	const q = `UPDATE workflow_executions
		SET status = 'failed', failure_reason = $2, updated_at = NOW()
		WHERE id = $1`
	_, err := c.Pool.Exec(ctx, q, executionID, reason)
	return err
}

// IncrementExecutionRetry increments retry_count and returns the new value.
func (c *Client) IncrementExecutionRetry(ctx context.Context, executionID string) (int, error) {
	const q = `UPDATE workflow_executions
		SET retry_count = COALESCE(retry_count, 0) + 1, updated_at = NOW()
		WHERE id = $1
		RETURNING retry_count`
	var count int
	err := c.Pool.QueryRow(ctx, q, executionID).Scan(&count)
	return count, err
}

// FetchDueExecutions returns executions in 'waiting' status whose next_run_at
// has passed. Used by the delay scheduler.
func (c *Client) FetchDueExecutions(ctx context.Context, limit int) ([]ExecutionRow, error) {
	const q = `SELECT id, workspace_id, workflow_id, contact_id,
		COALESCE(status,'waiting'), COALESCE(current_node_id,''),
		next_run_at, COALESCE(retry_count,0), COALESCE(failure_reason,'')
		FROM workflow_executions
		WHERE status = 'waiting' AND next_run_at <= NOW()
		ORDER BY next_run_at
		LIMIT $1`
	rows, err := c.Pool.Query(ctx, q, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ExecutionRow
	for rows.Next() {
		var r ExecutionRow
		if err := rows.Scan(
			&r.ID, &r.WorkspaceID, &r.WorkflowID, &r.ContactID,
			&r.Status, &r.CurrentNodeID, &r.NextRunAt,
			&r.RetryCount, &r.FailureReason,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// GetContactForWorkflow loads the minimal contact data needed to send an email.
func (c *Client) GetContactForWorkflow(ctx context.Context, contactID, workspaceID string) (*ContactRow, error) {
	return c.FindContactByUserID(ctx, workspaceID, contactID)
}

// ============================================================
// campaign scheduler
// ============================================================

// DueCampaign holds the fields needed to fire a scheduled campaign.
type DueCampaign struct {
	ID          string
	WorkspaceID string
	SegmentID   string
	SenderEmail string
	SenderName  string
	ReplyTo     string
	Subject     string
	HTMLBody    string
	TextBody    string
}

// GetDueCampaigns returns up to limit campaigns whose scheduled_at has passed
// and are still in 'scheduled' status. Atomically transitions them to 'sending'
// so no other scheduler instance picks them up.
func (c *Client) GetDueCampaigns(ctx context.Context, limit int) ([]DueCampaign, error) {
	const q = `
		UPDATE campaigns
		SET    status     = 'sending',
		       started_at = COALESCE(started_at, NOW()),
		       updated_at = NOW()
		WHERE  id IN (
		    SELECT id FROM campaigns
		    WHERE  status       = 'scheduled'
		      AND  scheduled_at <= NOW()
		      AND  deleted_at   IS NULL
		    ORDER  BY scheduled_at
		    LIMIT  $1
		    FOR UPDATE SKIP LOCKED
		)
		RETURNING id, workspace_id,
		          COALESCE(segment_id::text, '')   AS segment_id,
		          COALESCE(sender_email, '')        AS sender_email,
		          COALESCE(sender_name,  '')        AS sender_name,
		          COALESCE(reply_to,     '')        AS reply_to,
		          COALESCE(subject,      '')        AS subject,
		          COALESCE(html_body,    '')        AS html_body,
		          COALESCE(text_body,    '')        AS text_body`

	rows, err := c.Pool.Query(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("get due campaigns: %w", err)
	}
	defer rows.Close()

	var out []DueCampaign
	for rows.Next() {
		var d DueCampaign
		if err := rows.Scan(
			&d.ID, &d.WorkspaceID, &d.SegmentID,
			&d.SenderEmail, &d.SenderName, &d.ReplyTo,
			&d.Subject, &d.HTMLBody, &d.TextBody,
		); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
