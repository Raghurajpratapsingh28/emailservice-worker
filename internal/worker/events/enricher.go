package events

import (
	"context"
	"encoding/json"
	"fmt"

	"go.uber.org/zap"

	"engageiq-workers/internal/infra/postgres"
	"engageiq-workers/pkg/types"
)

// EnrichmentResult is the output of the enrichment pipeline.
type EnrichmentResult struct {
	ContactID   string
	MergedTraits map[string]interface{}
	EnrichedData map[string]interface{}
}

// ContactDB is the persistence surface required by the enricher.
type ContactDB interface {
	FindContactByUserID(ctx context.Context, workspaceID, userID string) (*postgres.ContactRow, error)
	FindContactByAnonymousID(ctx context.Context, workspaceID, anonymousID string) (*postgres.ContactRow, error)
	UpsertContact(ctx context.Context, workspaceID, userID, anonymousID string, traits []byte) (string, error)
	UpsertAnonymousContact(ctx context.Context, workspaceID, anonymousID string, traits []byte) (string, error)
	MergeAnonymousIntoContact(ctx context.Context, workspaceID, contactID, anonymousID string) error
}

// Enricher resolves contacts and stitches identities for raw events.
type Enricher struct {
	db     ContactDB
	logger *zap.Logger
}

func NewEnricher(db ContactDB, logger *zap.Logger) *Enricher {
	return &Enricher{db: db, logger: logger}
}

// Enrich resolves the contact for the event, performs identity stitching,
// and returns the enrichment result. It is idempotent — re-running with the
// same event produces the same contact linkage.
func (e *Enricher) Enrich(ctx context.Context, ev *types.RawEventPayload) (EnrichmentResult, error) {
	res := EnrichmentResult{
		EnrichedData: map[string]interface{}{
			"eventType": ev.EventType,
		},
	}

	switch ev.EventType {
	case "identify":
		return e.handleIdentify(ctx, ev, res)
	case "alias":
		return e.handleAlias(ctx, ev, res)
	default:
		return e.handleTrack(ctx, ev, res)
	}
}

// handleTrack resolves the contact for track/page/screen/group events.
// Resolution order: userId → anonymousId → create anonymous profile.
func (e *Enricher) handleTrack(ctx context.Context, ev *types.RawEventPayload, res EnrichmentResult) (EnrichmentResult, error) {
	if ev.UserID != "" {
		contact, err := e.db.FindContactByUserID(ctx, ev.WorkspaceID, ev.UserID)
		if err != nil {
			return res, fmt.Errorf("find contact by userId: %w", err)
		}
		if contact != nil {
			res.ContactID = contact.ID
			res.MergedTraits = mergeTraits(contact.Traits, ev.Traits)
			return res, nil
		}
		// userId known but no contact yet — create one.
		traitsJSON, _ := json.Marshal(ev.Traits)
		id, err := e.db.UpsertContact(ctx, ev.WorkspaceID, ev.UserID, ev.AnonymousID, traitsJSON)
		if err != nil {
			return res, fmt.Errorf("upsert contact: %w", err)
		}
		res.ContactID = id
		res.MergedTraits = ev.Traits
		return res, nil
	}

	if ev.AnonymousID != "" {
		contact, err := e.db.FindContactByAnonymousID(ctx, ev.WorkspaceID, ev.AnonymousID)
		if err != nil {
			return res, fmt.Errorf("find contact by anonymousId: %w", err)
		}
		if contact != nil {
			res.ContactID = contact.ID
			res.MergedTraits = mergeTraits(contact.Traits, ev.Traits)
			return res, nil
		}
		// Anonymous profile not found — create one.
		traitsJSON, _ := json.Marshal(ev.Traits)
		id, err := e.db.UpsertAnonymousContact(ctx, ev.WorkspaceID, ev.AnonymousID, traitsJSON)
		if err != nil {
			return res, fmt.Errorf("upsert anonymous contact: %w", err)
		}
		res.ContactID = id
		res.MergedTraits = ev.Traits
		return res, nil
	}

	// No identity at all — event is unlinked.
	e.logger.Warn("event has no userId or anonymousId",
		zap.String("event_id", ev.EventID),
		zap.String("workspace_id", ev.WorkspaceID),
	)
	return res, nil
}

// handleIdentify merges traits into the contact and links anonymousId → userId.
// If the contact doesn't exist, it is created.
func (e *Enricher) handleIdentify(ctx context.Context, ev *types.RawEventPayload, res EnrichmentResult) (EnrichmentResult, error) {
	if ev.UserID == "" {
		// identify without userId is a no-op for identity stitching.
		return e.handleTrack(ctx, ev, res)
	}

	traitsJSON, _ := json.Marshal(ev.Traits)
	id, err := e.db.UpsertContact(ctx, ev.WorkspaceID, ev.UserID, ev.AnonymousID, traitsJSON)
	if err != nil {
		return res, fmt.Errorf("upsert contact (identify): %w", err)
	}
	res.ContactID = id
	res.MergedTraits = ev.Traits

	// If there's an anonymous profile for this anonymousId, link it.
	if ev.AnonymousID != "" {
		anonContact, err := e.db.FindContactByAnonymousID(ctx, ev.WorkspaceID, ev.AnonymousID)
		if err != nil {
			// Non-fatal: log and continue.
			e.logger.Warn("find anonymous contact failed during identify",
				zap.Error(err), zap.String("anonymous_id", ev.AnonymousID))
		} else if anonContact != nil && anonContact.ID != id {
			// Merge the anonymous profile into the identified contact.
			if err := e.db.MergeAnonymousIntoContact(ctx, ev.WorkspaceID, id, ev.AnonymousID); err != nil {
				e.logger.Warn("merge anonymous into contact failed",
					zap.Error(err), zap.String("contact_id", id))
			}
		}
	}

	res.EnrichedData["identified"] = true
	return res, nil
}

// handleAlias links an anonymous history to a known userId.
// Segment alias convention: userId = new canonical ID, anonymousId = previous ID.
func (e *Enricher) handleAlias(ctx context.Context, ev *types.RawEventPayload, res EnrichmentResult) (EnrichmentResult, error) {
	if ev.UserID == "" || ev.AnonymousID == "" {
		e.logger.Warn("alias event missing userId or anonymousId",
			zap.String("event_id", ev.EventID))
		return res, nil
	}

	// Ensure the canonical contact exists.
	traitsJSON, _ := json.Marshal(ev.Traits)
	id, err := e.db.UpsertContact(ctx, ev.WorkspaceID, ev.UserID, ev.AnonymousID, traitsJSON)
	if err != nil {
		return res, fmt.Errorf("upsert contact (alias): %w", err)
	}
	res.ContactID = id

	// Link the previous anonymous ID to the canonical contact.
	if err := e.db.MergeAnonymousIntoContact(ctx, ev.WorkspaceID, id, ev.AnonymousID); err != nil {
		return res, fmt.Errorf("merge alias: %w", err)
	}

	res.EnrichedData["aliased"] = true
	res.MergedTraits = ev.Traits
	return res, nil
}

// mergeTraits merges existing contact traits (base) with incoming event traits.
// Incoming traits take precedence for non-nil values.
func mergeTraits(existingJSON []byte, incoming map[string]interface{}) map[string]interface{} {
	merged := make(map[string]interface{})
	if len(existingJSON) > 0 {
		_ = json.Unmarshal(existingJSON, &merged)
	}
	for k, v := range incoming {
		if v != nil {
			merged[k] = v
		}
	}
	return merged
}
