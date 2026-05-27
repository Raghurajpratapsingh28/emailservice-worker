package events

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"

	"engageiq-workers/internal/infra/postgres"
	"engageiq-workers/pkg/types"
)

// ============================================================================
// Mocks
// ============================================================================

type dbOp struct {
	name  string
	id    string
	value string
}

type mockEventDB struct {
	mu sync.Mutex

	// State
	rawEventStatus  map[string]string
	contacts        map[string]*postgres.ContactRow // key: workspaceID+":"+userID
	anonContacts    map[string]*postgres.ContactRow // key: workspaceID+":"+anonymousID
	triggers        []postgres.WorkflowTriggerRule
	upsertContactID string
	upsertAnonID    string

	// Errors
	getStatusErr    error
	markErr         error
	enrichErr       error
	insertErr       error
	triggerErr      error
	findContactErr  error

	calls []dbOp
}

func (m *mockEventDB) GetRawEventStatus(_ context.Context, eventID, _ string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "get_status", id: eventID})
	if m.getStatusErr != nil {
		return "", m.getStatusErr
	}
	return m.rawEventStatus[eventID], nil
}

func (m *mockEventDB) MarkRawEventProcessing(_ context.Context, eventID, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "mark_processing", id: eventID})
	return m.markErr
}

func (m *mockEventDB) MarkRawEventProcessed(_ context.Context, eventID, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "mark_processed", id: eventID})
	if m.rawEventStatus == nil {
		m.rawEventStatus = map[string]string{}
	}
	m.rawEventStatus[eventID] = "processed"
	return nil
}

func (m *mockEventDB) MarkRawEventFailed(_ context.Context, eventID, _, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "mark_failed", id: eventID, value: reason})
	return nil
}

func (m *mockEventDB) IncrementRawEventAttempts(_ context.Context, eventID, _ string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "increment_attempts", id: eventID})
	return 1, nil
}

func (m *mockEventDB) InsertEnrichedEvent(_ context.Context, row postgres.EnrichedEventRow) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "insert_enriched", id: row.RawEventID, value: row.ContactID})
	return m.insertErr
}

func (m *mockEventDB) FindMatchingWorkflowTriggers(_ context.Context, _, _, _ string) ([]postgres.WorkflowTriggerRule, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "find_triggers"})
	return m.triggers, m.triggerErr
}

func (m *mockEventDB) FindContactByUserID(_ context.Context, workspaceID, userID string) (*postgres.ContactRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "find_by_user", id: userID})
	if m.findContactErr != nil {
		return nil, m.findContactErr
	}
	return m.contacts[workspaceID+":"+userID], nil
}

func (m *mockEventDB) FindContactByAnonymousID(_ context.Context, workspaceID, anonID string) (*postgres.ContactRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "find_by_anon", id: anonID})
	if m.findContactErr != nil {
		return nil, m.findContactErr
	}
	return m.anonContacts[workspaceID+":"+anonID], nil
}

func (m *mockEventDB) UpsertContact(_ context.Context, workspaceID, userID, anonID string, _ []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "upsert_contact", id: userID})
	if m.enrichErr != nil {
		return "", m.enrichErr
	}
	id := m.upsertContactID
	if id == "" {
		id = "contact-" + userID
	}
	return id, nil
}

func (m *mockEventDB) UpsertAnonymousContact(_ context.Context, workspaceID, anonID string, _ []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "upsert_anon", id: anonID})
	if m.enrichErr != nil {
		return "", m.enrichErr
	}
	id := m.upsertAnonID
	if id == "" {
		id = "anon-" + anonID
	}
	return id, nil
}

func (m *mockEventDB) MergeAnonymousIntoContact(_ context.Context, _, contactID, anonID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "merge_anon", id: contactID, value: anonID})
	return nil
}

func (m *mockEventDB) hasCall(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.calls {
		if c.name == name {
			return true
		}
	}
	return false
}

func (m *mockEventDB) callCount(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, c := range m.calls {
		if c.name == name {
			n++
		}
	}
	return n
}

type publishedMsg struct {
	subject string
	payload []byte
	msgID   string
}

type mockPublisher struct {
	mu   sync.Mutex
	msgs []publishedMsg
	err  error
}

func (m *mockPublisher) Publish(_ context.Context, subject string, payload any, msgID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	data, _ := json.Marshal(payload)
	m.msgs = append(m.msgs, publishedMsg{subject: subject, payload: data, msgID: msgID})
	return nil
}

func (m *mockPublisher) workflowCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, msg := range m.msgs {
		if msg.subject == SubjectWorkflowTrigger {
			n++
		}
	}
	return n
}

// mockMsg implements jetstream.Msg.
type mockMsg struct {
	data         []byte
	numDelivered uint64
	acked        bool
	naked        bool
	termed       bool
}

func (m *mockMsg) Data() []byte                       { return m.data }
func (m *mockMsg) Subject() string                    { return "events.raw.ws1" }
func (m *mockMsg) Reply() string                      { return "" }
func (m *mockMsg) Ack() error                         { m.acked = true; return nil }
func (m *mockMsg) Nak() error                         { m.naked = true; return nil }
func (m *mockMsg) NakWithDelay(_ time.Duration) error { m.naked = true; return nil }
func (m *mockMsg) InProgress() error                  { return nil }
func (m *mockMsg) Term() error                        { m.termed = true; return nil }
func (m *mockMsg) TermWithReason(_ string) error      { m.termed = true; return nil }
func (m *mockMsg) DoubleAck(_ context.Context) error  { m.acked = true; return nil }
func (m *mockMsg) Headers() nats.Header               { return nats.Header{} }
func (m *mockMsg) Metadata() (*jetstream.MsgMetadata, error) {
	d := m.numDelivered
	if d == 0 {
		d = 1
	}
	return &jetstream.MsgMetadata{NumDelivered: d}, nil
}

var _ jetstream.Msg = (*mockMsg)(nil)

// ============================================================================
// Helpers
// ============================================================================

func newHandler(db EventDB, pub EventPublisher, maxRetries int) *Handler {
	logger, _ := zap.NewDevelopment()
	return NewHandler(db, pub, maxRetries, logger)
}

func validTrackPayload() types.RawEventPayload {
	return types.RawEventPayload{
		EventID:     "evt-001",
		WorkspaceID: "ws-001",
		APIKeyID:    "key-001",
		EventType:   "track",
		EventName:   "Trial Upgraded",
		UserID:      "user_123",
		AnonymousID: "anon_abc",
		Traits:      map[string]interface{}{"plan": "pro"},
		Properties:  map[string]interface{}{"revenue": 99},
		Context:     map[string]interface{}{"ip": "1.2.3.4"},
		ReceivedAt:  time.Now(),
	}
}

func makeMsg(t *testing.T, p types.RawEventPayload, attempt uint64) *mockMsg {
	t.Helper()
	data, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return &mockMsg{data: data, numDelivered: attempt}
}

// ============================================================================
// Handler tests
// ============================================================================

func TestHandle_TrackEvent_Success(t *testing.T) {
	db := &mockEventDB{
		upsertContactID: "contact-1",
	}
	pub := &mockPublisher{}
	h := newHandler(db, pub, 5)

	msg := makeMsg(t, validTrackPayload(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !msg.acked {
		t.Fatal("expected Ack on success")
	}
	if !db.hasCall("mark_processed") {
		t.Fatal("expected mark_processed call")
	}
	if !db.hasCall("insert_enriched") {
		t.Fatal("expected insert_enriched call")
	}
}

func TestHandle_TrackEvent_ExistingContact_Linked(t *testing.T) {
	db := &mockEventDB{
		contacts: map[string]*postgres.ContactRow{
			"ws-001:user_123": {ID: "existing-contact", UserID: "user_123", Traits: []byte(`{"plan":"free"}`)},
		},
	}
	pub := &mockPublisher{}
	h := newHandler(db, pub, 5)

	msg := makeMsg(t, validTrackPayload(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.acked {
		t.Fatal("expected Ack")
	}
	// Should find by userId, not upsert
	if db.hasCall("upsert_contact") {
		t.Error("should not upsert when contact already exists")
	}
	if !db.hasCall("find_by_user") {
		t.Error("expected find_by_user call")
	}
}

func TestHandle_AnonymousOnly_CreatesAnonProfile(t *testing.T) {
	db := &mockEventDB{upsertAnonID: "anon-contact-1"}
	pub := &mockPublisher{}
	h := newHandler(db, pub, 5)

	p := validTrackPayload()
	p.UserID = "" // anonymous only
	msg := makeMsg(t, p, 1)

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.acked {
		t.Fatal("expected Ack")
	}
	if !db.hasCall("upsert_anon") {
		t.Fatal("expected upsert_anon for anonymous-only event")
	}
}

func TestHandle_IdentifyEvent_MergesTraitsAndLinksAnon(t *testing.T) {
	db := &mockEventDB{
		upsertContactID: "contact-identified",
		anonContacts: map[string]*postgres.ContactRow{
			"ws-001:anon_abc": {ID: "anon-contact-old"},
		},
	}
	pub := &mockPublisher{}
	h := newHandler(db, pub, 5)

	p := validTrackPayload()
	p.EventType = "identify"
	p.EventName = ""
	p.Traits = map[string]interface{}{"email": "alice@example.com", "plan": "pro"}
	msg := makeMsg(t, p, 1)

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.acked {
		t.Fatal("expected Ack")
	}
	if !db.hasCall("upsert_contact") {
		t.Fatal("expected upsert_contact for identify")
	}
	// Should merge anonymous profile into identified contact
	if !db.hasCall("merge_anon") {
		t.Fatal("expected merge_anon for identify with anonymousId")
	}
}

func TestHandle_AliasEvent_LinksAnonymousHistory(t *testing.T) {
	db := &mockEventDB{upsertContactID: "contact-canonical"}
	pub := &mockPublisher{}
	h := newHandler(db, pub, 5)

	p := validTrackPayload()
	p.EventType = "alias"
	p.EventName = ""
	msg := makeMsg(t, p, 1)

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.acked {
		t.Fatal("expected Ack")
	}
	if !db.hasCall("upsert_contact") {
		t.Fatal("expected upsert_contact for alias")
	}
	if !db.hasCall("merge_anon") {
		t.Fatal("expected merge_anon for alias")
	}
}

func TestHandle_MissingContact_CreatesNew(t *testing.T) {
	// No existing contacts — should create one.
	db := &mockEventDB{upsertContactID: "new-contact"}
	pub := &mockPublisher{}
	h := newHandler(db, pub, 5)

	msg := makeMsg(t, validTrackPayload(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.acked {
		t.Fatal("expected Ack")
	}
	if !db.hasCall("upsert_contact") {
		t.Fatal("expected upsert_contact when contact not found")
	}
}

func TestHandle_WorkflowTriggerMatch_PublishesEvent(t *testing.T) {
	db := &mockEventDB{
		upsertContactID: "contact-1",
		triggers: []postgres.WorkflowTriggerRule{
			{WorkflowID: "wf-001", EventName: "Trial Upgraded", EventType: "track"},
		},
	}
	pub := &mockPublisher{}
	h := newHandler(db, pub, 5)

	msg := makeMsg(t, validTrackPayload(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if pub.workflowCount() != 1 {
		t.Errorf("expected 1 workflow trigger, got %d", pub.workflowCount())
	}
	// Verify payload
	pub.mu.Lock()
	defer pub.mu.Unlock()
	var trigger types.WorkflowTriggerPayload
	_ = json.Unmarshal(pub.msgs[0].payload, &trigger)
	if trigger.WorkspaceID != "ws-001" || trigger.EventID != "evt-001" {
		t.Errorf("unexpected trigger payload: %+v", trigger)
	}
	// Verify dedup msgID
	if !strings.HasPrefix(pub.msgs[0].msgID, "wf-wf-001-") {
		t.Errorf("expected deterministic msgID, got %q", pub.msgs[0].msgID)
	}
}

func TestHandle_NoWorkflowMatch_NoPublish(t *testing.T) {
	db := &mockEventDB{
		upsertContactID: "contact-1",
		triggers:        nil, // no matching triggers
	}
	pub := &mockPublisher{}
	h := newHandler(db, pub, 5)

	msg := makeMsg(t, validTrackPayload(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if pub.workflowCount() != 0 {
		t.Errorf("expected 0 workflow triggers, got %d", pub.workflowCount())
	}
}

func TestHandle_DuplicateEvent_AcksWithoutProcessing(t *testing.T) {
	db := &mockEventDB{
		rawEventStatus: map[string]string{"evt-001": "processed"},
	}
	pub := &mockPublisher{}
	h := newHandler(db, pub, 5)

	msg := makeMsg(t, validTrackPayload(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.acked {
		t.Fatal("expected Ack on duplicate")
	}
	if db.hasCall("mark_processing") {
		t.Fatal("should not mark processing for duplicate")
	}
	if db.hasCall("insert_enriched") {
		t.Fatal("should not insert enriched for duplicate")
	}
}

func TestHandle_MalformedPayload_Terms(t *testing.T) {
	db := &mockEventDB{}
	pub := &mockPublisher{}
	h := newHandler(db, pub, 5)

	msg := &mockMsg{data: []byte("not json")}
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.termed {
		t.Fatal("expected Term for malformed payload")
	}
}

func TestHandle_InvalidPayload_MissingEventID_Terms(t *testing.T) {
	db := &mockEventDB{}
	pub := &mockPublisher{}
	h := newHandler(db, pub, 5)

	p := validTrackPayload()
	p.EventID = ""
	msg := makeMsg(t, p, 1)

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.termed {
		t.Fatal("expected Term for missing eventId")
	}
}

func TestHandle_EnrichmentFailure_TransientRetry(t *testing.T) {
	db := &mockEventDB{enrichErr: errors.New("db timeout")}
	pub := &mockPublisher{}
	h := newHandler(db, pub, 5)

	msg := makeMsg(t, validTrackPayload(), 2)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.naked {
		t.Fatal("expected Nak on transient enrichment failure")
	}
}

func TestHandle_EnrichmentFailure_AtMaxRetries_Terms(t *testing.T) {
	db := &mockEventDB{enrichErr: errors.New("db timeout")}
	pub := &mockPublisher{}
	h := newHandler(db, pub, 5)

	msg := makeMsg(t, validTrackPayload(), 5) // attempt = maxRetries
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.termed {
		t.Fatal("expected Term at max retries")
	}
	if !db.hasCall("mark_failed") {
		t.Fatal("expected mark_failed at max retries")
	}
}

func TestHandle_DBStatusCheckFails_Naks(t *testing.T) {
	db := &mockEventDB{getStatusErr: errors.New("db down")}
	pub := &mockPublisher{}
	h := newHandler(db, pub, 5)

	msg := makeMsg(t, validTrackPayload(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.naked {
		t.Fatal("expected Nak when status check fails")
	}
}

func TestHandle_InsertEnrichedFails_TransientRetry(t *testing.T) {
	db := &mockEventDB{
		upsertContactID: "contact-1",
		insertErr:       errors.New("db write error"),
	}
	pub := &mockPublisher{}
	h := newHandler(db, pub, 5)

	msg := makeMsg(t, validTrackPayload(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.naked {
		t.Fatal("expected Nak when insert enriched fails")
	}
}

// ============================================================================
// Enricher unit tests
// ============================================================================

func newEnricher(db ContactDB) *Enricher {
	logger, _ := zap.NewDevelopment()
	return NewEnricher(db, logger)
}

func TestEnricher_Track_ExistingContact(t *testing.T) {
	db := &mockEventDB{
		contacts: map[string]*postgres.ContactRow{
			"ws-001:user_123": {ID: "c1", Traits: []byte(`{"plan":"free"}`)},
		},
	}
	e := newEnricher(db)
	p := validTrackPayload()
	res, err := e.Enrich(context.Background(), &p)
	if err != nil {
		t.Fatal(err)
	}
	if res.ContactID != "c1" {
		t.Errorf("expected contact c1, got %q", res.ContactID)
	}
	// Traits should be merged: existing "plan:free" overridden by incoming "plan:pro"
	if res.MergedTraits["plan"] != "pro" {
		t.Errorf("expected merged plan=pro, got %v", res.MergedTraits["plan"])
	}
}

func TestEnricher_Track_AnonymousOnly_FallsBackToAnonLookup(t *testing.T) {
	db := &mockEventDB{
		anonContacts: map[string]*postgres.ContactRow{
			"ws-001:anon_abc": {ID: "anon-c1"},
		},
	}
	e := newEnricher(db)
	p := validTrackPayload()
	p.UserID = ""
	res, err := e.Enrich(context.Background(), &p)
	if err != nil {
		t.Fatal(err)
	}
	if res.ContactID != "anon-c1" {
		t.Errorf("expected anon-c1, got %q", res.ContactID)
	}
}

func TestEnricher_Identify_CreatesContactAndMergesAnon(t *testing.T) {
	db := &mockEventDB{
		upsertContactID: "new-contact",
		anonContacts: map[string]*postgres.ContactRow{
			"ws-001:anon_abc": {ID: "old-anon"},
		},
	}
	e := newEnricher(db)
	p := validTrackPayload()
	p.EventType = "identify"
	res, err := e.Enrich(context.Background(), &p)
	if err != nil {
		t.Fatal(err)
	}
	if res.ContactID != "new-contact" {
		t.Errorf("expected new-contact, got %q", res.ContactID)
	}
	if !db.hasCall("merge_anon") {
		t.Fatal("expected merge_anon for identify with existing anon profile")
	}
}

func TestEnricher_Alias_LinksAnonymousToCanonical(t *testing.T) {
	db := &mockEventDB{upsertContactID: "canonical"}
	e := newEnricher(db)
	p := validTrackPayload()
	p.EventType = "alias"
	res, err := e.Enrich(context.Background(), &p)
	if err != nil {
		t.Fatal(err)
	}
	if res.ContactID != "canonical" {
		t.Errorf("expected canonical, got %q", res.ContactID)
	}
	if !db.hasCall("merge_anon") {
		t.Fatal("expected merge_anon for alias")
	}
}

func TestEnricher_NoIdentity_ReturnsEmptyContactID(t *testing.T) {
	db := &mockEventDB{}
	e := newEnricher(db)
	p := validTrackPayload()
	p.UserID = ""
	p.AnonymousID = ""
	res, err := e.Enrich(context.Background(), &p)
	if err != nil {
		t.Fatal(err)
	}
	if res.ContactID != "" {
		t.Errorf("expected empty contactId for unlinked event, got %q", res.ContactID)
	}
}

// ============================================================================
// mergeTraits unit tests
// ============================================================================

func TestMergeTraits_IncomingOverridesExisting(t *testing.T) {
	existing := []byte(`{"plan":"free","name":"Alice"}`)
	incoming := map[string]interface{}{"plan": "pro", "email": "alice@example.com"}
	merged := mergeTraits(existing, incoming)
	if merged["plan"] != "pro" {
		t.Errorf("expected plan=pro, got %v", merged["plan"])
	}
	if merged["name"] != "Alice" {
		t.Errorf("expected name=Alice preserved, got %v", merged["name"])
	}
	if merged["email"] != "alice@example.com" {
		t.Errorf("expected email added, got %v", merged["email"])
	}
}

func TestMergeTraits_NilIncomingValueNotOverriding(t *testing.T) {
	existing := []byte(`{"plan":"pro"}`)
	incoming := map[string]interface{}{"plan": nil}
	merged := mergeTraits(existing, incoming)
	if merged["plan"] != "pro" {
		t.Errorf("nil incoming should not override existing, got %v", merged["plan"])
	}
}

func TestMergeTraits_EmptyExisting(t *testing.T) {
	merged := mergeTraits(nil, map[string]interface{}{"plan": "pro"})
	if merged["plan"] != "pro" {
		t.Errorf("expected plan=pro, got %v", merged["plan"])
	}
}

// ============================================================================
// validatePayload tests
// ============================================================================

func TestValidatePayload(t *testing.T) {
	cases := []struct {
		name string
		mod  func(*types.RawEventPayload)
		want string
	}{
		{"valid", func(p *types.RawEventPayload) {}, ""},
		{"missing eventId", func(p *types.RawEventPayload) { p.EventID = "" }, "eventId"},
		{"missing workspaceId", func(p *types.RawEventPayload) { p.WorkspaceID = "" }, "workspaceId"},
		{"missing eventType", func(p *types.RawEventPayload) { p.EventType = "" }, "eventType"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := validTrackPayload()
			tc.mod(&p)
			err := validatePayload(&p)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("expected error to contain %q, got %v", tc.want, err)
			}
		})
	}
}
