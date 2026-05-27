package email

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"go.uber.org/zap"

	"engageiq-workers/internal/infra/postgres"
	infraSesAlias "engageiq-workers/internal/infra/ses"
	"engageiq-workers/pkg/types"
)

// ============================================================================
// Mocks for SegmentDB (start handler / batcher)
// ============================================================================

type segmentDBOp struct {
	name        string
	campaignID  string
	workspaceID string
	contacts    int
	total       int
	reason      string
}

type mockSegmentDB struct {
	mu sync.Mutex

	// Test setup
	statusByCampaign map[string]string
	contactPages     [][]postgres.Contact // each call returns next page; empty means done
	insertReturns    func(contacts []postgres.Contact) []postgres.CampaignRecipientRow

	// Errors
	getStatusErr  error
	fetchErr      error
	insertErr     error
	updateErr     error
	completeErr   error
	failedErr     error

	// Call log
	calls    []segmentDBOp
	pageIdx  int
}

func (m *mockSegmentDB) GetCampaignStatus(_ context.Context, campaignID, workspaceID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, segmentDBOp{name: "get_status", campaignID: campaignID, workspaceID: workspaceID})
	if m.getStatusErr != nil {
		return "", m.getStatusErr
	}
	return m.statusByCampaign[campaignID], nil
}

func (m *mockSegmentDB) FetchSegmentContactsBatch(
	_ context.Context, _, _, _ string, _ int,
) ([]postgres.Contact, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, segmentDBOp{name: "fetch_contacts"})
	if m.fetchErr != nil {
		return nil, m.fetchErr
	}
	if m.pageIdx >= len(m.contactPages) {
		return nil, nil
	}
	page := m.contactPages[m.pageIdx]
	m.pageIdx++
	return page, nil
}

func (m *mockSegmentDB) BulkInsertCampaignRecipients(
	_ context.Context, _, _ string, contacts []postgres.Contact,
) ([]postgres.CampaignRecipientRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, segmentDBOp{name: "bulk_insert", contacts: len(contacts)})
	if m.insertErr != nil {
		return nil, m.insertErr
	}
	if m.insertReturns != nil {
		return m.insertReturns(contacts), nil
	}
	out := make([]postgres.CampaignRecipientRow, 0, len(contacts))
	for _, c := range contacts {
		out = append(out, postgres.CampaignRecipientRow{ID: "rcpt-" + c.ID, Email: c.Email, Name: c.Name})
	}
	return out, nil
}

func (m *mockSegmentDB) UpdateCampaignStarted(
	_ context.Context, campaignID, workspaceID string, total int,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, segmentDBOp{name: "update_started", campaignID: campaignID, workspaceID: workspaceID, total: total})
	return m.updateErr
}

func (m *mockSegmentDB) MarkCampaignCompleteEmpty(
	_ context.Context, campaignID, workspaceID string,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, segmentDBOp{name: "complete_empty", campaignID: campaignID, workspaceID: workspaceID})
	return m.completeErr
}

func (m *mockSegmentDB) UpdateCampaignFailed(
	_ context.Context, campaignID, workspaceID, reason string,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, segmentDBOp{name: "update_failed", campaignID: campaignID, workspaceID: workspaceID, reason: reason})
	return m.failedErr
}

func (m *mockSegmentDB) hasCall(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.calls {
		if c.name == name {
			return true
		}
	}
	return false
}

func (m *mockSegmentDB) callCount(name string) int {
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

// ============================================================================
// Mocks for CampaignDB (chunk handler)
// ============================================================================

type campaignDBOp struct {
	name        string
	recipientID string
	value       string
	sent        int
	failed      int
}

type mockCampaignDB struct {
	mu sync.Mutex

	// State
	recipientStatus map[string]string
	progress        postgres.CampaignProgress
	completeReturns bool

	// Errors
	getStatusErr error
	updateErr    error
	incrErr      error

	calls []campaignDBOp
}

func (m *mockCampaignDB) GetCampaignRecipientStatus(_ context.Context, recipientID, _ string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, campaignDBOp{name: "get_status", recipientID: recipientID})
	if m.getStatusErr != nil {
		return "", m.getStatusErr
	}
	return m.recipientStatus[recipientID], nil
}

func (m *mockCampaignDB) MarkCampaignRecipientSending(_ context.Context, recipientID, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, campaignDBOp{name: "mark_sending", recipientID: recipientID})
	return m.updateErr
}

func (m *mockCampaignDB) MarkCampaignRecipientSent(_ context.Context, recipientID, _, msgID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, campaignDBOp{name: "mark_sent", recipientID: recipientID, value: msgID})
	if m.recipientStatus == nil {
		m.recipientStatus = map[string]string{}
	}
	m.recipientStatus[recipientID] = "sent"
	return m.updateErr
}

func (m *mockCampaignDB) MarkCampaignRecipientFailed(_ context.Context, recipientID, _, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, campaignDBOp{name: "mark_failed", recipientID: recipientID, value: reason})
	if m.recipientStatus == nil {
		m.recipientStatus = map[string]string{}
	}
	m.recipientStatus[recipientID] = "failed"
	return m.updateErr
}

func (m *mockCampaignDB) IncrementCampaignCounts(
	_ context.Context, _, _ string, sent, failed int,
) (postgres.CampaignProgress, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, campaignDBOp{name: "increment", sent: sent, failed: failed})
	if m.incrErr != nil {
		return postgres.CampaignProgress{}, m.incrErr
	}
	m.progress.SentCount += sent
	m.progress.FailedCount += failed
	return m.progress, nil
}

func (m *mockCampaignDB) MarkCampaignComplete(_ context.Context, _, _ string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, campaignDBOp{name: "complete"})
	return m.completeReturns, nil
}

func (m *mockCampaignDB) callCount(name string) int {
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

// ============================================================================
// Helpers
// ============================================================================

func makeContacts(n int) []postgres.Contact {
	out := make([]postgres.Contact, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, postgres.Contact{
			ID:    "contact-" + itoa(i),
			Email: "user" + itoa(i) + "@example.com",
			Name:  "User " + itoa(i),
		})
	}
	return out
}

func itoa(i int) string {
	// Avoid strconv to keep test deps minimal.
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	digits := []byte{}
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	if neg {
		return "-" + string(digits)
	}
	return string(digits)
}

func validCampaignStartPayload() types.CampaignStartPayload {
	return types.CampaignStartPayload{
		JobID:       "00000000-0000-0000-0000-000000000010",
		WorkspaceID: "00000000-0000-0000-0000-000000000020",
		CampaignID:  "00000000-0000-0000-0000-000000000030",
		SegmentID:   "00000000-0000-0000-0000-000000000040",
		Sender:      types.EmailAddress{Email: "hello@acme.com", Name: "Acme"},
		ReplyTo:     "support@acme.com",
		Subject:     "Welcome",
		HTML:        "<h1>Hello</h1>",
		Text:        "Hello",
	}
}

func validCampaignChunkPayload(numRecipients int) types.CampaignChunkPayload {
	r := make([]types.CampaignChunkRecipient, 0, numRecipients)
	for i := 0; i < numRecipients; i++ {
		r = append(r, types.CampaignChunkRecipient{
			RecipientID: "rcpt-" + itoa(i),
			Email:       "user" + itoa(i) + "@example.com",
		})
	}
	return types.CampaignChunkPayload{
		CampaignID:  "00000000-0000-0000-0000-000000000030",
		WorkspaceID: "00000000-0000-0000-0000-000000000020",
		ChunkID:     "chunk-1",
		Sender:      types.EmailAddress{Email: "hello@acme.com", Name: "Acme"},
		ReplyTo:     "support@acme.com",
		Subject:     "Welcome",
		HTML:        "<h1>Hello</h1>",
		Text:        "Hello",
		Recipients:  r,
	}
}

func makeChunkMsg(t *testing.T, p types.CampaignChunkPayload, attempt uint64) *mockMsg {
	t.Helper()
	data, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return &mockMsg{data: data, numDelivered: attempt}
}

func makeStartMsg(t *testing.T, p types.CampaignStartPayload, attempt uint64) *mockMsg {
	t.Helper()
	data, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return &mockMsg{data: data, numDelivered: attempt}
}

func newCampaignStartHandler(db SegmentDB, pub EventPublisher, chunkSize int) *CampaignStartHandler {
	logger, _ := zap.NewDevelopment()
	return NewCampaignStartHandler(db, pub, chunkSize, logger)
}

func newCampaignChunkHandler(ses SESSender, db CampaignDB, lim Limiter, pub EventPublisher) *CampaignChunkHandler {
	logger, _ := zap.NewDevelopment()
	h := NewCampaignChunkHandler(ses, db, lim, pub, logger)
	h.SetClassifier(func(err error) bool {
		return err != nil && strings.Contains(err.Error(), "permanent")
	})
	return h
}

// ============================================================================
// Start handler tests
// ============================================================================

func TestCampaignStart_EmptySegment_MarksCompleteAndAcks(t *testing.T) {
	db := &mockSegmentDB{
		statusByCampaign: map[string]string{},
		contactPages:     nil, // no pages → 0 contacts
	}
	pub := &mockPublisher{}
	h := newCampaignStartHandler(db, pub, 500)

	msg := makeStartMsg(t, validCampaignStartPayload(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if !msg.acked {
		t.Fatal("expected Ack on empty segment")
	}
	if !db.hasCall("complete_empty") {
		t.Fatal("expected MarkCampaignCompleteEmpty call")
	}
	// No chunks should be published
	pub.mu.Lock()
	chunkCount := 0
	for _, e := range pub.events {
		if e.subject == SubjectCampaignChunk {
			chunkCount++
		}
	}
	pub.mu.Unlock()
	if chunkCount != 0 {
		t.Errorf("expected 0 chunks for empty segment, got %d", chunkCount)
	}
}

func TestCampaignStart_HugeAudience_ChunksCorrectly(t *testing.T) {
	// 2,300 contacts split across 5 pages (500/500/500/500/300).
	db := &mockSegmentDB{
		contactPages: [][]postgres.Contact{
			makeContacts(500),
			makeContacts(500),
			makeContacts(500),
			makeContacts(500),
			makeContacts(300), // partial page → batcher stops here
		},
	}
	pub := &mockPublisher{}
	h := newCampaignStartHandler(db, pub, 500)

	msg := makeStartMsg(t, validCampaignStartPayload(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if !msg.acked {
		t.Fatal("expected Ack")
	}

	// Check chunk publish count
	pub.mu.Lock()
	chunks := 0
	totalRecipients := 0
	for _, e := range pub.events {
		if e.subject == SubjectCampaignChunk {
			chunks++
			var c types.CampaignChunkPayload
			_ = json.Unmarshal(e.payload, &c)
			totalRecipients += len(c.Recipients)
		}
	}
	pub.mu.Unlock()

	if chunks != 5 {
		t.Errorf("expected 5 chunks, got %d", chunks)
	}
	if totalRecipients != 2300 {
		t.Errorf("expected 2300 recipients across chunks, got %d", totalRecipients)
	}

	// Verify total_recipients was set
	db.mu.Lock()
	defer db.mu.Unlock()
	var found bool
	for _, c := range db.calls {
		if c.name == "update_started" && c.total == 2300 {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected UpdateCampaignStarted with total=2300, calls=%+v", db.calls)
	}
}

func TestCampaignStart_DeterministicChunkIDs(t *testing.T) {
	db := &mockSegmentDB{
		contactPages: [][]postgres.Contact{makeContacts(10)},
	}
	pub := &mockPublisher{}
	h := newCampaignStartHandler(db, pub, 500)

	msg := makeStartMsg(t, validCampaignStartPayload(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}

	pub.mu.Lock()
	defer pub.mu.Unlock()
	for _, e := range pub.events {
		if e.subject == SubjectCampaignChunk {
			expected := validCampaignStartPayload().CampaignID + "-chunk-0"
			if e.msgID != expected {
				t.Errorf("expected deterministic msgID %q, got %q", expected, e.msgID)
			}
		}
	}
}

func TestCampaignStart_AlreadyCompleted_SkipsAndAcks(t *testing.T) {
	db := &mockSegmentDB{
		statusByCampaign: map[string]string{
			validCampaignStartPayload().CampaignID: CampaignStatusCompleted,
		},
	}
	pub := &mockPublisher{}
	h := newCampaignStartHandler(db, pub, 500)

	msg := makeStartMsg(t, validCampaignStartPayload(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.acked {
		t.Fatal("expected Ack on already-completed")
	}
	if db.hasCall("fetch_contacts") {
		t.Fatal("should not fetch contacts when campaign is already terminal")
	}
}

func TestCampaignStart_MalformedPayload_Terms(t *testing.T) {
	db := &mockSegmentDB{}
	pub := &mockPublisher{}
	h := newCampaignStartHandler(db, pub, 500)

	msg := &mockMsg{data: []byte("not json")}
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.termed {
		t.Fatal("expected Term for malformed payload")
	}
}

func TestCampaignStart_InvalidPayload_Terms(t *testing.T) {
	db := &mockSegmentDB{}
	pub := &mockPublisher{}
	h := newCampaignStartHandler(db, pub, 500)

	bad := validCampaignStartPayload()
	bad.SegmentID = "" // missing required field
	msg := makeStartMsg(t, bad, 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.termed {
		t.Fatal("expected Term for invalid payload")
	}
}

func TestCampaignStart_DBFetchFailure_Naks(t *testing.T) {
	db := &mockSegmentDB{fetchErr: errors.New("db down")}
	pub := &mockPublisher{}
	h := newCampaignStartHandler(db, pub, 500)

	msg := makeStartMsg(t, validCampaignStartPayload(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.naked {
		t.Fatal("expected Nak when fetch fails")
	}
}

// ============================================================================
// Chunk handler tests
// ============================================================================

func TestCampaignChunk_AllSucceed_AcksAndUpdates(t *testing.T) {
	ses := &mockSES{messageID: "ses-msg-x"}
	db := &mockCampaignDB{progress: postgres.CampaignProgress{TotalRecipients: 3}}
	lim := &mockLimiter{}
	pub := &mockPublisher{}
	h := newCampaignChunkHandler(ses, db, lim, pub)

	msg := makeChunkMsg(t, validCampaignChunkPayload(3), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if !msg.acked {
		t.Fatal("expected Ack")
	}
	if len(ses.calls) != 3 {
		t.Errorf("expected 3 SES calls, got %d", len(ses.calls))
	}
	if db.callCount("mark_sent") != 3 {
		t.Errorf("expected 3 mark_sent, got %d", db.callCount("mark_sent"))
	}
	// Increment with sent=3 failed=0
	db.mu.Lock()
	defer db.mu.Unlock()
	var found bool
	for _, c := range db.calls {
		if c.name == "increment" && c.sent == 3 && c.failed == 0 {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected increment(sent=3, failed=0), calls=%+v", db.calls)
	}
}

func TestCampaignChunk_PermanentFailure_PerRecipient_AcksChunk(t *testing.T) {
	ses := &mockSES{err: errors.New("permanent: rejected")}
	db := &mockCampaignDB{progress: postgres.CampaignProgress{TotalRecipients: 2}}
	lim := &mockLimiter{}
	pub := &mockPublisher{}
	h := newCampaignChunkHandler(ses, db, lim, pub)

	msg := makeChunkMsg(t, validCampaignChunkPayload(2), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	// All recipients permanent — chunk acks (no transient remaining)
	if !msg.acked {
		t.Fatal("expected Ack when all recipients permanently failed")
	}
	if db.callCount("mark_failed") != 2 {
		t.Errorf("expected 2 mark_failed, got %d", db.callCount("mark_failed"))
	}
}

func TestCampaignChunk_TransientFailure_NaksForRetry(t *testing.T) {
	ses := &mockSES{err: errors.New("transient throttle")}
	db := &mockCampaignDB{}
	lim := &mockLimiter{}
	pub := &mockPublisher{}
	h := newCampaignChunkHandler(ses, db, lim, pub)

	msg := makeChunkMsg(t, validCampaignChunkPayload(2), 2)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.naked {
		t.Fatal("expected Nak on transient failure")
	}
	// No mark_failed yet — recipients still pending retry
	if db.callCount("mark_failed") != 0 {
		t.Errorf("transient should not mark failed, got %d", db.callCount("mark_failed"))
	}
}

func TestCampaignChunk_TransientAtMaxAttempts_TermsAndDLQ(t *testing.T) {
	ses := &mockSES{err: errors.New("transient throttle")}
	db := &mockCampaignDB{progress: postgres.CampaignProgress{TotalRecipients: 2}}
	lim := &mockLimiter{}
	pub := &mockPublisher{}
	h := newCampaignChunkHandler(ses, db, lim, pub)

	msg := makeChunkMsg(t, validCampaignChunkPayload(2), uint64(MaxAttempts))
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.termed {
		t.Fatal("expected Term at max attempts")
	}
	// All recipients should be marked failed
	if db.callCount("mark_failed") != 2 {
		t.Errorf("expected 2 mark_failed at DLQ time, got %d", db.callCount("mark_failed"))
	}
	// DLQ event should be published
	pub.mu.Lock()
	defer pub.mu.Unlock()
	var hasDLQ bool
	for _, e := range pub.events {
		if e.subject == SubjectCampaignDLQ {
			hasDLQ = true
			break
		}
	}
	if !hasDLQ {
		t.Fatal("expected DLQ publish on max attempts")
	}
}

func TestCampaignChunk_MixedSuccessAndPermanent_AcksAndIncrements(t *testing.T) {
	// Custom SES that fails the second send permanently.
	ses := &mockMixedSES{
		responses: []sesResponse{
			{messageID: "id-1"},
			{err: errors.New("permanent: rejected")},
			{messageID: "id-3"},
		},
	}
	db := &mockCampaignDB{progress: postgres.CampaignProgress{TotalRecipients: 3}}
	lim := &mockLimiter{}
	pub := &mockPublisher{}
	h := newCampaignChunkHandler(ses, db, lim, pub)

	msg := makeChunkMsg(t, validCampaignChunkPayload(3), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.acked {
		t.Fatal("expected Ack with mix of permanent failures and successes (no transient)")
	}
	if db.callCount("mark_sent") != 2 {
		t.Errorf("expected 2 mark_sent, got %d", db.callCount("mark_sent"))
	}
	if db.callCount("mark_failed") != 1 {
		t.Errorf("expected 1 mark_failed, got %d", db.callCount("mark_failed"))
	}
}

func TestCampaignChunk_DuplicateRecipient_SkippedNotResent(t *testing.T) {
	ses := &mockSES{messageID: "should-not-call"}
	db := &mockCampaignDB{
		recipientStatus: map[string]string{
			"rcpt-0": RecipientStatusSent,
			"rcpt-1": RecipientStatusSent,
		},
	}
	lim := &mockLimiter{}
	pub := &mockPublisher{}
	h := newCampaignChunkHandler(ses, db, lim, pub)

	msg := makeChunkMsg(t, validCampaignChunkPayload(2), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.acked {
		t.Fatal("expected Ack")
	}
	if len(ses.calls) != 0 {
		t.Errorf("SES should not be called for already-sent recipients, got %d", len(ses.calls))
	}
}

func TestCampaignChunk_MalformedPayload_Terms(t *testing.T) {
	ses := &mockSES{}
	db := &mockCampaignDB{}
	lim := &mockLimiter{}
	pub := &mockPublisher{}
	h := newCampaignChunkHandler(ses, db, lim, pub)

	msg := &mockMsg{data: []byte("not json")}
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.termed {
		t.Fatal("expected Term for malformed payload")
	}
}

func TestCampaignChunk_EmptyRecipients_Terms(t *testing.T) {
	ses := &mockSES{}
	db := &mockCampaignDB{}
	lim := &mockLimiter{}
	pub := &mockPublisher{}
	h := newCampaignChunkHandler(ses, db, lim, pub)

	bad := validCampaignChunkPayload(0) // empty recipients
	msg := makeChunkMsg(t, bad, 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.termed {
		t.Fatal("expected Term for empty recipients")
	}
}

func TestCampaignChunk_RateLimitDenied_RecipientTreatedAsTransient(t *testing.T) {
	ses := &mockSES{messageID: "id"}
	db := &mockCampaignDB{}
	lim := &mockLimiter{err: errors.New("rate limit exceeded")}
	pub := &mockPublisher{}
	h := newCampaignChunkHandler(ses, db, lim, pub)

	msg := makeChunkMsg(t, validCampaignChunkPayload(1), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.naked {
		t.Fatal("expected Nak when all recipients hit rate limit (transient)")
	}
	if len(ses.calls) != 0 {
		t.Fatal("SES must not be called when rate limited")
	}
}

func TestCampaignChunk_CompletionDetection(t *testing.T) {
	ses := &mockSES{messageID: "id"}
	// total=5, current=4 → after sending 1 we hit 5 and complete
	db := &mockCampaignDB{
		progress:        postgres.CampaignProgress{SentCount: 4, FailedCount: 0, TotalRecipients: 5},
		completeReturns: true,
	}
	lim := &mockLimiter{}
	pub := &mockPublisher{}
	h := newCampaignChunkHandler(ses, db, lim, pub)

	msg := makeChunkMsg(t, validCampaignChunkPayload(1), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.acked {
		t.Fatal("expected Ack")
	}
	if db.callCount("complete") != 1 {
		t.Errorf("expected MarkCampaignComplete called once, got %d", db.callCount("complete"))
	}
}

func TestCampaignChunk_PublishesDeliveryEvent(t *testing.T) {
	ses := &mockSES{messageID: "ses-id-42"}
	db := &mockCampaignDB{progress: postgres.CampaignProgress{TotalRecipients: 1}}
	lim := &mockLimiter{}
	pub := &mockPublisher{}
	h := newCampaignChunkHandler(ses, db, lim, pub)

	msg := makeChunkMsg(t, validCampaignChunkPayload(1), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}

	pub.mu.Lock()
	defer pub.mu.Unlock()
	var evt *types.EmailDeliveryEvent
	for _, e := range pub.events {
		if e.subject != SubjectEvents {
			continue
		}
		var d types.EmailDeliveryEvent
		_ = json.Unmarshal(e.payload, &d)
		if d.Status == StatusSent {
			evt = &d
			break
		}
	}
	if evt == nil {
		t.Fatal("expected sent delivery event")
	}
	if evt.CampaignID == "" {
		t.Error("event should include campaignId")
	}
	if evt.RecipientEmail == "" {
		t.Error("event should include recipientEmail")
	}
	if evt.ProviderMessageID != "ses-id-42" {
		t.Errorf("expected providerMessageId=ses-id-42, got %q", evt.ProviderMessageID)
	}
}

// ============================================================================
// mockMixedSES: returns different responses per call (for mixed-result tests)
// ============================================================================

type sesResponse struct {
	messageID string
	err       error
}

type mockMixedSES struct {
	mu        sync.Mutex
	responses []sesResponse
	idx       int
	calls     []sesCall
}

func (m *mockMixedSES) SendEmail(_ context.Context, in infraSesAlias.SendEmailInput) (infraSesAlias.SendEmailOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, sesCall{in: in})
	if m.idx >= len(m.responses) {
		return infraSesAlias.SendEmailOutput{}, errors.New("no more mocked responses")
	}
	r := m.responses[m.idx]
	m.idx++
	if r.err != nil {
		return infraSesAlias.SendEmailOutput{}, r.err
	}
	return infraSesAlias.SendEmailOutput{MessageID: r.messageID}, nil
}
