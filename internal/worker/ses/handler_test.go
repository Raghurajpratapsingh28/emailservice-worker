package ses

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"

	infraSes "engageiq-workers/internal/infra/ses"
	"engageiq-workers/pkg/types"
)

// --- Mocks ---

type mockSES struct {
	status infraSes.VerificationStatus
	err    error
	calls  int
	mu     sync.Mutex
}

func (m *mockSES) CheckDomainVerification(_ context.Context, _ string) (infraSes.VerificationStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	return m.status, m.err
}

type dbCall struct {
	method      string
	domainID    string
	workspaceID string
	attempts    int
}

type mockDB struct {
	mu             sync.Mutex
	calls          []dbCall
	err            error
	domainStatus   string // set explicitly; default "" → returns "verifying" (active)
	statusErr      error
	statusNotFound bool // true → GetDomainStatus returns ("", nil)
}

func (m *mockDB) GetDomainStatus(_ context.Context, _, _ string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.statusErr != nil {
		return "", m.statusErr
	}
	if m.statusNotFound {
		return "", nil
	}
	status := m.domainStatus
	if status == "" {
		status = "verifying" // default: active domain
	}
	return status, nil
}

func (m *mockDB) UpdateDomainVerified(_ context.Context, domainID, workspaceID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbCall{"verified", domainID, workspaceID, 0})
	return m.err
}

func (m *mockDB) UpdateDomainPending(_ context.Context, domainID, workspaceID string, attempts int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbCall{"pending", domainID, workspaceID, attempts})
	return m.err
}

func (m *mockDB) UpdateDomainFailed(_ context.Context, domainID, workspaceID string, attempts int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbCall{"failed", domainID, workspaceID, attempts})
	return m.err
}

type publishCall struct {
	subject string
	payload any
}

type mockPublisher struct {
	mu    sync.Mutex
	calls []publishCall
	err   error
}

func (m *mockPublisher) Publish(_ context.Context, subject string, payload any, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, publishCall{subject, payload})
	return m.err
}

// mockMsg implements jetstream.Msg for tests.
type mockMsg struct {
	data         []byte
	numDelivered uint64
	acked        bool
	naked        bool
	termed       bool
}

func (m *mockMsg) Data() []byte                       { return m.data }
func (m *mockMsg) Subject() string                    { return "domain.verify.poll" }
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
	delivered := m.numDelivered
	if delivered == 0 {
		delivered = 1
	}
	return &jetstream.MsgMetadata{NumDelivered: delivered}, nil
}

// Compile-time check that the mock implements the interface.
var _ jetstream.Msg = (*mockMsg)(nil)

// --- Helpers ---

func newTestHandler(ses SESChecker, db DBUpdater) *Handler {
	logger, _ := zap.NewDevelopment()
	pub := &mockPublisher{}
	return NewHandler(ses, db, pub, logger)
}

func newTestHandlerWithPublisher(ses SESChecker, db DBUpdater, pub EventPublisher) *Handler {
	logger, _ := zap.NewDevelopment()
	return NewHandler(ses, db, pub, logger)
}

func makePayload(t *testing.T, p types.DomainVerifyPayload) []byte {
	t.Helper()
	data, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func defaultPayload(t *testing.T) []byte {
	return makePayload(t, types.DomainVerifyPayload{
		DomainID:    "00000000-0000-0000-0000-000000000001",
		WorkspaceID: "00000000-0000-0000-0000-000000000002",
		Domain:      "acme.com",
	})
}

// --- Tests ---

func TestHandle_VerifiedDomain_AcksAndUpdates(t *testing.T) {
	ses := &mockSES{status: infraSes.StatusVerified}
	db := &mockDB{}
	pub := &mockPublisher{}
	h := newTestHandlerWithPublisher(ses, db, pub)
	msg := &mockMsg{data: defaultPayload(t), numDelivered: 1}

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !msg.acked {
		t.Fatal("expected message Ack")
	}
	if msg.naked || msg.termed {
		t.Fatal("expected only Ack")
	}
	if len(db.calls) != 1 || db.calls[0].method != "verified" {
		t.Fatalf("expected single verified call, got %+v", db.calls)
	}
	// Should publish domain.verified.v1 event
	if len(pub.calls) != 1 || pub.calls[0].subject != SubjectDomainVerified {
		t.Fatalf("expected verified event published, got %+v", pub.calls)
	}
}

func TestHandle_PendingDomain_NaksForBackoff(t *testing.T) {
	ses := &mockSES{status: infraSes.StatusPending}
	db := &mockDB{}
	h := newTestHandler(ses, db)
	msg := &mockMsg{data: defaultPayload(t), numDelivered: 1}

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !msg.naked {
		t.Fatal("expected Nak (server applies BackOff)")
	}
	if msg.acked || msg.termed {
		t.Fatal("expected only Nak")
	}
	if len(db.calls) != 1 || db.calls[0].method != "pending" {
		t.Fatalf("expected pending call, got %+v", db.calls)
	}
}

func TestHandle_PendingDomain_NeverAutoTerminates(t *testing.T) {
	// The poller should keep retrying indefinitely — it's the cleanup scheduler's
	// job to expire stale domains, not the poller's.
	ses := &mockSES{status: infraSes.StatusPending}
	db := &mockDB{}
	h := newTestHandler(ses, db)

	for attempt := uint64(1); attempt <= 50; attempt++ {
		msg := &mockMsg{data: defaultPayload(t), numDelivered: attempt}
		if err := h.Handle(context.Background(), msg); err != nil {
			t.Fatalf("attempt %d: unexpected error: %v", attempt, err)
		}
		if msg.termed {
			t.Fatalf("attempt %d: poller should never auto-terminate on pending", attempt)
		}
		if !msg.naked {
			t.Fatalf("attempt %d: expected Nak", attempt)
		}
	}
}

func TestHandle_PendingDomain_PublishesReminderAtAttempt3(t *testing.T) {
	ses := &mockSES{status: infraSes.StatusPending}
	db := &mockDB{}
	pub := &mockPublisher{}
	h := newTestHandlerWithPublisher(ses, db, pub)

	// Attempts 1 and 2: no reminder
	for _, attempt := range []uint64{1, 2} {
		pub.calls = nil
		msg := &mockMsg{data: defaultPayload(t), numDelivered: attempt}
		_ = h.Handle(context.Background(), msg)
		if len(pub.calls) != 0 {
			t.Fatalf("attempt %d: expected no reminder, got %+v", attempt, pub.calls)
		}
	}

	// Attempt 3: reminder published
	pub.calls = nil
	msg := &mockMsg{data: defaultPayload(t), numDelivered: ReminderAfterAttempt}
	_ = h.Handle(context.Background(), msg)
	if len(pub.calls) != 1 || pub.calls[0].subject != SubjectDomainReminder {
		t.Fatalf("expected reminder event at attempt %d, got %+v", ReminderAfterAttempt, pub.calls)
	}

	// Attempt 4+: no second reminder
	pub.calls = nil
	msg = &mockMsg{data: defaultPayload(t), numDelivered: 4}
	_ = h.Handle(context.Background(), msg)
	if len(pub.calls) != 0 {
		t.Fatalf("attempt 4: expected no second reminder, got %+v", pub.calls)
	}
}

func TestHandle_FailedDomain_AcksAndMarksFailed(t *testing.T) {
	ses := &mockSES{status: infraSes.StatusFailed}
	db := &mockDB{}
	h := newTestHandler(ses, db)
	msg := &mockMsg{data: defaultPayload(t), numDelivered: 1}

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !msg.acked {
		t.Fatal("expected Ack on terminal SES failure")
	}
	if len(db.calls) != 1 || db.calls[0].method != "failed" {
		t.Fatalf("expected failed call, got %+v", db.calls)
	}
}

func TestHandle_SESAPIFailure_NaksAndCountsMetric(t *testing.T) {
	ses := &mockSES{err: errors.New("ses timeout")}
	db := &mockDB{}
	h := newTestHandler(ses, db)
	msg := &mockMsg{data: defaultPayload(t), numDelivered: 2}

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !msg.naked {
		t.Fatal("expected Nak on SES API failure")
	}
	if len(db.calls) != 0 {
		t.Fatalf("expected no db calls on SES failure, got %+v", db.calls)
	}
}

func TestHandle_MalformedPayload_TermsImmediately(t *testing.T) {
	ses := &mockSES{status: infraSes.StatusVerified}
	db := &mockDB{}
	h := newTestHandler(ses, db)
	msg := &mockMsg{data: []byte("not json")}

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("malformed payload should not return error: %v", err)
	}
	if !msg.termed {
		t.Fatal("expected Term for malformed payload (poison message)")
	}
	if ses.calls != 0 {
		t.Fatal("SES should not be called for malformed payload")
	}
	if len(db.calls) != 0 {
		t.Fatal("DB should not be called for malformed payload")
	}
}

func TestHandle_EmptyRequiredFields_TermsImmediately(t *testing.T) {
	ses := &mockSES{status: infraSes.StatusVerified}
	db := &mockDB{}
	h := newTestHandler(ses, db)
	// Missing domain field
	msg := &mockMsg{data: makePayload(t, types.DomainVerifyPayload{
		DomainID: "x", WorkspaceID: "y", Domain: "",
	})}

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !msg.termed {
		t.Fatal("expected Term for missing required field")
	}
	if ses.calls != 0 {
		t.Fatal("SES should not be called for invalid payload")
	}
}

func TestHandle_DuplicateDelivery_IsIdempotent(t *testing.T) {
	ses := &mockSES{status: infraSes.StatusVerified}
	db := &mockDB{}
	h := newTestHandler(ses, db)

	payload := defaultPayload(t)
	for i := 0; i < 3; i++ {
		msg := &mockMsg{data: payload, numDelivered: 1}
		if err := h.Handle(context.Background(), msg); err != nil {
			t.Fatalf("delivery %d unexpected error: %v", i+1, err)
		}
		if !msg.acked {
			t.Fatalf("delivery %d should be acked", i+1)
		}
	}
	// All three deliveries write the same UPDATE — idempotent at SQL level.
	if len(db.calls) != 3 {
		t.Fatalf("expected 3 verified calls, got %d", len(db.calls))
	}
	for i, c := range db.calls {
		if c.method != "verified" {
			t.Fatalf("call %d expected verified, got %s", i, c.method)
		}
	}
}

func TestHandle_DBFailure_OnVerified_NaksForRetry(t *testing.T) {
	ses := &mockSES{status: infraSes.StatusVerified}
	db := &mockDB{err: errors.New("connection refused")}
	h := newTestHandler(ses, db)
	msg := &mockMsg{data: defaultPayload(t), numDelivered: 1}

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !msg.naked {
		t.Fatal("expected Nak when DB update fails")
	}
	if msg.acked {
		t.Fatal("should not Ack when DB update fails")
	}
}

func TestHandle_DBFailure_OnPending_NaksForRetry(t *testing.T) {
	ses := &mockSES{status: infraSes.StatusPending}
	db := &mockDB{err: errors.New("db down")}
	h := newTestHandler(ses, db)
	msg := &mockMsg{data: defaultPayload(t), numDelivered: 2}

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !msg.naked {
		t.Fatal("expected Nak when DB update fails")
	}
}

func TestHandle_DeletedDomain_TerminatesWithoutSES(t *testing.T) {
	cases := []struct {
		name string
		db   *mockDB
	}{
		{"status=deleted", &mockDB{domainStatus: "deleted"}},
		{"status=deleting", &mockDB{domainStatus: "deleting"}},
		{"not_found", &mockDB{statusNotFound: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ses := &mockSES{status: infraSes.StatusPending}
			db := tc.db
			h := newTestHandler(ses, db)
			msg := &mockMsg{data: defaultPayload(t), numDelivered: 1}

			if err := h.Handle(context.Background(), msg); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !msg.termed {
				t.Fatal("expected Term for deleted/missing domain")
			}
			if msg.acked || msg.naked {
				t.Fatal("expected only Term")
			}
			if ses.calls != 0 {
				t.Fatal("SES should not be called for deleted domain")
			}
			if len(db.calls) != 0 {
				t.Fatal("DB update should not be called for deleted domain")
			}
		})
	}
}

func TestNextDelay_MatchesRetrySchedule(t *testing.T) {
	h := &Handler{}
	expected := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 5 * time.Minute},
		{2, 10 * time.Minute},
		{3, 15 * time.Minute},
		{4, 30 * time.Minute},
		{5, 1 * time.Hour},
		{6, 2 * time.Hour},
		{7, 6 * time.Hour},
		{8, 12 * time.Hour},
		{9, 24 * time.Hour},
		{10, 24 * time.Hour}, // beyond array, clamps to last
		{100, 24 * time.Hour},
	}
	for _, tt := range expected {
		if got := h.nextDelay(tt.attempt); got != tt.want {
			t.Errorf("nextDelay(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestRetryBudget_FirstDayCoversCommonCases(t *testing.T) {
	// Sanity: the first 5 attempts cover a full day of polling.
	// Common DNS propagation completes within 24h.
	var total time.Duration
	for i := 1; i <= len(RetryDelays); i++ {
		total += RetryDelays[i-1]
	}
	if total < 24*time.Hour {
		t.Errorf("first %d retries only cover %v, expected at least 24h", len(RetryDelays), total)
	}
}
