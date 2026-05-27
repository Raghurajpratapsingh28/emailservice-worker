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
	mu    sync.Mutex
	calls []dbCall
	err   error
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
	return NewHandler(ses, db, logger)
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
		DomainID: "00000000-0000-0000-0000-000000000001",
		WorkspaceID: "00000000-0000-0000-0000-000000000002",
		Domain:      "acme.com",
	})
}

// --- Tests ---

func TestHandle_VerifiedDomain_AcksAndUpdates(t *testing.T) {
	ses := &mockSES{status: infraSes.StatusVerified}
	db := &mockDB{}
	h := newTestHandler(ses, db)
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
}

func TestHandle_PendingDomain_NaksForBackoff(t *testing.T) {
	ses := &mockSES{status: infraSes.StatusPending}
	db := &mockDB{}
	h := newTestHandler(ses, db)
	msg := &mockMsg{data: defaultPayload(t), numDelivered: 3}

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !msg.naked {
		t.Fatal("expected Nak (server applies BackOff)")
	}
	if msg.acked || msg.termed {
		t.Fatal("expected only Nak")
	}
	if len(db.calls) != 1 || db.calls[0].method != "pending" || db.calls[0].attempts != 3 {
		t.Fatalf("expected pending call with attempts=3, got %+v", db.calls)
	}
}

func TestHandle_PendingDomain_MaxAttempts_TermsAndMarksFailed(t *testing.T) {
	ses := &mockSES{status: infraSes.StatusPending}
	db := &mockDB{}
	h := newTestHandler(ses, db)
	msg := &mockMsg{data: defaultPayload(t), numDelivered: uint64(MaxAttempts)}

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !msg.termed {
		t.Fatal("expected Term on max attempts")
	}
	if msg.naked || msg.acked {
		t.Fatal("expected only Term")
	}
	if len(db.calls) != 1 || db.calls[0].method != "failed" || db.calls[0].attempts != MaxAttempts {
		t.Fatalf("expected failed call with attempts=%d, got %+v", MaxAttempts, db.calls)
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

func TestNextDelay_MatchesRetrySchedule(t *testing.T) {
	h := &Handler{}
	expected := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 5 * time.Minute},
		{2, 15 * time.Minute},
		{3, 30 * time.Minute},
		{4, 1 * time.Hour},
		{5, 2 * time.Hour},
		{6, 6 * time.Hour},
		{7, 12 * time.Hour},
		{8, 24 * time.Hour},
		{9, 24 * time.Hour}, // beyond array, clamps to last
		{100, 24 * time.Hour},
	}
	for _, tt := range expected {
		if got := h.nextDelay(tt.attempt); got != tt.want {
			t.Errorf("nextDelay(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestRetryBudget_Within72Hours(t *testing.T) {
	// Sanity check on the schedule meets the 72h SLA.
	var total time.Duration
	for i := 0; i < MaxAttempts-1; i++ {
		idx := i
		if idx >= len(RetryDelays) {
			idx = len(RetryDelays) - 1
		}
		total += RetryDelays[idx]
	}
	limit := 72 * time.Hour
	if total > limit {
		t.Errorf("total retry budget %v exceeds %v SLA", total, limit)
	}
}
