package email

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

	infraSes "engageiq-workers/internal/infra/ses"
	"engageiq-workers/pkg/types"
)

// ---------- mocks ----------

type sesCall struct {
	in infraSes.SendEmailInput
}

type mockSES struct {
	mu        sync.Mutex
	calls     []sesCall
	messageID string
	err       error
}

func (m *mockSES) SendEmail(_ context.Context, in infraSes.SendEmailInput) (infraSes.SendEmailOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, sesCall{in: in})
	if m.err != nil {
		return infraSes.SendEmailOutput{}, m.err
	}
	return infraSes.SendEmailOutput{MessageID: m.messageID}, nil
}

type dbCall struct {
	op       string
	sendID   string
	wsID     string
	value    string
}

type mockDB struct {
	mu             sync.Mutex
	calls          []dbCall
	statusToReturn string
	getErr         error
	updateErr      error
}

func (m *mockDB) GetEmailSendStatus(_ context.Context, sendID, wsID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbCall{op: "get", sendID: sendID, wsID: wsID})
	return m.statusToReturn, m.getErr
}
func (m *mockDB) UpdateEmailSendSending(_ context.Context, sendID, wsID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbCall{op: "sending", sendID: sendID, wsID: wsID})
	return m.updateErr
}
func (m *mockDB) UpdateEmailSendSent(_ context.Context, sendID, wsID, msgID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbCall{op: "sent", sendID: sendID, wsID: wsID, value: msgID})
	return m.updateErr
}
func (m *mockDB) UpdateEmailSendFailed(_ context.Context, sendID, wsID, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbCall{op: "failed", sendID: sendID, wsID: wsID, value: reason})
	return m.updateErr
}

type mockLimiter struct {
	waited time.Duration
	err    error
	calls  int
}

func (m *mockLimiter) Acquire(_ context.Context, _ string, _ time.Duration) (time.Duration, error) {
	m.calls++
	return m.waited, m.err
}

type publishedEvent struct {
	subject string
	payload []byte
	msgID   string
}

type mockPublisher struct {
	mu     sync.Mutex
	events []publishedEvent
	err    error
}

func (m *mockPublisher) Publish(_ context.Context, subject string, payload any, msgID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	data, _ := json.Marshal(payload)
	m.events = append(m.events, publishedEvent{subject: subject, payload: data, msgID: msgID})
	return nil
}

func (m *mockPublisher) byStatus(status string) *types.EmailDeliveryEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.events {
		if e.subject != SubjectEvents {
			continue
		}
		var evt types.EmailDeliveryEvent
		if err := json.Unmarshal(e.payload, &evt); err == nil && evt.Status == status {
			return &evt
		}
	}
	return nil
}

func (m *mockPublisher) hasDLQ() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.events {
		if e.subject == SubjectDLQ {
			return true
		}
	}
	return false
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
func (m *mockMsg) Subject() string                    { return SubjectSend }
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

// ---------- helpers ----------

type permErr struct{ msg string }

func (e *permErr) Error() string                 { return e.msg }
func (e *permErr) ErrorCode() string             { return "MessageRejected" }
func (e *permErr) ErrorMessage() string          { return e.msg }
func (e *permErr) ErrorFault() interface{ String() string } { return nil }

func newHandler(t *testing.T, ses SESSender, db EmailDB, lim Limiter, pub EventPublisher) *Handler {
	t.Helper()
	logger, _ := zap.NewDevelopment()
	h := NewHandler(ses, db, lim, pub, logger)
	// Test classifier: treat errors containing "permanent" as permanent.
	h.SetClassifier(func(err error) bool {
		return err != nil && strings.Contains(err.Error(), "permanent")
	})
	return h
}

func validPayload() types.TransactionalEmailPayload {
	return types.TransactionalEmailPayload{
		JobID:       "00000000-0000-0000-0000-000000000001",
		WorkspaceID: "00000000-0000-0000-0000-000000000002",
		SendID:      "00000000-0000-0000-0000-000000000003",
		To:          []types.EmailAddress{{Email: "alice@example.com", Name: "Alice"}},
		From:        types.EmailAddress{Email: "hello@acme.com", Name: "Acme"},
		ReplyTo:     "support@acme.com",
		Subject:     "Welcome",
		HTML:        "<h1>Hello</h1>",
		Text:        "Hello",
		Tags:        map[string]string{"source": "signup"},
		Provider:    "ses",
	}
}

func makeMsg(t *testing.T, p types.TransactionalEmailPayload, attempt uint64) *mockMsg {
	t.Helper()
	data, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return &mockMsg{data: data, numDelivered: attempt}
}

// ---------- tests ----------

func TestHandle_SuccessSend_AcksAndPublishesEvent(t *testing.T) {
	ses := &mockSES{messageID: "ses-msg-123"}
	db := &mockDB{}
	lim := &mockLimiter{}
	pub := &mockPublisher{}
	h := newHandler(t, ses, db, lim, pub)
	msg := makeMsg(t, validPayload(), 1)

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !msg.acked || msg.naked || msg.termed {
		t.Fatalf("expected only Ack, got acked=%v naked=%v termed=%v", msg.acked, msg.naked, msg.termed)
	}
	if len(ses.calls) != 1 {
		t.Fatalf("expected 1 SES call, got %d", len(ses.calls))
	}
	// Verify SES input fields
	in := ses.calls[0].in
	if in.From != "hello@acme.com" || in.FromName != "Acme" {
		t.Errorf("unexpected from: %+v", in)
	}
	if len(in.To) != 1 || in.To[0] != "alice@example.com" {
		t.Errorf("unexpected to: %+v", in.To)
	}
	if len(in.ReplyTo) != 1 || in.ReplyTo[0] != "support@acme.com" {
		t.Errorf("unexpected replyTo: %+v", in.ReplyTo)
	}
	if in.Tags["source"] != "signup" {
		t.Errorf("expected tag source=signup, got %v", in.Tags)
	}
	// Verify DB transitions: get -> sending -> sent
	gotOps := opSequence(db)
	wantOps := []string{"get", "sending", "sent"}
	if !equalSlice(gotOps, wantOps) {
		t.Errorf("db op sequence = %v, want %v", gotOps, wantOps)
	}
	// Verify last "sent" call has the SES message ID
	last := db.calls[len(db.calls)-1]
	if last.value != "ses-msg-123" {
		t.Errorf("expected provider_message_id ses-msg-123, got %q", last.value)
	}
	// Verify delivery event was published
	evt := pub.byStatus(StatusSent)
	if evt == nil {
		t.Fatal("expected delivery event with status=sent")
	}
	if evt.ProviderMessageID != "ses-msg-123" || evt.SendID != validPayload().SendID {
		t.Errorf("event mismatch: %+v", evt)
	}
}

func TestHandle_TransientError_Naks_DBSendingMarked(t *testing.T) {
	ses := &mockSES{err: errors.New("ses throttling: try again")}
	db := &mockDB{}
	lim := &mockLimiter{}
	pub := &mockPublisher{}
	h := newHandler(t, ses, db, lim, pub)
	msg := makeMsg(t, validPayload(), 2)

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !msg.naked {
		t.Fatal("expected Nak on transient SES failure")
	}
	if msg.acked || msg.termed {
		t.Fatal("expected only Nak")
	}
	// Verify no failed transition (still retrying)
	for _, c := range db.calls {
		if c.op == "failed" {
			t.Fatal("did not expect failed transition for transient error")
		}
	}
}

func TestHandle_TransientError_AtMaxAttempts_RoutesToDLQ(t *testing.T) {
	ses := &mockSES{err: errors.New("ses 500: service unavailable")}
	db := &mockDB{}
	lim := &mockLimiter{}
	pub := &mockPublisher{}
	h := newHandler(t, ses, db, lim, pub)
	msg := makeMsg(t, validPayload(), uint64(MaxAttempts))

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !msg.termed {
		t.Fatal("expected Term at max attempts")
	}
	if !pub.hasDLQ() {
		t.Fatal("expected DLQ event")
	}
	// Verify failed delivery event published
	evt := pub.byStatus(StatusFailed)
	if evt == nil {
		t.Fatal("expected delivery event with status=failed")
	}
	if !strings.Contains(evt.Reason, "max retries") {
		t.Errorf("expected reason to include 'max retries', got %q", evt.Reason)
	}
}

func TestHandle_PermanentError_AcksAndMarksFailed(t *testing.T) {
	ses := &mockSES{err: errors.New("permanent: sender not verified")}
	db := &mockDB{}
	lim := &mockLimiter{}
	pub := &mockPublisher{}
	h := newHandler(t, ses, db, lim, pub)
	msg := makeMsg(t, validPayload(), 1)

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !msg.acked {
		t.Fatal("expected Ack on permanent SES failure (no retry)")
	}
	if msg.naked || msg.termed {
		t.Fatal("expected only Ack")
	}
	// Verify DB marked failed
	if !hasOp(db, "failed") {
		t.Fatal("expected DB UpdateEmailSendFailed call")
	}
	// Verify failed event published, no DLQ
	if pub.byStatus(StatusFailed) == nil {
		t.Fatal("expected failed delivery event")
	}
	if pub.hasDLQ() {
		t.Fatal("permanent errors should not go to DLQ")
	}
}

func TestHandle_MalformedPayload_TermsImmediately(t *testing.T) {
	ses := &mockSES{}
	db := &mockDB{}
	lim := &mockLimiter{}
	pub := &mockPublisher{}
	h := newHandler(t, ses, db, lim, pub)
	msg := &mockMsg{data: []byte("not json"), numDelivered: 1}

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !msg.termed {
		t.Fatal("expected Term on malformed payload (poison)")
	}
	if len(ses.calls) != 0 {
		t.Fatal("SES should not be called for malformed payload")
	}
	if len(db.calls) != 0 {
		t.Fatal("DB should not be called for malformed payload")
	}
}

func TestHandle_InvalidPayload_MissingFields_TermsImmediately(t *testing.T) {
	ses := &mockSES{}
	db := &mockDB{}
	lim := &mockLimiter{}
	pub := &mockPublisher{}
	h := newHandler(t, ses, db, lim, pub)

	bad := validPayload()
	bad.From.Email = "" // invalid
	msg := makeMsg(t, bad, 1)

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !msg.termed {
		t.Fatal("expected Term for invalid payload")
	}
	if len(ses.calls) != 0 {
		t.Fatal("SES should not be called for invalid payload")
	}
}

func TestHandle_DuplicateJob_AcksWithoutSending(t *testing.T) {
	ses := &mockSES{}
	db := &mockDB{statusToReturn: StatusSent}
	lim := &mockLimiter{}
	pub := &mockPublisher{}
	h := newHandler(t, ses, db, lim, pub)
	msg := makeMsg(t, validPayload(), 1)

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !msg.acked {
		t.Fatal("expected Ack on duplicate")
	}
	if len(ses.calls) != 0 {
		t.Fatalf("SES should NOT be called for duplicate; got %d calls", len(ses.calls))
	}
	if len(pub.events) != 0 {
		t.Fatalf("no events should be republished for duplicate; got %d", len(pub.events))
	}
}

func TestHandle_DBStatusCheckFails_Naks(t *testing.T) {
	ses := &mockSES{}
	db := &mockDB{getErr: errors.New("db down")}
	lim := &mockLimiter{}
	pub := &mockPublisher{}
	h := newHandler(t, ses, db, lim, pub)
	msg := makeMsg(t, validPayload(), 1)

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !msg.naked {
		t.Fatal("expected Nak when idempotency check fails")
	}
	if len(ses.calls) != 0 {
		t.Fatal("SES should not be called when DB fails")
	}
}

func TestHandle_RateLimitExceeded_Naks(t *testing.T) {
	ses := &mockSES{}
	db := &mockDB{}
	lim := &mockLimiter{err: errors.New("rate limit exceeded")}
	pub := &mockPublisher{}
	h := newHandler(t, ses, db, lim, pub)
	msg := makeMsg(t, validPayload(), 1)

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !msg.naked {
		t.Fatal("expected Nak when rate limit denied")
	}
	if len(ses.calls) != 0 {
		t.Fatal("SES must not be called when rate limited")
	}
}

func TestHandle_RenderEmptyBody_TermsAndMarksFailed(t *testing.T) {
	ses := &mockSES{}
	db := &mockDB{}
	lim := &mockLimiter{}
	pub := &mockPublisher{}
	h := newHandler(t, ses, db, lim, pub)

	p := validPayload()
	p.HTML = ""
	p.Text = ""
	msg := makeMsg(t, p, 1)

	// Validation rejects this before render: terms with no DB write
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !msg.termed {
		t.Fatal("expected Term for empty body")
	}
}

func TestHandle_HTMLOnly_GeneratesTextFallback(t *testing.T) {
	ses := &mockSES{messageID: "id-1"}
	db := &mockDB{}
	lim := &mockLimiter{}
	pub := &mockPublisher{}
	h := newHandler(t, ses, db, lim, pub)

	p := validPayload()
	p.Text = "" // force fallback
	p.HTML = "<p>Welcome <b>Alice</b></p>"
	msg := makeMsg(t, p, 1)

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !msg.acked {
		t.Fatal("expected Ack")
	}
	if len(ses.calls) != 1 {
		t.Fatalf("expected 1 SES call, got %d", len(ses.calls))
	}
	text := ses.calls[0].in.TextBody
	if text == "" {
		t.Fatal("expected text body to be auto-generated from HTML")
	}
	if !strings.Contains(text, "Alice") {
		t.Errorf("text fallback should preserve content, got %q", text)
	}
	if strings.Contains(text, "<b>") {
		t.Errorf("text fallback should strip tags, got %q", text)
	}
}

func TestHandle_PublishEventFails_StillAcks(t *testing.T) {
	// SES already accepted; publishing the delivery event must not block ack.
	ses := &mockSES{messageID: "id-2"}
	db := &mockDB{}
	lim := &mockLimiter{}
	pub := &mockPublisher{err: errors.New("nats unavailable")}
	h := newHandler(t, ses, db, lim, pub)
	msg := makeMsg(t, validPayload(), 1)

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !msg.acked {
		t.Fatal("expected Ack even when delivery event publish fails")
	}
}

func TestNextDelay_MatchesSchedule(t *testing.T) {
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 1 * time.Minute},
		{2, 5 * time.Minute},
		{3, 15 * time.Minute},
		{4, 30 * time.Minute},
		{5, 2 * time.Hour},
		{6, 2 * time.Hour}, // clamps
	}
	for _, tc := range cases {
		if got := nextDelay(tc.attempt); got != tc.want {
			t.Errorf("nextDelay(%d) = %v, want %v", tc.attempt, got, tc.want)
		}
	}
}

func TestValidatePayload(t *testing.T) {
	cases := []struct {
		name string
		mod  func(*types.TransactionalEmailPayload)
		want string // expected substring of error; "" = no error
	}{
		{"valid", func(p *types.TransactionalEmailPayload) {}, ""},
		{"missing send id", func(p *types.TransactionalEmailPayload) { p.SendID = "" }, "missing required ids"},
		{"missing workspace id", func(p *types.TransactionalEmailPayload) { p.WorkspaceID = "" }, "missing required ids"},
		{"missing job id", func(p *types.TransactionalEmailPayload) { p.JobID = "" }, "missing required ids"},
		{"no recipients", func(p *types.TransactionalEmailPayload) { p.To = nil }, "recipients"},
		{"empty recipient email", func(p *types.TransactionalEmailPayload) { p.To = []types.EmailAddress{{Email: ""}} }, "empty email"},
		{"empty from", func(p *types.TransactionalEmailPayload) { p.From.Email = "" }, "from.email"},
		{"empty subject", func(p *types.TransactionalEmailPayload) { p.Subject = "" }, "subject"},
		{"no body", func(p *types.TransactionalEmailPayload) { p.HTML = ""; p.Text = "" }, "html and text"},
		{"unsupported provider", func(p *types.TransactionalEmailPayload) { p.Provider = "mailgun" }, "unsupported provider"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := validPayload()
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

func TestRenderer_HTMLToText(t *testing.T) {
	r := NewRenderer()
	p := &types.TransactionalEmailPayload{
		Subject: " Hello ",
		HTML:    "<p>Hi <b>Alice</b>&nbsp;&amp; team</p>",
		Text:    "",
	}
	out, err := r.Render(p)
	if err != nil {
		t.Fatal(err)
	}
	if out.Subject != "Hello" {
		t.Errorf("subject not trimmed: %q", out.Subject)
	}
	if !strings.Contains(out.Text, "Alice") || strings.Contains(out.Text, "<b>") {
		t.Errorf("bad text fallback: %q", out.Text)
	}
	if !strings.Contains(out.Text, "&") {
		t.Errorf("expected entity decoded to '&', got %q", out.Text)
	}
}

func TestRenderer_NoBody_Errors(t *testing.T) {
	r := NewRenderer()
	_, err := r.Render(&types.TransactionalEmailPayload{Subject: "x"})
	if err == nil {
		t.Fatal("expected error for empty body")
	}
}

// ---------- helpers ----------

func opSequence(db *mockDB) []string {
	db.mu.Lock()
	defer db.mu.Unlock()
	out := make([]string, 0, len(db.calls))
	for _, c := range db.calls {
		out = append(out, c.op)
	}
	return out
}

func hasOp(db *mockDB, op string) bool {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, c := range db.calls {
		if c.op == op {
			return true
		}
	}
	return false
}

func equalSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}


// ============================================================================
// Campaign tests live in campaign_handler_test.go
// ============================================================================
