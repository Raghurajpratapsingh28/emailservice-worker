package workflows

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

	"engageiq-workers/internal/infra/postgres"
	infraRedis "engageiq-workers/internal/infra/redis"
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

type mockDB struct {
	mu sync.Mutex

	workflow       *postgres.WorkflowRow
	execution      *postgres.ExecutionRow
	contact        *postgres.ContactRow
	createID       string
	createCreated  bool
	dueExecutions  []postgres.ExecutionRow

	getWorkflowErr  error
	getExecErr      error
	createExecErr   error
	advanceErr      error
	scheduleErr     error
	completeErr     error
	failErr         error
	getContactErr   error

	calls []dbOp
}

func (m *mockDB) GetWorkflow(_ context.Context, workflowID, _ string) (*postgres.WorkflowRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "get_workflow", id: workflowID})
	return m.workflow, m.getWorkflowErr
}

func (m *mockDB) GetExecution(_ context.Context, execID string) (*postgres.ExecutionRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "get_execution", id: execID})
	return m.execution, m.getExecErr
}

func (m *mockDB) CreateExecution(_ context.Context, workflowID, _, contactID, eventID string) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "create_execution", id: workflowID, value: contactID})
	return m.createID, m.createCreated, m.createExecErr
}

func (m *mockDB) AdvanceExecution(_ context.Context, execID, nodeID, status string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "advance", id: execID, value: nodeID + ":" + status})
	return m.advanceErr
}

func (m *mockDB) ScheduleDelay(_ context.Context, execID, nodeID string, _ time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "schedule_delay", id: execID, value: nodeID})
	return m.scheduleErr
}

func (m *mockDB) CompleteExecution(_ context.Context, execID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "complete", id: execID})
	return m.completeErr
}

func (m *mockDB) FailExecution(_ context.Context, execID, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "fail", id: execID, value: reason})
	return m.failErr
}

func (m *mockDB) IncrementExecutionRetry(_ context.Context, execID string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "increment_retry", id: execID})
	return 1, nil
}

func (m *mockDB) FetchDueExecutions(_ context.Context, _ int) ([]postgres.ExecutionRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "fetch_due"})
	return m.dueExecutions, nil
}

func (m *mockDB) GetContactForWorkflow(_ context.Context, contactID, _ string) (*postgres.ContactRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "get_contact", id: contactID})
	return m.contact, m.getContactErr
}

func (m *mockDB) hasCall(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.calls {
		if c.name == name {
			return true
		}
	}
	return false
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

func (m *mockPublisher) emailCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, msg := range m.msgs {
		if msg.subject == "email.send.transactional" {
			n++
		}
	}
	return n
}

type mockLocker struct {
	mu       sync.Mutex
	locked   map[string]bool
	acquireErr error
}

func (m *mockLocker) AcquireLock(_ context.Context, key, _ string, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.acquireErr != nil {
		return m.acquireErr
	}
	if m.locked[key] {
		return infraRedis.ErrLockNotAcquired
	}
	if m.locked == nil {
		m.locked = map[string]bool{}
	}
	m.locked[key] = true
	return nil
}

func (m *mockLocker) ReleaseLock(_ context.Context, key, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.locked, key)
	return nil
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
func (m *mockMsg) Subject() string                    { return SubjectWorkflowTrigger }
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

func newTriggerHandler(db WorkflowDB, pub EmailPublisher, locker DistributedLocker) *TriggerHandler {
	logger, _ := zap.NewDevelopment()
	return NewTriggerHandler(db, pub, locker, 5, logger)
}

func makeMsg(t *testing.T, p types.WorkflowTriggerPayload, attempt uint64) *mockMsg {
	t.Helper()
	data, _ := json.Marshal(p)
	return &mockMsg{data: data, numDelivered: attempt}
}

func validTrigger() types.WorkflowTriggerPayload {
	return types.WorkflowTriggerPayload{
		WorkspaceID: "ws-001",
		WorkflowID:  "wf-001",
		ContactID:   "contact-001",
		EventName:   "Trial Started",
		EventID:     "evt-001",
	}
}

// nodes builds a simple trigger → email → end workflow.
func triggerEmailEndNodes() []byte {
	nodes := []postgres.WorkflowNode{
		{ID: "n1", Type: NodeTypeTrigger, NextNode: "n2"},
		{ID: "n2", Type: NodeTypeEmail, NextNode: "n3", Config: map[string]interface{}{
			"subject":   "Welcome",
			"html":      "<h1>Hi</h1>",
			"fromEmail": "hello@acme.com",
		}},
		{ID: "n3", Type: NodeTypeEnd},
	}
	data, _ := json.Marshal(nodes)
	return data
}

// nodes builds a trigger → delay → email → end workflow.
func triggerDelayEmailEndNodes() []byte {
	nodes := []postgres.WorkflowNode{
		{ID: "n1", Type: NodeTypeTrigger, NextNode: "n2"},
		{ID: "n2", Type: NodeTypeDelay, NextNode: "n3", Config: map[string]interface{}{"seconds": float64(3600)}},
		{ID: "n3", Type: NodeTypeEmail, NextNode: "n4", Config: map[string]interface{}{
			"subject":   "Follow-up",
			"html":      "<p>Hi</p>",
			"fromEmail": "hello@acme.com",
		}},
		{ID: "n4", Type: NodeTypeEnd},
	}
	data, _ := json.Marshal(nodes)
	return data
}

func publishedWorkflow(nodes []byte) *postgres.WorkflowRow {
	return &postgres.WorkflowRow{
		ID:          "wf-001",
		WorkspaceID: "ws-001",
		Status:      "published",
		Nodes:       nodes,
	}
}

// ============================================================================
// TriggerHandler tests
// ============================================================================

func TestTriggerHandler_TriggerEmailEnd_AcksAndSendsEmail(t *testing.T) {
	db := &mockDB{
		workflow:      publishedWorkflow(triggerEmailEndNodes()),
		createID:      "exec-001",
		createCreated: true,
		contact:       &postgres.ContactRow{ID: "contact-001", Email: "alice@example.com", Name: "Alice"},
	}
	pub := &mockPublisher{}
	locker := &mockLocker{}
	h := newTriggerHandler(db, pub, locker)

	msg := makeMsg(t, validTrigger(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !msg.acked {
		t.Fatal("expected Ack")
	}
	if pub.emailCount() != 1 {
		t.Errorf("expected 1 email published, got %d", pub.emailCount())
	}
	if !db.hasCall("complete") {
		t.Fatal("expected CompleteExecution call")
	}
}

func TestTriggerHandler_TriggerDelayEnd_PausesAtDelay(t *testing.T) {
	db := &mockDB{
		workflow:      publishedWorkflow(triggerDelayEmailEndNodes()),
		createID:      "exec-001",
		createCreated: true,
		contact:       &postgres.ContactRow{ID: "contact-001", Email: "alice@example.com"},
	}
	pub := &mockPublisher{}
	locker := &mockLocker{}
	h := newTriggerHandler(db, pub, locker)

	msg := makeMsg(t, validTrigger(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.acked {
		t.Fatal("expected Ack")
	}
	if !db.hasCall("schedule_delay") {
		t.Fatal("expected ScheduleDelay call")
	}
	// Email should NOT be sent yet (paused at delay)
	if pub.emailCount() != 0 {
		t.Errorf("expected 0 emails before delay, got %d", pub.emailCount())
	}
}

func TestTriggerHandler_DuplicateExecution_AcksWithoutProcessing(t *testing.T) {
	db := &mockDB{
		workflow:      publishedWorkflow(triggerEmailEndNodes()),
		createID:      "",
		createCreated: false, // conflict — already exists
	}
	pub := &mockPublisher{}
	locker := &mockLocker{}
	h := newTriggerHandler(db, pub, locker)

	msg := makeMsg(t, validTrigger(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.acked {
		t.Fatal("expected Ack on duplicate")
	}
	if pub.emailCount() != 0 {
		t.Fatal("should not send email for duplicate execution")
	}
}

func TestTriggerHandler_WorkflowNotPublished_AcksWithoutExecution(t *testing.T) {
	db := &mockDB{
		workflow: &postgres.WorkflowRow{
			ID: "wf-001", WorkspaceID: "ws-001", Status: "draft", Nodes: triggerEmailEndNodes(),
		},
	}
	pub := &mockPublisher{}
	locker := &mockLocker{}
	h := newTriggerHandler(db, pub, locker)

	msg := makeMsg(t, validTrigger(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.acked {
		t.Fatal("expected Ack for unpublished workflow")
	}
	if db.hasCall("create_execution") {
		t.Fatal("should not create execution for unpublished workflow")
	}
}

func TestTriggerHandler_WorkflowNotFound_Terms(t *testing.T) {
	db := &mockDB{workflow: nil}
	pub := &mockPublisher{}
	locker := &mockLocker{}
	h := newTriggerHandler(db, pub, locker)

	msg := makeMsg(t, validTrigger(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.termed {
		t.Fatal("expected Term when workflow not found")
	}
}

func TestTriggerHandler_MalformedPayload_Terms(t *testing.T) {
	db := &mockDB{}
	pub := &mockPublisher{}
	locker := &mockLocker{}
	h := newTriggerHandler(db, pub, locker)

	msg := &mockMsg{data: []byte("not json")}
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.termed {
		t.Fatal("expected Term for malformed payload")
	}
}

func TestTriggerHandler_InvalidPayload_MissingWorkflowID_Terms(t *testing.T) {
	db := &mockDB{}
	pub := &mockPublisher{}
	locker := &mockLocker{}
	h := newTriggerHandler(db, pub, locker)

	p := validTrigger()
	p.WorkflowID = ""
	msg := makeMsg(t, p, 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.termed {
		t.Fatal("expected Term for missing workflowId")
	}
}

func TestTriggerHandler_LockAlreadyHeld_AcksWithoutExecution(t *testing.T) {
	db := &mockDB{
		workflow:      publishedWorkflow(triggerEmailEndNodes()),
		createID:      "exec-001",
		createCreated: true,
	}
	pub := &mockPublisher{}
	// Pre-lock the execution key
	locker := &mockLocker{locked: map[string]bool{"workflow:execution:exec-001": true}}
	h := newTriggerHandler(db, pub, locker)

	msg := makeMsg(t, validTrigger(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.acked {
		t.Fatal("expected Ack when execution lock already held")
	}
	if pub.emailCount() != 0 {
		t.Fatal("should not send email when lock held")
	}
}

func TestTriggerHandler_DBError_Naks(t *testing.T) {
	db := &mockDB{getWorkflowErr: errors.New("db down")}
	pub := &mockPublisher{}
	locker := &mockLocker{}
	h := newTriggerHandler(db, pub, locker)

	msg := makeMsg(t, validTrigger(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.naked {
		t.Fatal("expected Nak on DB error")
	}
}

func TestTriggerHandler_DBError_AtMaxRetries_Terms(t *testing.T) {
	db := &mockDB{getWorkflowErr: errors.New("db down")}
	pub := &mockPublisher{}
	locker := &mockLocker{}
	h := newTriggerHandler(db, pub, locker)

	msg := makeMsg(t, validTrigger(), 5)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.termed {
		t.Fatal("expected Term at max retries")
	}
}

// ============================================================================
// Executor unit tests
// ============================================================================

func newExecutor(db WorkflowDB, pub EmailPublisher) *Executor {
	logger, _ := zap.NewDevelopment()
	return NewExecutor(db, pub, logger)
}

func TestExecutor_TriggerEmailEnd_Completes(t *testing.T) {
	db := &mockDB{
		contact: &postgres.ContactRow{Email: "alice@example.com", Name: "Alice"},
	}
	pub := &mockPublisher{}
	e := newExecutor(db, pub)

	nodes, _ := ParseWorkflowNodes(triggerEmailEndNodes())
	exec := &postgres.ExecutionRow{
		ID: "exec-1", WorkspaceID: "ws-1", WorkflowID: "wf-1", ContactID: "c-1",
	}

	status, err := e.Run(context.Background(), exec, nodes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != StatusCompleted {
		t.Errorf("expected completed, got %s", status)
	}
	if pub.emailCount() != 1 {
		t.Errorf("expected 1 email, got %d", pub.emailCount())
	}
	if !db.hasCall("complete") {
		t.Fatal("expected CompleteExecution")
	}
}

func TestExecutor_TriggerDelayEmailEnd_PausesAtDelay(t *testing.T) {
	db := &mockDB{
		contact: &postgres.ContactRow{Email: "alice@example.com"},
	}
	pub := &mockPublisher{}
	e := newExecutor(db, pub)

	nodes, _ := ParseWorkflowNodes(triggerDelayEmailEndNodes())
	exec := &postgres.ExecutionRow{
		ID: "exec-1", WorkspaceID: "ws-1", WorkflowID: "wf-1", ContactID: "c-1",
	}

	status, err := e.Run(context.Background(), exec, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if status != StatusWaiting {
		t.Errorf("expected waiting, got %s", status)
	}
	if !db.hasCall("schedule_delay") {
		t.Fatal("expected ScheduleDelay")
	}
	if pub.emailCount() != 0 {
		t.Errorf("expected 0 emails before delay, got %d", pub.emailCount())
	}
}

func TestExecutor_ResumeFromDelay_SendsEmailAndCompletes(t *testing.T) {
	db := &mockDB{
		contact: &postgres.ContactRow{Email: "alice@example.com"},
	}
	pub := &mockPublisher{}
	e := newExecutor(db, pub)

	nodes, _ := ParseWorkflowNodes(triggerDelayEmailEndNodes())
	// Execution is paused at delay node n2
	exec := &postgres.ExecutionRow{
		ID: "exec-1", WorkspaceID: "ws-1", WorkflowID: "wf-1", ContactID: "c-1",
		Status: StatusWaiting, CurrentNodeID: "n2",
	}

	status, err := e.ResumeFromDelay(context.Background(), exec, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if status != StatusCompleted {
		t.Errorf("expected completed after resume, got %s", status)
	}
	if pub.emailCount() != 1 {
		t.Errorf("expected 1 email after resume, got %d", pub.emailCount())
	}
	if !db.hasCall("complete") {
		t.Fatal("expected CompleteExecution after resume")
	}
}

func TestExecutor_EmailNode_NoContact_SkipsEmail(t *testing.T) {
	db := &mockDB{contact: nil} // no contact found
	pub := &mockPublisher{}
	e := newExecutor(db, pub)

	nodes, _ := ParseWorkflowNodes(triggerEmailEndNodes())
	exec := &postgres.ExecutionRow{
		ID: "exec-1", WorkspaceID: "ws-1", WorkflowID: "wf-1", ContactID: "c-1",
	}

	status, err := e.Run(context.Background(), exec, nodes)
	if err != nil {
		t.Fatal(err)
	}
	// Should still complete — missing email is a soft skip
	if status != StatusCompleted {
		t.Errorf("expected completed even with no contact, got %s", status)
	}
	if pub.emailCount() != 0 {
		t.Errorf("expected 0 emails for contact with no email, got %d", pub.emailCount())
	}
}

func TestExecutor_UnknownNodeType_ReturnsFailed(t *testing.T) {
	db := &mockDB{}
	pub := &mockPublisher{}
	e := newExecutor(db, pub)

	nodes := []postgres.WorkflowNode{
		{ID: "n1", Type: "unknown_node"},
	}
	exec := &postgres.ExecutionRow{ID: "exec-1", WorkspaceID: "ws-1", WorkflowID: "wf-1", ContactID: "c-1"}

	status, err := e.Run(context.Background(), exec, nodes)
	if err == nil {
		t.Fatal("expected error for unknown node type")
	}
	if status != StatusFailed {
		t.Errorf("expected failed, got %s", status)
	}
}

// ============================================================================
// ParseWorkflowNodes tests
// ============================================================================

func TestParseWorkflowNodes_Valid(t *testing.T) {
	nodes, err := ParseWorkflowNodes(triggerEmailEndNodes())
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(nodes))
	}
	if nodes[0].Type != NodeTypeTrigger {
		t.Errorf("expected trigger, got %s", nodes[0].Type)
	}
}

func TestParseWorkflowNodes_Empty(t *testing.T) {
	nodes, err := ParseWorkflowNodes([]byte("[]"))
	if err != nil {
		t.Fatal(err)
	}
	if nodes != nil {
		t.Errorf("expected nil for empty array, got %v", nodes)
	}
}

func TestParseWorkflowNodes_Invalid(t *testing.T) {
	_, err := ParseWorkflowNodes([]byte("not json"))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

// ============================================================================
// parseDuration tests
// ============================================================================

func TestParseDuration_Seconds(t *testing.T) {
	d, err := parseDuration(map[string]interface{}{"seconds": float64(3600)})
	if err != nil {
		t.Fatal(err)
	}
	if d != time.Hour {
		t.Errorf("expected 1h, got %v", d)
	}
}

func TestParseDuration_DurationString(t *testing.T) {
	d, err := parseDuration(map[string]interface{}{"duration": "24h"})
	if err != nil {
		t.Fatal(err)
	}
	if d != 24*time.Hour {
		t.Errorf("expected 24h, got %v", d)
	}
}

func TestParseDuration_Missing_Error(t *testing.T) {
	_, err := parseDuration(map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for missing duration config")
	}
}
