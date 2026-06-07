package segments

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
	count int
}

type mockSegmentDB struct {
	mu sync.Mutex

	segment        *postgres.SegmentRow
	currentMembers map[string]struct{}
	contacts       []postgres.ContactForEval
	eventResults   map[string]bool // contactID → hasPerformed

	getSegmentErr    error
	updateStatusErr  error
	getMembersErr    error
	insertErr        error
	deleteErr        error
	streamErr        error

	calls []dbOp
}

func (m *mockSegmentDB) GetSegment(_ context.Context, segmentID, _ string) (*postgres.SegmentRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "get_segment", id: segmentID})
	return m.segment, m.getSegmentErr
}

func (m *mockSegmentDB) UpdateSegmentStatus(_ context.Context, segmentID, _, status string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "update_status", id: segmentID, value: status})
	return m.updateStatusErr
}

func (m *mockSegmentDB) UpdateSegmentReady(_ context.Context, segmentID, _ string, count int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "update_ready", id: segmentID, count: count})
	return nil
}

func (m *mockSegmentDB) GetCurrentMemberIDs(_ context.Context, _, _ string) (map[string]struct{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "get_members"})
	if m.getMembersErr != nil {
		return nil, m.getMembersErr
	}
	if m.currentMembers == nil {
		return map[string]struct{}{}, nil
	}
	return m.currentMembers, nil
}

func (m *mockSegmentDB) InsertSegmentMembers(_ context.Context, _, _ string, ids []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "insert_members", count: len(ids)})
	return m.insertErr
}

func (m *mockSegmentDB) DeleteSegmentMembers(_ context.Context, _, _ string, ids []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, dbOp{name: "delete_members", count: len(ids)})
	return m.deleteErr
}

func (m *mockSegmentDB) StreamContactsForEval(_ context.Context, _ string, _ int, fn func([]postgres.ContactForEval) error) error {
	m.mu.Lock()
	contacts := m.contacts
	m.mu.Unlock()
	if m.streamErr != nil {
		return m.streamErr
	}
	if len(contacts) == 0 {
		return nil
	}
	return fn(contacts)
}

func (m *mockSegmentDB) ContactHasPerformedEvent(_ context.Context, _, contactID, _ string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.eventResults[contactID], nil
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

func (m *mockSegmentDB) callWithValue(name, value string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.calls {
		if c.name == name && c.value == value {
			return true
		}
	}
	return false
}

func (m *mockSegmentDB) insertCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.calls {
		if c.name == "insert_members" {
			return c.count
		}
	}
	return 0
}

func (m *mockSegmentDB) deleteCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.calls {
		if c.name == "delete_members" {
			return c.count
		}
	}
	return 0
}

// mockLocker implements DistributedLocker.
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
func (m *mockMsg) Subject() string                    { return SubjectSegmentRefresh }
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

func newHandler(db SegmentDB, locker DistributedLocker, maxRetries int) *Handler {
	logger, _ := zap.NewDevelopment()
	return NewHandler(db, locker, maxRetries, logger)
}

func makeMsg(t *testing.T, p types.SegmentRefreshPayload, attempt uint64) *mockMsg {
	t.Helper()
	data, _ := json.Marshal(p)
	return &mockMsg{data: data, numDelivered: attempt}
}

func validPayload() types.SegmentRefreshPayload {
	return types.SegmentRefreshPayload{
		WorkspaceID: "ws-001",
		SegmentID:   "seg-001",
	}
}

func segmentWithFilter(filterJSON string) *postgres.SegmentRow {
	return &postgres.SegmentRow{
		ID:          "seg-001",
		WorkspaceID: "ws-001",
		Name:        "Test Segment",
		FilterTree:  []byte(filterJSON),
		Status:      "queued",
	}
}

func contact(id, email string) postgres.ContactForEval {
	return postgres.ContactForEval{
		ID:          id,
		WorkspaceID: "ws-001",
		Email:       email,
		FirstName:   "Test",
		Properties:  []byte(`{}`),
		CreatedAt:   time.Now(),
	}
}

// ============================================================================
// Handler tests
// ============================================================================

func TestHandle_SimpleFilter_MatchesAndInsertsMembers(t *testing.T) {
	db := &mockSegmentDB{
		segment: segmentWithFilter(`{"field":"email","operator":"contains","value":"@acme.com"}`),
		contacts: []postgres.ContactForEval{
			contact("c1", "alice@acme.com"),
			contact("c2", "bob@other.com"),
		},
	}
	locker := &mockLocker{}
	h := newHandler(db, locker, 5)

	msg := makeMsg(t, validPayload(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !msg.acked {
		t.Fatal("expected Ack")
	}
	if db.insertCount() != 1 {
		t.Errorf("expected 1 insert (alice), got %d", db.insertCount())
	}
	if db.deleteCount() != 0 {
		t.Errorf("expected 0 deletes, got %d", db.deleteCount())
	}
	if !db.callWithValue("update_status", "processing") {
		t.Fatal("expected update_status=processing")
	}
	if !db.hasCall("update_ready") {
		t.Fatal("expected update_ready call")
	}
}

func TestHandle_EmptySegment_NoContacts_Completes(t *testing.T) {
	db := &mockSegmentDB{
		segment:  segmentWithFilter(`{}`),
		contacts: nil, // no contacts
	}
	locker := &mockLocker{}
	h := newHandler(db, locker, 5)

	msg := makeMsg(t, validPayload(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.acked {
		t.Fatal("expected Ack on empty segment")
	}
	if db.insertCount() != 0 {
		t.Errorf("expected 0 inserts, got %d", db.insertCount())
	}
}

func TestHandle_StaleMembers_Removed(t *testing.T) {
	db := &mockSegmentDB{
		segment: segmentWithFilter(`{"field":"email","operator":"contains","value":"@acme.com"}`),
		contacts: []postgres.ContactForEval{
			contact("c1", "alice@acme.com"),
		},
		// c2 was a member but no longer matches
		currentMembers: map[string]struct{}{"c2": {}},
	}
	locker := &mockLocker{}
	h := newHandler(db, locker, 5)

	msg := makeMsg(t, validPayload(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.acked {
		t.Fatal("expected Ack")
	}
	if db.insertCount() != 1 {
		t.Errorf("expected 1 insert (c1), got %d", db.insertCount())
	}
	if db.deleteCount() != 1 {
		t.Errorf("expected 1 delete (c2), got %d", db.deleteCount())
	}
}

func TestHandle_DuplicateRefresh_LockAlreadyHeld_Acks(t *testing.T) {
	db := &mockSegmentDB{
		segment: segmentWithFilter(`{}`),
	}
	// Pre-lock the key
	locker := &mockLocker{locked: map[string]bool{"segment:seg-001:refresh": true}}
	h := newHandler(db, locker, 5)

	msg := makeMsg(t, validPayload(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.acked {
		t.Fatal("expected Ack when lock already held (another worker is processing)")
	}
	if db.hasCall("get_segment") {
		t.Fatal("should not load segment when lock not acquired")
	}
}

func TestHandle_SegmentNotFound_Terms(t *testing.T) {
	db := &mockSegmentDB{segment: nil}
	locker := &mockLocker{}
	h := newHandler(db, locker, 5)

	msg := makeMsg(t, validPayload(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.termed {
		t.Fatal("expected Term when segment not found")
	}
}

func TestHandle_MalformedPayload_Terms(t *testing.T) {
	db := &mockSegmentDB{}
	locker := &mockLocker{}
	h := newHandler(db, locker, 5)

	msg := &mockMsg{data: []byte("not json")}
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.termed {
		t.Fatal("expected Term for malformed payload")
	}
}

func TestHandle_InvalidPayload_MissingSegmentID_Terms(t *testing.T) {
	db := &mockSegmentDB{}
	locker := &mockLocker{}
	h := newHandler(db, locker, 5)

	msg := makeMsg(t, types.SegmentRefreshPayload{WorkspaceID: "ws-001"}, 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.termed {
		t.Fatal("expected Term for missing segmentId")
	}
}

func TestHandle_DBError_Transient_Naks(t *testing.T) {
	db := &mockSegmentDB{
		segment:       segmentWithFilter(`{}`),
		getMembersErr: errors.New("db timeout"),
	}
	locker := &mockLocker{}
	h := newHandler(db, locker, 5)

	msg := makeMsg(t, validPayload(), 2)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.naked {
		t.Fatal("expected Nak on transient DB error")
	}
}

func TestHandle_DBError_AtMaxRetries_Terms(t *testing.T) {
	db := &mockSegmentDB{
		segment:       segmentWithFilter(`{}`),
		getMembersErr: errors.New("db timeout"),
	}
	locker := &mockLocker{}
	h := newHandler(db, locker, 5)

	msg := makeMsg(t, validPayload(), 5)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.termed {
		t.Fatal("expected Term at max retries")
	}
}

func TestHandle_LockAcquireError_Naks(t *testing.T) {
	db := &mockSegmentDB{}
	locker := &mockLocker{acquireErr: errors.New("redis down")}
	h := newHandler(db, locker, 5)

	msg := makeMsg(t, validPayload(), 1)
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !msg.naked {
		t.Fatal("expected Nak when lock acquire fails")
	}
}

// ============================================================================
// Evaluator tests
// ============================================================================

type mockEventChecker struct {
	results map[string]bool
}

func (m *mockEventChecker) ContactHasPerformedEvent(_ context.Context, _, contactID, _ string) (bool, error) {
	return m.results[contactID], nil
}

func newEval(events map[string]bool) *Evaluator {
	return NewEvaluator(&mockEventChecker{results: events})
}

func ct(id, email, firstName string, properties map[string]interface{}) postgres.ContactForEval {
	propsJSON, _ := json.Marshal(properties)
	if propsJSON == nil {
		propsJSON = []byte(`{}`)
	}
	return postgres.ContactForEval{
		ID:          id,
		WorkspaceID: "ws-001",
		Email:       email,
		FirstName:   firstName,
		Properties:  propsJSON,
		CreatedAt:   time.Now(),
	}
}

func TestEvaluator_Equals(t *testing.T) {
	e := newEval(nil)
	tree := &FilterNode{Field: "email", Operator: "equals", Value: "alice@acme.com"}
	c := ct("c1", "alice@acme.com", "Alice", nil)
	ok, err := e.Matches(context.Background(), "ws-001", &c, tree)
	if err != nil || !ok {
		t.Errorf("expected match: ok=%v err=%v", ok, err)
	}
	c2 := ct("c2", "bob@acme.com", "Bob", nil)
	ok2, _ := e.Matches(context.Background(), "ws-001", &c2, tree)
	if ok2 {
		t.Error("expected no match for different email")
	}
}

func TestEvaluator_NotEquals(t *testing.T) {
	e := newEval(nil)
	tree := &FilterNode{Field: "email", Operator: "not_equals", Value: "alice@acme.com"}
	c := ct("c1", "bob@acme.com", "Bob", nil)
	ok, _ := e.Matches(context.Background(), "ws-001", &c, tree)
	if !ok {
		t.Error("expected match for not_equals")
	}
}

func TestEvaluator_Contains(t *testing.T) {
	e := newEval(nil)
	tree := &FilterNode{Field: "email", Operator: "contains", Value: "@acme.com"}
	c := ct("c1", "alice@acme.com", "Alice", nil)
	ok, _ := e.Matches(context.Background(), "ws-001", &c, tree)
	if !ok {
		t.Error("expected match for contains")
	}
}

func TestEvaluator_StartsWith(t *testing.T) {
	e := newEval(nil)
	tree := &FilterNode{Field: "first_name", Operator: "starts_with", Value: "Al"}
	c := ct("c1", "alice@acme.com", "Alice", nil)
	ok, _ := e.Matches(context.Background(), "ws-001", &c, tree)
	if !ok {
		t.Error("expected match for starts_with")
	}
}

func TestEvaluator_EndsWith(t *testing.T) {
	e := newEval(nil)
	tree := &FilterNode{Field: "email", Operator: "ends_with", Value: ".com"}
	c := ct("c1", "alice@acme.com", "Alice", nil)
	ok, _ := e.Matches(context.Background(), "ws-001", &c, tree)
	if !ok {
		t.Error("expected match for ends_with")
	}
}

func TestEvaluator_GreaterThan(t *testing.T) {
	e := newEval(nil)
	tree := &FilterNode{Field: "properties.age", Operator: "greater_than", Value: float64(25)}
	c := ct("c1", "alice@acme.com", "Alice", map[string]interface{}{"age": float64(30)})
	ok, _ := e.Matches(context.Background(), "ws-001", &c, tree)
	if !ok {
		t.Error("expected match for greater_than")
	}
	c2 := ct("c2", "bob@acme.com", "Bob", map[string]interface{}{"age": float64(20)})
	ok2, _ := e.Matches(context.Background(), "ws-001", &c2, tree)
	if ok2 {
		t.Error("expected no match for age < 25")
	}
}

func TestEvaluator_LessThan(t *testing.T) {
	e := newEval(nil)
	tree := &FilterNode{Field: "properties.score", Operator: "less_than", Value: float64(50)}
	c := ct("c1", "a@b.com", "A", map[string]interface{}{"score": float64(30)})
	ok, _ := e.Matches(context.Background(), "ws-001", &c, tree)
	if !ok {
		t.Error("expected match for less_than")
	}
}

func TestEvaluator_Exists(t *testing.T) {
	e := newEval(nil)
	tree := &FilterNode{Field: "properties.plan", Operator: "exists"}
	c := ct("c1", "a@b.com", "A", map[string]interface{}{"plan": "pro"})
	ok, _ := e.Matches(context.Background(), "ws-001", &c, tree)
	if !ok {
		t.Error("expected match for exists")
	}
	c2 := ct("c2", "b@b.com", "B", nil)
	ok2, _ := e.Matches(context.Background(), "ws-001", &c2, tree)
	if ok2 {
		t.Error("expected no match when field absent")
	}
}

func TestEvaluator_NotExists(t *testing.T) {
	e := newEval(nil)
	tree := &FilterNode{Field: "properties.plan", Operator: "not_exists"}
	c := ct("c1", "a@b.com", "A", nil)
	ok, _ := e.Matches(context.Background(), "ws-001", &c, tree)
	if !ok {
		t.Error("expected match for not_exists when field absent")
	}
}

func TestEvaluator_In(t *testing.T) {
	e := newEval(nil)
	tree := &FilterNode{Field: "properties.plan", Operator: "in", Value: []interface{}{"pro", "enterprise"}}
	c := ct("c1", "a@b.com", "A", map[string]interface{}{"plan": "pro"})
	ok, _ := e.Matches(context.Background(), "ws-001", &c, tree)
	if !ok {
		t.Error("expected match for in")
	}
	c2 := ct("c2", "b@b.com", "B", map[string]interface{}{"plan": "free"})
	ok2, _ := e.Matches(context.Background(), "ws-001", &c2, tree)
	if ok2 {
		t.Error("expected no match for plan=free")
	}
}

func TestEvaluator_NotIn(t *testing.T) {
	e := newEval(nil)
	tree := &FilterNode{Field: "properties.plan", Operator: "not_in", Value: []interface{}{"free"}}
	c := ct("c1", "a@b.com", "A", map[string]interface{}{"plan": "pro"})
	ok, _ := e.Matches(context.Background(), "ws-001", &c, tree)
	if !ok {
		t.Error("expected match for not_in")
	}
}

func TestEvaluator_AND_Logic(t *testing.T) {
	e := newEval(nil)
	tree := &FilterNode{
		Operator: "AND",
		Rules: []*FilterNode{
			{Field: "email", Operator: "contains", Value: "@acme.com"},
			{Field: "properties.plan", Operator: "equals", Value: "pro"},
		},
	}
	c := ct("c1", "alice@acme.com", "Alice", map[string]interface{}{"plan": "pro"})
	ok, _ := e.Matches(context.Background(), "ws-001", &c, tree)
	if !ok {
		t.Error("expected AND match")
	}
	c2 := ct("c2", "alice@acme.com", "Alice", map[string]interface{}{"plan": "free"})
	ok2, _ := e.Matches(context.Background(), "ws-001", &c2, tree)
	if ok2 {
		t.Error("expected AND no-match when plan=free")
	}
}

func TestEvaluator_OR_Logic(t *testing.T) {
	e := newEval(nil)
	tree := &FilterNode{
		Operator: "OR",
		Rules: []*FilterNode{
			{Field: "email", Operator: "contains", Value: "@acme.com"},
			{Field: "properties.plan", Operator: "equals", Value: "enterprise"},
		},
	}
	c := ct("c1", "bob@other.com", "Bob", map[string]interface{}{"plan": "enterprise"})
	ok, _ := e.Matches(context.Background(), "ws-001", &c, tree)
	if !ok {
		t.Error("expected OR match via plan=enterprise")
	}
}

func TestEvaluator_NestedLogic(t *testing.T) {
	e := newEval(nil)
	// (email contains @acme.com AND plan = pro) OR (plan = enterprise)
	tree := &FilterNode{
		Operator: "OR",
		Rules: []*FilterNode{
			{
				Operator: "AND",
				Rules: []*FilterNode{
					{Field: "email", Operator: "contains", Value: "@acme.com"},
					{Field: "properties.plan", Operator: "equals", Value: "pro"},
				},
			},
			{Field: "properties.plan", Operator: "equals", Value: "enterprise"},
		},
	}
	c1 := ct("c1", "alice@acme.com", "Alice", map[string]interface{}{"plan": "pro"})
	ok1, _ := e.Matches(context.Background(), "ws-001", &c1, tree)
	if !ok1 {
		t.Error("expected match via AND branch")
	}
	c2 := ct("c2", "bob@other.com", "Bob", map[string]interface{}{"plan": "enterprise"})
	ok2, _ := e.Matches(context.Background(), "ws-001", &c2, tree)
	if !ok2 {
		t.Error("expected match via enterprise branch")
	}
	c3 := ct("c3", "carol@other.com", "Carol", map[string]interface{}{"plan": "free"})
	ok3, _ := e.Matches(context.Background(), "ws-001", &c3, tree)
	if ok3 {
		t.Error("expected no match for free plan on other domain")
	}
}

func TestEvaluator_EventFilter(t *testing.T) {
	e := newEval(map[string]bool{"c1": true, "c2": false})
	tree := &FilterNode{Field: "event:Trial Started", Operator: "exists"}
	c1 := ct("c1", "a@b.com", "A", nil)
	ok1, _ := e.Matches(context.Background(), "ws-001", &c1, tree)
	if !ok1 {
		t.Error("expected match for contact that performed event")
	}
	c2 := ct("c2", "b@b.com", "B", nil)
	ok2, _ := e.Matches(context.Background(), "ws-001", &c2, tree)
	if ok2 {
		t.Error("expected no match for contact that did not perform event")
	}
}

func TestEvaluator_NilTree_MatchesAll(t *testing.T) {
	e := newEval(nil)
	c := ct("c1", "a@b.com", "A", nil)
	ok, err := e.Matches(context.Background(), "ws-001", &c, nil)
	if err != nil || !ok {
		t.Errorf("nil tree should match all: ok=%v err=%v", ok, err)
	}
}

func TestEvaluator_PropertiesNestedField(t *testing.T) {
	e := newEval(nil)
	tree := &FilterNode{Field: "properties.address.city", Operator: "equals", Value: "NYC"}
	propsJSON, _ := json.Marshal(map[string]interface{}{
		"address": map[string]interface{}{"city": "NYC"},
	})
	c := postgres.ContactForEval{
		ID: "c1", WorkspaceID: "ws-001", Email: "a@b.com",
		Properties: propsJSON, CreatedAt: time.Now(),
	}
	ok, _ := e.Matches(context.Background(), "ws-001", &c, tree)
	if !ok {
		t.Error("expected match for nested properties field")
	}
}

func TestDiff(t *testing.T) {
	a := map[string]struct{}{"1": {}, "2": {}, "3": {}}
	b := map[string]struct{}{"2": {}, "3": {}, "4": {}}
	result := diff(a, b)
	if len(result) != 1 || result[0] != "1" {
		t.Errorf("expected diff=[1], got %v", result)
	}
}
