package segments

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"engageiq-workers/internal/infra/postgres"
)

// FilterNode is a node in the filter tree. It is either a logical group
// (AND/OR with children) or a leaf condition.
type FilterNode struct {
	// Logical group fields
	Logic    string        `json:"logic,omitempty"` // "AND" | "OR"
	Children []*FilterNode `json:"children,omitempty"`

	// Leaf condition fields
	Field    string      `json:"field,omitempty"`
	Operator string      `json:"operator,omitempty"`
	Value    interface{} `json:"value,omitempty"`

	// Event filter (special leaf)
	EventName string `json:"eventName,omitempty"` // non-empty → event filter
}

// EventChecker checks whether a contact has performed a named event.
type EventChecker interface {
	ContactHasPerformedEvent(ctx context.Context, workspaceID, contactID, eventName string) (bool, error)
}

// Evaluator evaluates a filter tree against a contact.
type Evaluator struct {
	events EventChecker
}

func NewEvaluator(events EventChecker) *Evaluator {
	return &Evaluator{events: events}
}

// ParseFilterTree deserialises the JSONB filter tree from the segment row.
func ParseFilterTree(data []byte) (*FilterNode, error) {
	if len(data) == 0 || string(data) == "{}" || string(data) == "null" {
		return nil, nil
	}
	var node FilterNode
	if err := json.Unmarshal(data, &node); err != nil {
		return nil, fmt.Errorf("parse filter tree: %w", err)
	}
	return &node, nil
}

// Matches returns true if the contact satisfies the filter tree.
// A nil tree matches all contacts.
func (e *Evaluator) Matches(ctx context.Context, workspaceID string, ct *postgres.ContactForEval, tree *FilterNode) (bool, error) {
	if tree == nil {
		return true, nil
	}
	return e.eval(ctx, workspaceID, ct, tree)
}

func (e *Evaluator) eval(ctx context.Context, workspaceID string, ct *postgres.ContactForEval, node *FilterNode) (bool, error) {
	// Logical group
	if node.Logic != "" {
		return e.evalLogic(ctx, workspaceID, ct, node)
	}
	// Event filter leaf
	if node.EventName != "" {
		return e.events.ContactHasPerformedEvent(ctx, workspaceID, ct.ID, node.EventName)
	}
	// Condition leaf
	return e.evalCondition(ct, node)
}

func (e *Evaluator) evalLogic(ctx context.Context, workspaceID string, ct *postgres.ContactForEval, node *FilterNode) (bool, error) {
	if len(node.Children) == 0 {
		return true, nil
	}
	switch strings.ToUpper(node.Logic) {
	case "AND":
		for _, child := range node.Children {
			ok, err := e.eval(ctx, workspaceID, ct, child)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
		}
		return true, nil
	case "OR":
		for _, child := range node.Children {
			ok, err := e.eval(ctx, workspaceID, ct, child)
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}
		return false, nil
	default:
		return false, fmt.Errorf("unknown logic operator: %q", node.Logic)
	}
}

func (e *Evaluator) evalCondition(ct *postgres.ContactForEval, node *FilterNode) (bool, error) {
	fieldVal, err := extractField(ct, node.Field)
	if err != nil {
		return false, err
	}

	switch node.Operator {
	case "exists":
		return fieldVal != nil && fieldVal != "", nil
	case "not_exists":
		return fieldVal == nil || fieldVal == "", nil
	}

	if fieldVal == nil {
		return false, nil
	}

	switch node.Operator {
	case "equals":
		return compareEqual(fieldVal, node.Value), nil
	case "not_equals":
		return !compareEqual(fieldVal, node.Value), nil
	case "contains":
		return compareContains(fieldVal, node.Value), nil
	case "starts_with":
		return compareStartsWith(fieldVal, node.Value), nil
	case "ends_with":
		return compareEndsWith(fieldVal, node.Value), nil
	case "greater_than":
		return compareNumeric(fieldVal, node.Value, ">")
	case "less_than":
		return compareNumeric(fieldVal, node.Value, "<")
	case "in":
		return compareIn(fieldVal, node.Value), nil
	case "not_in":
		return !compareIn(fieldVal, node.Value), nil
	default:
		return false, fmt.Errorf("unknown operator: %q", node.Operator)
	}
}

// extractField resolves a dot-notation field path from the contact.
// Supports top-level fields (email, first_name, etc.) and traits.* / properties.*.
func extractField(ct *postgres.ContactForEval, field string) (interface{}, error) {
	switch field {
	case "email":
		return ct.Email, nil
	case "first_name", "firstName":
		return ct.FirstName, nil
	case "last_name", "lastName":
		return ct.LastName, nil
	case "phone":
		return ct.Phone, nil
	case "user_id", "userId":
		return ct.UserID, nil
	case "status":
		return ct.Status, nil
	case "created_at", "createdAt":
		return ct.CreatedAt.Format(time.RFC3339), nil
	}

	// traits.* or properties.*
	if strings.HasPrefix(field, "traits.") {
		return extractJSONBField(ct.Traits, strings.TrimPrefix(field, "traits."))
	}
	if strings.HasPrefix(field, "properties.") {
		return extractJSONBField(ct.Properties, strings.TrimPrefix(field, "properties."))
	}

	return nil, nil // unknown field → nil (not_exists will match)
}

func extractJSONBField(data []byte, key string) (interface{}, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, nil // malformed JSONB → treat as missing
	}
	// Support nested dot notation: traits.address.city
	parts := strings.SplitN(key, ".", 2)
	val, ok := m[parts[0]]
	if !ok {
		return nil, nil
	}
	if len(parts) == 2 {
		sub, ok := val.(map[string]interface{})
		if !ok {
			return nil, nil
		}
		return sub[parts[1]], nil
	}
	return val, nil
}

// --- comparison helpers ---

func compareEqual(a, b interface{}) bool {
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func compareContains(a, b interface{}) bool {
	return strings.Contains(strings.ToLower(fmt.Sprintf("%v", a)), strings.ToLower(fmt.Sprintf("%v", b)))
}

func compareStartsWith(a, b interface{}) bool {
	return strings.HasPrefix(strings.ToLower(fmt.Sprintf("%v", a)), strings.ToLower(fmt.Sprintf("%v", b)))
}

func compareEndsWith(a, b interface{}) bool {
	return strings.HasSuffix(strings.ToLower(fmt.Sprintf("%v", a)), strings.ToLower(fmt.Sprintf("%v", b)))
}

func compareNumeric(a, b interface{}, op string) (bool, error) {
	fa, err := toFloat(a)
	if err != nil {
		return false, nil // non-numeric field → no match
	}
	fb, err := toFloat(b)
	if err != nil {
		return false, fmt.Errorf("numeric comparison: invalid value %v", b)
	}
	switch op {
	case ">":
		return fa > fb, nil
	case "<":
		return fa < fb, nil
	}
	return false, nil
}

func compareIn(a, b interface{}) bool {
	// b should be a []interface{} or []string
	rv := reflect.ValueOf(b)
	if rv.Kind() != reflect.Slice {
		return compareEqual(a, b)
	}
	aStr := fmt.Sprintf("%v", a)
	for i := 0; i < rv.Len(); i++ {
		if fmt.Sprintf("%v", rv.Index(i).Interface()) == aStr {
			return true
		}
	}
	return false
}

func toFloat(v interface{}) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case int:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case json.Number:
		return x.Float64()
	case string:
		return strconv.ParseFloat(x, 64)
	}
	return 0, fmt.Errorf("cannot convert %T to float64", v)
}
