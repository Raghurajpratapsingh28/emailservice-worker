package workflows

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"engageiq-workers/internal/infra/postgres"
	"engageiq-workers/pkg/types"
)

// NodeType constants for the MVP workflow.
const (
	NodeTypeTrigger = "trigger"
	NodeTypeEmail   = "email"
	NodeTypeDelay   = "delay"
	NodeTypeEnd     = "end"
)

// ExecutionStatus values.
const (
	StatusRunning   = "running"
	StatusWaiting   = "waiting"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
)

// EmailPublisher publishes transactional email jobs.
type EmailPublisher interface {
	Publish(ctx context.Context, subject string, payload any, msgID string) error
}

// Executor runs a workflow execution from the current node to the next pause point.
type Executor struct {
	db     WorkflowDB
	pub    EmailPublisher
	logger *zap.Logger
}

func NewExecutor(db WorkflowDB, pub EmailPublisher, logger *zap.Logger) *Executor {
	return &Executor{db: db, pub: pub, logger: logger}
}

// Run advances the execution from its current node until it hits a delay or end.
// Returns the final status after this run.
func (e *Executor) Run(ctx context.Context, exec *postgres.ExecutionRow, nodes []postgres.WorkflowNode) (string, error) {
	nodeMap := buildNodeMap(nodes)

	// Find starting node: if current_node_id is set, start from there; otherwise start from first node.
	currentID := exec.CurrentNodeID
	if currentID == "" && len(nodes) > 0 {
		currentID = nodes[0].ID
	}

	log := e.logger.With(
		zap.String("execution_id", exec.ID),
		zap.String("workflow_id", exec.WorkflowID),
		zap.String("contact_id", exec.ContactID),
	)

	for {
		node, ok := nodeMap[currentID]
		if !ok {
			return StatusFailed, fmt.Errorf("node %q not found in workflow", currentID)
		}

		log.Info("node executed", zap.String("node_id", node.ID), zap.String("node_type", node.Type))

		switch node.Type {
		case NodeTypeTrigger:
			// Advance immediately to next node.
			if node.NextNode == "" {
				if err := e.db.CompleteExecution(ctx, exec.ID); err != nil {
					return StatusFailed, fmt.Errorf("complete after trigger: %w", err)
				}
				return StatusCompleted, nil
			}
			if err := e.db.AdvanceExecution(ctx, exec.ID, node.NextNode, StatusRunning); err != nil {
				return StatusFailed, fmt.Errorf("advance from trigger: %w", err)
			}
			currentID = node.NextNode

		case NodeTypeEmail:
			if err := e.executeEmail(ctx, exec, node, log); err != nil {
				return StatusFailed, fmt.Errorf("email node %s: %w", node.ID, err)
			}
			if node.NextNode == "" {
				if err := e.db.CompleteExecution(ctx, exec.ID); err != nil {
					return StatusFailed, fmt.Errorf("complete after email: %w", err)
				}
				return StatusCompleted, nil
			}
			if err := e.db.AdvanceExecution(ctx, exec.ID, node.NextNode, StatusRunning); err != nil {
				return StatusFailed, fmt.Errorf("advance from email: %w", err)
			}
			currentID = node.NextNode

		case NodeTypeDelay:
			delay, err := parseDuration(node.Config)
			if err != nil {
				return StatusFailed, fmt.Errorf("delay node %s: %w", node.ID, err)
			}
			nextRunAt := time.Now().Add(delay)
			if err := e.db.ScheduleDelay(ctx, exec.ID, node.ID, nextRunAt); err != nil {
				return StatusFailed, fmt.Errorf("schedule delay: %w", err)
			}
			log.Info("delay scheduled",
				zap.String("node_id", node.ID),
				zap.Duration("delay", delay),
				zap.Time("next_run_at", nextRunAt),
			)
			return StatusWaiting, nil

		case NodeTypeEnd:
			if err := e.db.CompleteExecution(ctx, exec.ID); err != nil {
				return StatusFailed, fmt.Errorf("complete at end node: %w", err)
			}
			log.Info("workflow completed", zap.String("execution_id", exec.ID))
			return StatusCompleted, nil

		default:
			return StatusFailed, fmt.Errorf("unsupported node type %q", node.Type)
		}
	}
}

// ResumeFromDelay resumes an execution that was paused at a delay node.
// It advances past the delay node and continues execution.
func (e *Executor) ResumeFromDelay(ctx context.Context, exec *postgres.ExecutionRow, nodes []postgres.WorkflowNode) (string, error) {
	nodeMap := buildNodeMap(nodes)
	delayNode, ok := nodeMap[exec.CurrentNodeID]
	if !ok {
		return StatusFailed, fmt.Errorf("delay node %q not found", exec.CurrentNodeID)
	}
	if delayNode.NextNode == "" {
		if err := e.db.CompleteExecution(ctx, exec.ID); err != nil {
			return StatusFailed, err
		}
		return StatusCompleted, nil
	}
	// Advance past the delay node and continue running.
	if err := e.db.AdvanceExecution(ctx, exec.ID, delayNode.NextNode, StatusRunning); err != nil {
		return StatusFailed, err
	}
	exec.CurrentNodeID = delayNode.NextNode
	return e.Run(ctx, exec, nodes)
}

func (e *Executor) executeEmail(
	ctx context.Context,
	exec *postgres.ExecutionRow,
	node postgres.WorkflowNode,
	log *zap.Logger,
) error {
	contact, err := e.db.GetContactForWorkflow(ctx, exec.ContactID, exec.WorkspaceID)
	if err != nil {
		return fmt.Errorf("load contact: %w", err)
	}

	subject, _ := node.Config["subject"].(string)
	html, _ := node.Config["html"].(string)
	text, _ := node.Config["text"].(string)
	fromEmail, _ := node.Config["fromEmail"].(string)
	fromName, _ := node.Config["fromName"].(string)
	replyTo, _ := node.Config["replyTo"].(string)

	if subject == "" || (html == "" && text == "") {
		return fmt.Errorf("email node %s missing subject or body", node.ID)
	}

	toEmail := ""
	toName := ""
	if contact != nil {
		toEmail = contact.Email
		toName = contact.Name
	}
	if toEmail == "" {
		// Contact has no email — skip silently (not a fatal error).
		log.Warn("contact has no email, skipping email node",
			zap.String("contact_id", exec.ContactID),
			zap.String("node_id", node.ID),
		)
		return nil
	}

	payload := types.TransactionalEmailPayload{
		JobID:       fmt.Sprintf("wf-%s-%s", exec.ID, node.ID),
		WorkspaceID: exec.WorkspaceID,
		SendID:      uuid.New().String(),
		To:          []types.EmailAddress{{Email: toEmail, Name: toName}},
		From:        types.EmailAddress{Email: fromEmail, Name: fromName},
		ReplyTo:     replyTo,
		Subject:     subject,
		HTML:        html,
		Text:        text,
		Tags:        map[string]string{"workflow_id": exec.WorkflowID, "execution_id": exec.ID},
		Provider:    "ses",
	}

	msgID := fmt.Sprintf("wf-email-%s-%s", exec.ID, node.ID)
	if err := e.pub.Publish(ctx, "email.send.transactional", payload, msgID); err != nil {
		return fmt.Errorf("publish email: %w", err)
	}
	log.Info("email triggered",
		zap.String("node_id", node.ID),
		zap.String("to", toEmail),
	)
	return nil
}

// buildNodeMap indexes nodes by ID for O(1) lookup.
func buildNodeMap(nodes []postgres.WorkflowNode) map[string]postgres.WorkflowNode {
	m := make(map[string]postgres.WorkflowNode, len(nodes))
	for _, n := range nodes {
		m[n.ID] = n
	}
	return m
}

// parseDuration extracts the delay duration from a node config.
// Config must have either "duration" (Go duration string) or "seconds" (int).
func parseDuration(cfg map[string]interface{}) (time.Duration, error) {
	if d, ok := cfg["duration"].(string); ok {
		return time.ParseDuration(d)
	}
	if s, ok := cfg["seconds"].(float64); ok {
		return time.Duration(s) * time.Second, nil
	}
	if s, ok := cfg["seconds"].(int); ok {
		return time.Duration(s) * time.Second, nil
	}
	return 0, fmt.Errorf("delay node config missing 'duration' or 'seconds'")
}

// ParseWorkflowNodes deserialises the JSONB nodes array from a workflow row.
func ParseWorkflowNodes(data []byte) ([]postgres.WorkflowNode, error) {
	if len(data) == 0 || string(data) == "null" || string(data) == "[]" {
		return nil, nil
	}
	var nodes []postgres.WorkflowNode
	if err := json.Unmarshal(data, &nodes); err != nil {
		return nil, fmt.Errorf("parse workflow nodes: %w", err)
	}
	return nodes, nil
}
