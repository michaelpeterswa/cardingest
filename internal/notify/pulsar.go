package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/michaelpeterswa/cardingest/internal/config"
)

// defaultTokenEnv is the environment variable consulted for the writer bearer
// token when a notifier config does not name one.
const defaultTokenEnv = "NOTIFY_PULSAR_TOKEN"

// pushover content limits from the writer's OpenAPI contract.
const (
	maxTitle = 250
	maxBody  = 1024
)

// pulsar delivers notifications to the pulsar-notification-pipeline writer via
// its POST /notifications HTTP endpoint (bearer auth, canonical JSON body).
type pulsar struct {
	url      string // writer base URL + /notifications
	token    string
	userKey  string
	priority int
	client   *http.Client
}

func newPulsar(c config.Notifier) (Notifier, error) {
	if c.URL == "" {
		return nil, fmt.Errorf("url is required")
	}
	if c.UserKey == "" {
		return nil, fmt.Errorf("user_key (Pushover user/group key) is required")
	}
	tokenEnv := c.TokenEnv
	if tokenEnv == "" {
		tokenEnv = defaultTokenEnv
	}
	token := os.Getenv(tokenEnv)
	if token == "" {
		return nil, fmt.Errorf("bearer token env %s is empty", tokenEnv)
	}

	return &pulsar{
		url:      strings.TrimRight(c.URL, "/") + "/notifications",
		token:    token,
		userKey:  c.UserKey,
		priority: clampPriority(c.Priority),
		client:   &http.Client{Timeout: 10 * time.Second},
	}, nil
}

// clampPriority keeps the Pushover priority within the contract's -2..2.
func clampPriority(p int) int {
	switch {
	case p < -2:
		return -2
	case p > 2:
		return 2
	default:
		return p
	}
}

// Canonical writer payload (proto3 JSON mapping; target is a flat property).
type payload struct {
	Content  content  `json:"content"`
	Pushover pushover `json:"pushover"`
}

type content struct {
	Title    string            `json:"title"`
	Body     string            `json:"body"`
	Priority int               `json:"priority,omitempty"`
	Data     map[string]string `json:"data,omitempty"`
}

type pushover struct {
	UserOrGroupKey string `json:"userOrGroupKey"`
}

func (p *pulsar) Notify(ctx context.Context, n Notification) error {
	body, _ := json.Marshal(payload{
		Content: content{
			Title:    clip(title(n), maxTitle),
			Body:     clip(body(n), maxBody),
			Priority: p.priority,
			Data:     dataMap(n),
		},
		Pushover: pushover{UserOrGroupKey: p.userKey},
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("pulsar notify: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.token)

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("pulsar notify: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode/100 != 2 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("pulsar notify: status %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func title(n Notification) string {
	return fmt.Sprintf("cardingest: slot %s %s", n.Slot, n.Event)
}

func body(n Notification) string {
	switch n.Event {
	case EventComplete:
		return fmt.Sprintf("copied %d, deduped %d, skipped %d, erased %d (%s) in %s",
			n.Copied, n.Deduped, n.Skipped, n.Erased,
			humanBytes(n.BytesCopied), n.Duration.Round(time.Millisecond))
	case EventError:
		if n.Err != "" {
			return n.Err
		}
	}
	if n.Message != "" {
		return n.Message
	}
	return "(no detail)"
}

// dataMap attaches structured fields (all string values, per the contract).
func dataMap(n Notification) map[string]string {
	d := map[string]string{
		"slot":  n.Slot,
		"event": string(n.Event),
	}
	if n.Event == EventComplete {
		d["copied"] = strconv.Itoa(n.Copied)
		d["deduped"] = strconv.Itoa(n.Deduped)
		d["skipped"] = strconv.Itoa(n.Skipped)
		d["erased"] = strconv.Itoa(n.Erased)
		d["bytes"] = strconv.FormatInt(n.BytesCopied, 10)
	}
	return d
}

func clip(s string, max int) string {
	if s == "" {
		return "(none)"
	}
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max])
}

func humanBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}
