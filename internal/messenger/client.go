package messenger

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
	"unicode"
)

const (
	// DefaultGraphAPIBaseURL is the Meta Graph API v26.0 base endpoint.
	DefaultGraphAPIBaseURL = "https://graph.facebook.com/v26.0"
	// MaxMessageRunes is the safe UTF-8 rune ceiling per outbound Messenger message (Meta hard limit is 2,000).
	MaxMessageRunes = 1900
	// MessagingTypeResponse is required by Meta's 24-hour standard messaging window policy.
	MessagingTypeResponse = "RESPONSE"

	defaultMaxRetries = 3
	defaultBaseDelay  = 250 * time.Millisecond
)

// Client sends typing indicators and UTF-8 rune-safe text messages via Meta Graph API v26.0.
type Client struct {
	baseURL         string
	pageAccessToken string
	httpClient      *http.Client
	logger          *slog.Logger
	maxRetries      int
	baseDelay       time.Duration
}

// Option customizes a Messenger Client.
type Option func(*Client)

// WithHTTPClient overrides the default *http.Client.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) {
		if hc != nil {
			c.httpClient = hc
		}
	}
}

// WithRetryConfig overrides the exponential backoff retry count and initial delay.
func WithRetryConfig(maxRetries int, baseDelay time.Duration) Option {
	return func(c *Client) {
		if maxRetries >= 0 {
			c.maxRetries = maxRetries
		}
		if baseDelay > 0 {
			c.baseDelay = baseDelay
		}
	}
}

// NewClient constructs a Meta Graph API v26.0 outbound client with explicit dependency injection.
func NewClient(baseURL, pageAccessToken string, logger *slog.Logger, opts ...Option) *Client {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		baseURL = DefaultGraphAPIBaseURL
	}
	if logger == nil {
		logger = slog.Default()
	}
	c := &Client{
		baseURL:         baseURL,
		pageAccessToken: strings.TrimSpace(pageAccessToken),
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
		logger:     logger,
		maxRetries: defaultMaxRetries,
		baseDelay:  defaultBaseDelay,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

type recipientRef struct {
	ID string `json:"id"`
}

type textMessageBody struct {
	Text string `json:"text"`
}

type sendMessageRequest struct {
	MessagingType string          `json:"messaging_type"`
	Recipient     recipientRef    `json:"recipient"`
	Message       textMessageBody `json:"message"`
}

type senderActionRequest struct {
	Recipient    recipientRef `json:"recipient"`
	SenderAction string       `json:"sender_action"`
}

// SendTyping sends a best-effort "typing_on" or "typing_off" indicator to the recipient.
func (c *Client) SendTyping(ctx context.Context, recipientID string, on bool) error {
	recipientID = strings.TrimSpace(recipientID)
	if recipientID == "" {
		return errors.New("messenger: send typing: recipient_id is required")
	}

	action := "typing_on"
	if !on {
		action = "typing_off"
	}

	reqBody := senderActionRequest{
		Recipient:    recipientRef{ID: recipientID},
		SenderAction: action,
	}
	if err := c.postMessagesEndpoint(ctx, reqBody); err != nil {
		return fmt.Errorf("messenger: send %s to %s: %w", action, recipientID, err)
	}
	return nil
}

// SendText splits text safely on UTF-8 Bangla rune boundaries (max 1,900 runes per chunk)
// and sends each chunk sequentially with "messaging_type": "RESPONSE".
func (c *Client) SendText(ctx context.Context, recipientID string, text string) error {
	recipientID = strings.TrimSpace(recipientID)
	if recipientID == "" {
		return errors.New("messenger: send text: recipient_id is required")
	}

	chunks := SplitMessageRunes(text, MaxMessageRunes)
	if len(chunks) == 0 {
		return errors.New("messenger: send text: message text is empty")
	}

	for i, chunk := range chunks {
		reqBody := sendMessageRequest{
			MessagingType: MessagingTypeResponse,
			Recipient:     recipientRef{ID: recipientID},
			Message:       textMessageBody{Text: chunk},
		}
		if err := c.postMessagesEndpoint(ctx, reqBody); err != nil {
			return fmt.Errorf("messenger: send text chunk %d/%d to %s: %w", i+1, len(chunks), recipientID, err)
		}
	}
	return nil
}

func (c *Client) postMessagesEndpoint(ctx context.Context, payload any) error {
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	endpoint := c.baseURL + "/me/messages"
	var lastErr error

	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			backoff := c.baseDelay * (1 << (attempt - 1))
			select {
			case <-ctx.Done():
				return fmt.Errorf("context canceled during retry backoff: %w", ctx.Err())
			case <-time.After(backoff):
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
		if err != nil {
			return fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		if c.pageAccessToken != "" {
			req.Header.Set("Authorization", "Bearer "+c.pageAccessToken)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("http post: %w", err)
			c.logger.Warn("transient messenger API transport error, retrying",
				"attempt", attempt+1,
				"err", err,
			)
			continue
		}

		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		_ = resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}

		lastErr = fmt.Errorf("graph api status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))

		// Retry on HTTP 429 Too Many Requests or HTTP 5xx Server Errors.
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= http.StatusInternalServerError {
			c.logger.Warn("messenger API rate-limited or 5xx, retrying",
				"status", resp.StatusCode,
				"attempt", attempt+1,
			)
			continue
		}

		// Permanent 4xx error: fail immediately without retrying.
		return lastErr
	}

	return fmt.Errorf("exceeded %d retries: %w", c.maxRetries, lastErr)
}

// SplitMessageRunes splits a UTF-8 string into chunks of at most maxRunes Unicode runes ([]rune),
// preferring paragraph (\n\n), line (\n), Bangla/English sentence (।, ?, !, .), and word boundaries,
// and never splitting a Bangla combining vowel sign or hasanta (্) from its base consonant.
func SplitMessageRunes(text string, maxRunes int) []string {
	if maxRunes <= 0 {
		maxRunes = MaxMessageRunes
	}

	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return nil
	}

	runes := []rune(trimmed)
	if len(runes) <= maxRunes {
		return []string{trimmed}
	}

	var chunks []string
	for len(runes) > 0 {
		if len(runes) <= maxRunes {
			chunk := strings.TrimSpace(string(runes))
			if chunk != "" {
				chunks = append(chunks, chunk)
			}
			break
		}

		splitIdx := chooseRuneSplitIndex(runes, maxRunes)
		chunk := strings.TrimSpace(string(runes[:splitIdx]))
		if chunk != "" {
			chunks = append(chunks, chunk)
		}

		runes = runes[splitIdx:]
		// Trim leading whitespace runes on the remaining slice.
		for len(runes) > 0 && unicode.IsSpace(runes[0]) {
			runes = runes[1:]
		}
	}

	return chunks
}

func chooseRuneSplitIndex(runes []rune, maxRunes int) int {
	window := runes[:maxRunes]
	minPreferred := maxRunes / 4
	if minPreferred < 1 {
		minPreferred = 1
	}

	// 1. Prefer paragraph break (\n\n).
	for i := len(window) - 1; i >= minPreferred; i-- {
		if window[i] == '\n' && window[i-1] == '\n' {
			return i + 1
		}
	}

	// 2. Prefer single line break (\n).
	for i := len(window) - 1; i >= minPreferred; i-- {
		if window[i] == '\n' {
			return i + 1
		}
	}

	// 3. Prefer sentence boundary (Bangla dari '।', '?', '!', '.') followed by whitespace.
	for i := len(window) - 1; i >= minPreferred; i-- {
		prev := window[i-1]
		if unicode.IsSpace(window[i]) && (prev == '।' || prev == '?' || prev == '!' || prev == '.') {
			return i
		}
	}

	// 4. Prefer word boundary (whitespace).
	for i := len(window) - 1; i >= minPreferred; i-- {
		if unicode.IsSpace(window[i]) {
			return i
		}
	}

	// 5. Hard rune split: step back if we would cut directly before a combining mark / Bangla vowel sign.
	cut := maxRunes
	for cut > 1 && isCombiningOrBanglaDependentMark(runes[cut]) {
		cut--
	}
	return cut
}

func isCombiningOrBanglaDependentMark(r rune) bool {
	if unicode.Is(unicode.Mn, r) || unicode.Is(unicode.Mc, r) || unicode.Is(unicode.Me, r) {
		return true
	}
	// Bangla dependent vowel signs, candrabindu, anusvara, visarga, nukta, hasanta (virama), and au length mark.
	switch {
	case r >= 0x0981 && r <= 0x0983:
		return true
	case r >= 0x09BC && r <= 0x09CD:
		return true
	case r == 0x09D7:
		return true
	default:
		return false
	}
}
