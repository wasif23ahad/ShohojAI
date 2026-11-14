package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"messenger-agent/internal/debounce"
)

const (
	// MaxWebhookBodyBytes enforces the 1 MB maximum payload size on POST /webhook.
	MaxWebhookBodyBytes = 1 << 20 // 1 MB
	// SignatureHeader is the Meta HMAC-SHA256 webhook signature header.
	SignatureHeader = "X-Hub-Signature-256"
	// signaturePrefix is the required prefix on X-Hub-Signature-256.
	signaturePrefix = "sha256="
)

// MessageDebouncer defines the debouncer operations used by the webhook handler.
type MessageDebouncer interface {
	ClaimMessageID(ctx context.Context, mid string) (bool, error)
	Push(ctx context.Context, msg debounce.InboundMessage) error
}

// BotPauseChecker checks whether AI responses are paused for a given customer thread.
type BotPauseChecker interface {
	IsBotPaused(ctx context.Context, senderID string) (bool, error)
}

// Handler serves GET /webhook verification and POST /webhook event ingestion.
type Handler struct {
	verifyToken string
	appSecret   []byte
	debouncer   MessageDebouncer
	pauseCheck  BotPauseChecker
	logger      *slog.Logger
}

// NewHandler constructs a Meta Webhook Handler with explicit dependency injection.
func NewHandler(
	verifyToken string,
	appSecret string,
	debouncer MessageDebouncer,
	pauseCheck BotPauseChecker,
	logger *slog.Logger,
) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{
		verifyToken: verifyToken,
		appSecret:   []byte(appSecret),
		debouncer:   debouncer,
		pauseCheck:  pauseCheck,
		logger:      logger,
	}
}

// Verify handles GET /webhook subscription verification challenges from Meta.
func (h *Handler) Verify(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	mode := q.Get("hub.mode")
	token := q.Get("hub.verify_token")
	challenge := q.Get("hub.challenge")

	if mode != "subscribe" || len(h.verifyToken) == 0 {
		h.logger.Warn("webhook verification rejected: invalid mode", "mode", mode)
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	// Non-Negotiable Rule #4: Always use crypto/subtle.ConstantTimeCompare for hub.verify_token.
	if subtle.ConstantTimeCompare([]byte(token), []byte(h.verifyToken)) != 1 {
		h.logger.Warn("webhook verification rejected: token mismatch")
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, challenge)
}

// Receive handles POST /webhook events, verifying X-Hub-Signature-256, deduplicating by mid,
// pushing to the Redis debouncer, and returning 200 OK immediately without blocking.
func (h *Handler) Receive(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, MaxWebhookBodyBytes)
	defer func() { _ = r.Body.Close() }()

	rawBody, err := io.ReadAll(r.Body)
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			h.logger.Warn("webhook payload exceeded 1MB limit")
			http.Error(w, "Request Entity Too Large", http.StatusRequestEntityTooLarge)
			return
		}
		h.logger.Error("failed reading webhook request body", "err", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	sigHeader := r.Header.Get(SignatureHeader)
	if !VerifySignature(rawBody, sigHeader, h.appSecret) {
		h.logger.Warn("webhook signature verification failed", "sig_present", sigHeader != "")
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var payload Payload
	if err := json.Unmarshal(rawBody, &payload); err != nil {
		h.logger.Warn("malformed webhook JSON payload", "err", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	if payload.Object != "page" {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	ctx := r.Context()
	for _, entry := range payload.Entry {
		for _, ev := range entry.Messaging {
			h.ingestMessagingEvent(ctx, ev)
		}
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "EVENT_RECEIVED")
}

func (h *Handler) ingestMessagingEvent(ctx context.Context, ev MessagingEvent) {
	senderID := strings.TrimSpace(ev.Sender.ID)
	if senderID == "" {
		return
	}

	var (
		mid       string
		text      string
		imageURLs []string
		audioURLs []string
	)

	switch {
	case ev.Message != nil:
		if ev.Message.IsEcho {
			return
		}
		mid = strings.TrimSpace(ev.Message.MID)
		text = strings.TrimSpace(ev.Message.Text)
		for _, att := range ev.Message.Attachments {
			url := strings.TrimSpace(att.Payload.URL)
			if url == "" {
				continue
			}
			switch strings.ToLower(strings.TrimSpace(att.Type)) {
			case "image":
				imageURLs = append(imageURLs, url)
			case "audio":
				audioURLs = append(audioURLs, url)
			}
		}
	case ev.Postback != nil:
		mid = strings.TrimSpace(ev.Postback.MID)
		text = strings.TrimSpace(ev.Postback.Payload)
		if text == "" {
			text = strings.TrimSpace(ev.Postback.Title)
		}
	default:
		return
	}

	if mid == "" || (text == "" && len(imageURLs) == 0 && len(audioURLs) == 0) {
		return
	}

	if h.pauseCheck != nil {
		paused, err := h.pauseCheck.IsBotPaused(ctx, senderID)
		if err != nil {
			h.logger.Error("failed checking bot pause state", "sender_id", senderID, "err", err)
		} else if paused {
			h.logger.Info("skipping message for paused thread", "sender_id", senderID, "mid", mid)
			return
		}
	}

	if h.debouncer == nil {
		return
	}

	claimed, err := h.debouncer.ClaimMessageID(ctx, mid)
	if err != nil {
		h.logger.Error("failed claiming message id", "sender_id", senderID, "mid", mid, "err", err)
		return
	}
	if !claimed {
		h.logger.Debug("skipping duplicate webhook message", "sender_id", senderID, "mid", mid)
		return
	}

	inbound := debounce.InboundMessage{
		MID:         mid,
		SenderID:    senderID,
		RecipientID: strings.TrimSpace(ev.Recipient.ID),
		Text:        text,
		ImageURLs:   imageURLs,
		AudioURLs:   audioURLs,
		Timestamp:   ev.Timestamp,
	}
	if err := h.debouncer.Push(ctx, inbound); err != nil {
		h.logger.Error("failed pushing message to debouncer", "sender_id", senderID, "mid", mid, "err", err)
	}
}

// VerifySignature validates X-Hub-Signature-256 ("sha256=<hex>") against HMAC-SHA256(body, secret)
// using constant-time hmac.Equal per AGENTS.md Rule #4.
func VerifySignature(body []byte, sigHeader string, secret []byte) bool {
	if len(secret) == 0 || !strings.HasPrefix(sigHeader, signaturePrefix) {
		return false
	}
	providedHex := strings.TrimPrefix(sigHeader, signaturePrefix)
	providedMAC, err := hex.DecodeString(providedHex)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	expectedMAC := mac.Sum(nil)
	return hmac.Equal(providedMAC, expectedMAC)
}
