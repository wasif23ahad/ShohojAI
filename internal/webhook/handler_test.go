package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"messenger-agent/internal/debounce"
)

type stubDebouncer struct {
	mu      sync.Mutex
	seenMID map[string]bool
	pushed  []debounce.InboundMessage
}

func newStubDebouncer() *stubDebouncer {
	return &stubDebouncer{
		seenMID: make(map[string]bool),
	}
}

func (s *stubDebouncer) ClaimMessageID(_ context.Context, mid string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seenMID[mid] {
		return false, nil
	}
	s.seenMID[mid] = true
	return true, nil
}

func (s *stubDebouncer) Push(_ context.Context, msg debounce.InboundMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pushed = append(s.pushed, msg)
	return nil
}

type stubPauseChecker struct {
	paused map[string]bool
}

func (p *stubPauseChecker) IsBotPaused(_ context.Context, senderID string) (bool, error) {
	return p.paused[senderID], nil
}

func signBody(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func TestHandler_Verify(t *testing.T) {
	t.Parallel()

	const verifyToken = "my-secret-verify-token"
	h := NewHandler(verifyToken, "app-secret", nil, nil, nil)

	tests := []struct {
		name       string
		query      string
		wantStatus int
		wantBody   string
	}{
		{
			name:       "valid subscribe challenge",
			query:      "hub.mode=subscribe&hub.verify_token=my-secret-verify-token&hub.challenge=987654321",
			wantStatus: http.StatusOK,
			wantBody:   "987654321",
		},
		{
			name:       "invalid verify_token rejected",
			query:      "hub.mode=subscribe&hub.verify_token=wrong-token&hub.challenge=987654321",
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "invalid mode rejected",
			query:      "hub.mode=unsubscribe&hub.verify_token=my-secret-verify-token&hub.challenge=987654321",
			wantStatus: http.StatusForbidden,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodGet, "/webhook?"+tc.query, nil)
			rec := httptest.NewRecorder()

			h.Verify(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if tc.wantBody != "" && rec.Body.String() != tc.wantBody {
				t.Errorf("body = %q, want %q", rec.Body.String(), tc.wantBody)
			}
		})
	}
}

func TestHandler_Receive_SignaturesAndFiltering(t *testing.T) {
	t.Parallel()

	const appSecret = "test-meta-app-secret-key"

	validBody := []byte(`{
		"object": "page",
		"entry": [{
			"id": "PAGE_123",
			"time": 1700000000000,
			"messaging": [{
				"sender": {"id": "USER_001"},
				"recipient": {"id": "PAGE_123"},
				"timestamp": 1700000000001,
				"message": {
					"mid": "mid.valid.1",
					"text": "vai ei hoodie ta ache?",
					"attachments": [
						{"type": "image", "payload": {"url": "https://cdn.example.com/hoodie.jpg"}},
						{"type": "audio", "payload": {"url": "https://cdn.example.com/voice.mp4"}}
					]
				}
			}]
		}]
	}`)

	echoBody := []byte(`{
		"object": "page",
		"entry": [{
			"id": "PAGE_123",
			"time": 1700000000000,
			"messaging": [{
				"sender": {"id": "PAGE_123"},
				"recipient": {"id": "USER_001"},
				"timestamp": 1700000000002,
				"message": {
					"mid": "mid.echo.1",
					"text": "echo reply",
					"is_echo": true
				}
			}]
		}]
	}`)

	tests := []struct {
		name           string
		body           []byte
		sigHeader      string
		pausedSenders  map[string]bool
		preClaimedMIDs []string
		wantStatus     int
		wantPushedLen  int
	}{
		{
			name:          "valid HMAC-SHA256 signature pushes message and attachments",
			body:          validBody,
			sigHeader:     signBody(appSecret, validBody),
			wantStatus:    http.StatusOK,
			wantPushedLen: 1,
		},
		{
			name:          "tampered X-Hub-Signature-256 rejected with 401",
			body:          validBody,
			sigHeader:     signBody("wrong-secret", validBody),
			wantStatus:    http.StatusUnauthorized,
			wantPushedLen: 0,
		},
		{
			name:          "missing X-Hub-Signature-256 rejected with 401",
			body:          validBody,
			sigHeader:     "",
			wantStatus:    http.StatusUnauthorized,
			wantPushedLen: 0,
		},
		{
			name:          "malformed hex in X-Hub-Signature-256 rejected with 401",
			body:          validBody,
			sigHeader:     "sha256=zzzznothex",
			wantStatus:    http.StatusUnauthorized,
			wantPushedLen: 0,
		},
		{
			name:           "duplicate MID is deduplicated and not pushed again",
			body:           validBody,
			sigHeader:      signBody(appSecret, validBody),
			preClaimedMIDs: []string{"mid.valid.1"},
			wantStatus:     http.StatusOK,
			wantPushedLen:  0,
		},
		{
			name:          "paused sender thread is skipped",
			body:          validBody,
			sigHeader:     signBody(appSecret, validBody),
			pausedSenders: map[string]bool{"USER_001": true},
			wantStatus:    http.StatusOK,
			wantPushedLen: 0,
		},
		{
			name:          "echo message is ignored",
			body:          echoBody,
			sigHeader:     signBody(appSecret, echoBody),
			wantStatus:    http.StatusOK,
			wantPushedLen: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			deb := newStubDebouncer()
			for _, mid := range tc.preClaimedMIDs {
				deb.seenMID[mid] = true
			}
			pauseChecker := &stubPauseChecker{paused: tc.pausedSenders}
			h := NewHandler("verify-token", appSecret, deb, pauseChecker, nil)

			req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(tc.body))
			if tc.sigHeader != "" {
				req.Header.Set(SignatureHeader, tc.sigHeader)
			}
			rec := httptest.NewRecorder()

			h.Receive(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}

			deb.mu.Lock()
			defer deb.mu.Unlock()
			if len(deb.pushed) != tc.wantPushedLen {
				t.Fatalf("len(pushed) = %d, want %d", len(deb.pushed), tc.wantPushedLen)
			}
			if tc.wantPushedLen == 1 {
				got := deb.pushed[0]
				if got.MID != "mid.valid.1" || got.SenderID != "USER_001" {
					t.Errorf("unexpected pushed message: %+v", got)
				}
				if len(got.ImageURLs) != 1 || len(got.AudioURLs) != 1 {
					t.Errorf("expected 1 image URL and 1 audio URL, got %+v", got)
				}
			}
		})
	}
}

func TestHandler_Receive_OversizedPayloadRejected(t *testing.T) {
	t.Parallel()

	const appSecret = "test-meta-app-secret-key"
	h := NewHandler("verify-token", appSecret, newStubDebouncer(), nil, nil)

	largeBody := []byte(`{"object":"page","pad":"` + strings.Repeat("A", MaxWebhookBodyBytes+1024) + `"}`)
	req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(largeBody))
	req.Header.Set(SignatureHeader, signBody(appSecret, largeBody))
	rec := httptest.NewRecorder()

	h.Receive(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 Request Entity Too Large, got %d", rec.Code)
	}
}
