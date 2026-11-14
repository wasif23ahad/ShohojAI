package messenger

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"
)

type handlerRoundTripper struct {
	handler http.Handler
}

func (rt *handlerRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	rt.handler.ServeHTTP(rec, req)
	return rec.Result(), nil
}

func TestSplitMessageRunes_BanglaUTF8Safety(t *testing.T) {
	t.Parallel()

	p1 := "জি স্যার/ম্যাডাম, আমাদের এই ব্ল্যাক ফ্লিস হুডিটির XXL সাইজ বর্তমানে স্টকে আছে।"
	p2 := "ঢাকার ভেতরে ডেলিভারি চার্জ ৭০ টাকা এবং ঢাকার বাইরে ১৩০ টাকা (ক্যাশ অন ডেলিভারি)।"
	p3 := "অর্ডার কনফার্ম করার জন্য অনুগ্রহ করে আপনার নাম, পূর্ণ ঠিকানা এবং মোবাইল নম্বরটি দিন।"
	multiParagraph := p1 + "\n\n" + p2 + "\n\n" + p3

	longWordBangla := strings.Repeat("বাংলাদেশ", 30) // 240 runes including combining vowel signs া and ে

	tests := []struct {
		name       string
		input      string
		maxRunes   int
		wantChunks int
		verify     func(t *testing.T, chunks []string)
	}{
		{
			name:       "short Bangla reply fits in single chunk",
			input:      p1,
			maxRunes:   MaxMessageRunes,
			wantChunks: 1,
			verify: func(t *testing.T, chunks []string) {
				if chunks[0] != p1 {
					t.Errorf("got %q, want %q", chunks[0], p1)
				}
			},
		},
		{
			name:       "multi-paragraph Bangla reply splits cleanly across paragraph boundaries",
			input:      multiParagraph,
			maxRunes:   95,
			wantChunks: 3,
			verify: func(t *testing.T, chunks []string) {
				if chunks[0] != p1 {
					t.Errorf("chunk[0] = %q, want %q", chunks[0], p1)
				}
				if chunks[1] != p2 {
					t.Errorf("chunk[1] = %q, want %q", chunks[1], p2)
				}
				if chunks[2] != p3 {
					t.Errorf("chunk[2] = %q, want %q", chunks[2], p3)
				}
			},
		},
		{
			name:       "continuous Bangla word with combining marks never splits mid-UTF8 or before vowel sign",
			input:      longWordBangla,
			maxRunes:   50,
			wantChunks: 5,
			verify: func(t *testing.T, chunks []string) {
				for i, c := range chunks {
					if !utf8.ValidString(c) {
						t.Fatalf("chunk[%d] is not valid UTF-8: %q", i, c)
					}
					runes := []rune(c)
					if len(runes) > 50 {
						t.Errorf("chunk[%d] rune length = %d, exceeds maxRunes 50", i, len(runes))
					}
					if isCombiningOrBanglaDependentMark(runes[0]) {
						t.Errorf("chunk[%d] starts with orphaned Bangla combining mark %U", i, runes[0])
					}
				}
				if strings.Join(chunks, "") != longWordBangla {
					t.Errorf("rejoined chunks do not match original continuous Bangla string")
				}
			},
		},
		{
			name:       "empty or whitespace string returns nil",
			input:      "   \n\t  ",
			maxRunes:   MaxMessageRunes,
			wantChunks: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			chunks := SplitMessageRunes(tc.input, tc.maxRunes)
			if len(chunks) != tc.wantChunks {
				t.Fatalf("len(chunks) = %d, want %d (chunks=%q)", len(chunks), tc.wantChunks, chunks)
			}
			for i, c := range chunks {
				if !utf8.ValidString(c) {
					t.Errorf("chunk %d contains invalid UTF-8", i)
				}
				if len([]rune(c)) > tc.maxRunes {
					t.Errorf("chunk %d has %d runes, max is %d", i, len([]rune(c)), tc.maxRunes)
				}
			}
			if tc.verify != nil {
				tc.verify(t, chunks)
			}
		})
	}
}

func TestClient_SendTypingAndSendTextWithRetries(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var (
		mu       sync.Mutex
		requests []map[string]any
		attempts atomic.Int32
	)

	mockHTTPClient := &http.Client{
		Transport: &handlerRoundTripper{
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v26.0/me/messages" {
					http.Error(w, "wrong path", http.StatusNotFound)
					return
				}
				if r.Header.Get("Authorization") != "Bearer page-secret-token" {
					http.Error(w, "unauthorized", http.StatusUnauthorized)
					return
				}

				cur := attempts.Add(1)
				// Simulate transient HTTP 429 on first request to verify exponential backoff retry.
				if cur == 1 {
					w.WriteHeader(http.StatusTooManyRequests)
					_, _ = w.Write([]byte(`{"error":{"message":"rate limit"}}`))
					return
				}

				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				mu.Lock()
				requests = append(requests, body)
				mu.Unlock()

				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"recipient_id":"USER_1","message_id":"mid.out.1"}`))
			}),
		},
	}

	client := NewClient(
		"https://graph.facebook.com/v26.0",
		"page-secret-token",
		nil,
		WithHTTPClient(mockHTTPClient),
		WithRetryConfig(3, 5*time.Millisecond),
	)

	if err := client.SendTyping(ctx, "USER_1", true); err != nil {
		t.Fatalf("SendTyping error: %v", err)
	}

	if err := client.SendText(ctx, "USER_1", "জি স্যার, আপনার অর্ডারটি কনফার্ম হয়েছে।"); err != nil {
		t.Fatalf("SendText error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 2 {
		t.Fatalf("expected 2 successful requests after retry, got %d", len(requests))
	}

	if requests[0]["sender_action"] != "typing_on" {
		t.Errorf("expected first request sender_action=typing_on, got %v", requests[0])
	}
	if requests[1]["messaging_type"] != "RESPONSE" {
		t.Errorf("expected messaging_type=RESPONSE, got %v", requests[1]["messaging_type"])
	}
}

type stubTranscriber struct {
	text string
}

func (s *stubTranscriber) TranscribeAudio(_ context.Context, _ []byte, _ string) (string, error) {
	return s.text, nil
}

func TestMediaProcessor_FetchAndTranscribe(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	mockHTTPClient := &http.Client{
		Transport: &handlerRoundTripper{
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "audio/mp4")
				_, _ = w.Write([]byte("fake-audio-payload"))
			}),
		},
	}

	mp := NewMediaProcessor(mockHTTPClient, &stubTranscriber{
		text: "ভাই এই কালো হুডিটা কি ডেলিভারি দেওয়া যাবে?",
	}, nil)

	transcript, err := mp.TranscribeVoiceNote(ctx, "https://cdn.fbsbx.com/voice.mp4")
	if err != nil {
		t.Fatalf("TranscribeVoiceNote error: %v", err)
	}
	if transcript != "ভাই এই কালো হুডিটা কি ডেলিভারি দেওয়া যাবে?" {
		t.Errorf("unexpected transcript: %q", transcript)
	}
}
