package messenger

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

const (
	// MaxMediaBytes caps downloaded Messenger image/audio attachments at 20 MB.
	MaxMediaBytes = 20 << 20 // 20 MB
)

// MediaData holds downloaded raw attachment bytes and detected MIME type.
type MediaData struct {
	URL      string
	MIMEType string
	Data     []byte
}

// AudioTranscriber transcribes raw audio bytes (such as Messenger Bangla voice notes) into text.
type AudioTranscriber interface {
	TranscribeAudio(ctx context.Context, audioBytes []byte, mimeType string) (string, error)
}

// MediaProcessor fetches image attachments and transcribes audio voice notes from Meta CDN URLs.
type MediaProcessor struct {
	httpClient  *http.Client
	transcriber AudioTranscriber
	logger      *slog.Logger
}

// NewMediaProcessor constructs a MediaProcessor with explicit dependency injection.
func NewMediaProcessor(httpClient *http.Client, transcriber AudioTranscriber, logger *slog.Logger) *MediaProcessor {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 20 * time.Second}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &MediaProcessor{
		httpClient:  httpClient,
		transcriber: transcriber,
		logger:      logger,
	}
}

// FetchAttachment downloads media bytes from a URL with a 20 MB limit and detects its MIME type.
func (m *MediaProcessor) FetchAttachment(ctx context.Context, mediaURL string) (*MediaData, error) {
	mediaURL = strings.TrimSpace(mediaURL)
	if mediaURL == "" {
		return nil, errors.New("messenger: fetch attachment: media url is required")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, mediaURL, nil)
	if err != nil {
		return nil, fmt.Errorf("messenger: create media request: %w", err)
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("messenger: download media %s: %w", mediaURL, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("messenger: download media %s returned status %d", mediaURL, resp.StatusCode)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxMediaBytes+1))
	if err != nil {
		return nil, fmt.Errorf("messenger: read media body: %w", err)
	}
	if len(data) > MaxMediaBytes {
		return nil, fmt.Errorf("messenger: media attachment exceeds %d bytes limit", MaxMediaBytes)
	}

	mimeType := strings.TrimSpace(resp.Header.Get("Content-Type"))
	if idx := strings.Index(mimeType, ";"); idx != -1 {
		mimeType = strings.TrimSpace(mimeType[:idx])
	}
	if mimeType == "" || mimeType == "application/octet-stream" {
		mimeType = http.DetectContentType(data)
	}

	return &MediaData{
		URL:      mediaURL,
		MIMEType: mimeType,
		Data:     data,
	}, nil
}

// TranscribeVoiceNote downloads an audio attachment from audioURL and transcribes it using the configured AudioTranscriber.
func (m *MediaProcessor) TranscribeVoiceNote(ctx context.Context, audioURL string) (string, error) {
	if m.transcriber == nil {
		return "", errors.New("messenger: audio transcriber is not configured")
	}

	media, err := m.FetchAttachment(ctx, audioURL)
	if err != nil {
		return "", fmt.Errorf("messenger: fetch voice note: %w", err)
	}

	mimeType := media.MIMEType
	if mimeType == "" || strings.HasPrefix(mimeType, "application/") {
		mimeType = "audio/mp4"
	}

	transcript, err := m.transcriber.TranscribeAudio(ctx, media.Data, mimeType)
	if err != nil {
		return "", fmt.Errorf("messenger: transcribe voice note: %w", err)
	}
	return strings.TrimSpace(transcript), nil
}
