package agent

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"messenger-agent/internal/debounce"
	"messenger-agent/internal/messenger"
	"messenger-agent/internal/store"
)

const (
	maxToolIterations = 6
)

// TurnMessage represents a message in the LLM conversation context.
type TurnMessage struct {
	Role       string     `json:"role"` // "user", "assistant", "tool"
	Content    string     `json:"content,omitempty"`
	ImageURLs  []string   `json:"image_urls,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
	ToolName   string     `json:"tool_name,omitempty"`
}

// GenerateRequest holds the full prompt, history, multimodal inputs, and tool declarations for a turn.
type GenerateRequest struct {
	SystemPrompt string           `json:"system_prompt"`
	Messages     []TurnMessage    `json:"messages"`
	Tools        []ToolDefinition `json:"tools"`
}

// GenerateResponse represents the LLM's output (either tool calls or final Bangla text).
type GenerateResponse struct {
	Text      string     `json:"text,omitempty"`
	ToolCalls []ToolCall `json:"tool_calls,omitempty"`
}

// LLMClient abstracts the multimodal LLM and embedding provider.
type LLMClient interface {
	Generate(ctx context.Context, req GenerateRequest) (*GenerateResponse, error)
	EmbedQuery(ctx context.Context, text string) ([]float32, error)
	TranscribeAudio(ctx context.Context, audioBytes []byte, mimeType string) (string, error)
}

// OutboundMessenger abstracts the Meta Messenger client used to send typing hints and replies.
type OutboundMessenger interface {
	SendTyping(ctx context.Context, recipientID string, on bool) error
	SendText(ctx context.Context, recipientID string, text string) error
}

// HistoryStore abstracts PostgreSQL and Redis conversation persistence.
type HistoryStore interface {
	ProductOrderStore
	SaveMessage(ctx context.Context, msg store.ChatMessage) (int64, error)
	GetRecentHistory(ctx context.Context, senderID string, limit int) ([]store.ChatMessage, error)
}

// CacheStore abstracts Redis pause state and sliding-window chat history caching.
type CacheStore interface {
	ThreadPauseStore
	IsBotPaused(ctx context.Context, senderID string) (bool, error)
	GetCachedChatHistory(ctx context.Context, senderID string) ([]store.ChatMessage, error)
	AppendChatHistory(ctx context.Context, senderID string, msgs ...store.ChatMessage) error
}

// Orchestrator coordinates the debounced burst handler, multimodal ingestion, deterministic tool loop,
// and UTF-8 rune-safe Bangla reply dispatch.
type Orchestrator struct {
	pgStore     HistoryStore
	redisStore  CacheStore
	messenger   OutboundMessenger
	media       *messenger.MediaProcessor
	llm         LLMClient
	tools       *ToolExecutor
	logger      *slog.Logger
}

// NewOrchestrator constructs an Orchestrator with explicit dependency injection.
func NewOrchestrator(
	pgStore HistoryStore,
	redisStore CacheStore,
	msgClient OutboundMessenger,
	mediaProc *messenger.MediaProcessor,
	llmClient LLMClient,
	logger *slog.Logger,
) *Orchestrator {
	if logger == nil {
		logger = slog.Default()
	}
	toolExec := NewToolExecutor(pgStore, redisStore, llmClient, logger)
	return &Orchestrator{
		pgStore:    pgStore,
		redisStore: redisStore,
		messenger:  msgClient,
		media:      mediaProc,
		llm:        llmClient,
		tools:      toolExec,
		logger:     logger,
	}
}

// HandleBurst processes an aggregated burst of customer messages flushed by the Redis debouncer.
func (o *Orchestrator) HandleBurst(ctx context.Context, senderID string, msgs []debounce.InboundMessage) {
	senderID = strings.TrimSpace(senderID)
	if senderID == "" || len(msgs) == 0 {
		return
	}

	// Double-check whether the thread was paused while the debounce timer was pending.
	if o.redisStore != nil {
		if paused, err := o.redisStore.IsBotPaused(ctx, senderID); err == nil && paused {
			o.logger.Info("skipping debounced turn because thread is paused", "sender_id", senderID)
			return
		}
	}

	// Best-effort typing_on UI hint (AGENTS.md Rule #5).
	if o.messenger != nil {
		_ = o.messenger.SendTyping(ctx, senderID, true)
	}

	combinedText, imageURLs, lastMID := o.aggregateBurstInputs(ctx, senderID, msgs)
	if combinedText == "" && len(imageURLs) == 0 {
		return
	}

	// Load sliding-window history (Redis cache first, fallback to PostgreSQL).
	historyTurns := o.loadConversationHistory(ctx, senderID)

	// Persist aggregated user message turn.
	userChatMsg := store.ChatMessage{
		SenderID:  senderID,
		MID:       lastMID,
		Role:      "user",
		Content:   combinedText,
		CreatedAt: time.Now().UTC(),
	}
	if o.pgStore != nil {
		if _, err := o.pgStore.SaveMessage(ctx, userChatMsg); err != nil {
			o.logger.Warn("failed saving user message to postgres", "sender_id", senderID, "err", err)
		}
	}
	if o.redisStore != nil {
		if err := o.redisStore.AppendChatHistory(ctx, senderID, userChatMsg); err != nil {
			o.logger.Warn("failed appending user message to redis cache", "sender_id", senderID, "err", err)
		}
	}

	turnMsgs := make([]TurnMessage, 0, len(historyTurns)+4)
	for _, h := range historyTurns {
		if h.Role == "user" || h.Role == "assistant" {
			turnMsgs = append(turnMsgs, TurnMessage{
				Role:    h.Role,
				Content: h.Content,
			})
		}
	}
	turnMsgs = append(turnMsgs, TurnMessage{
		Role:      "user",
		Content:   combinedText,
		ImageURLs: imageURLs,
	})

	replyText := o.runToolCallingLoop(ctx, senderID, turnMsgs)

	assistantChatMsg := store.ChatMessage{
		SenderID:  senderID,
		Role:      "assistant",
		Content:   replyText,
		CreatedAt: time.Now().UTC(),
	}
	if o.pgStore != nil {
		if _, err := o.pgStore.SaveMessage(ctx, assistantChatMsg); err != nil {
			o.logger.Warn("failed saving assistant reply to postgres", "sender_id", senderID, "err", err)
		}
	}
	if o.redisStore != nil {
		if err := o.redisStore.AppendChatHistory(ctx, senderID, assistantChatMsg); err != nil {
			o.logger.Warn("failed appending assistant reply to redis cache", "sender_id", senderID, "err", err)
		}
	}

	if o.messenger != nil {
		if err := o.messenger.SendText(ctx, senderID, replyText); err != nil {
			o.logger.Error("failed sending outbound messenger reply",
				"sender_id", senderID,
				"err", err,
			)
		}
	}
}

func (o *Orchestrator) aggregateBurstInputs(
	ctx context.Context,
	senderID string,
	msgs []debounce.InboundMessage,
) (string, []string, string) {
	var (
		textParts []string
		imageURLs []string
		lastMID   string
	)

	for _, m := range msgs {
		if strings.TrimSpace(m.MID) != "" {
			lastMID = strings.TrimSpace(m.MID)
		}
		if t := strings.TrimSpace(m.Text); t != "" {
			textParts = append(textParts, t)
		}
		for _, img := range m.ImageURLs {
			if u := strings.TrimSpace(img); u != "" {
				imageURLs = append(imageURLs, u)
			}
		}
		for _, audioURL := range m.AudioURLs {
			if o.media == nil {
				continue
			}
			transcript, err := o.media.TranscribeVoiceNote(ctx, audioURL)
			if err != nil {
				o.logger.Warn("voice note transcription failed",
					"sender_id", senderID,
					"audio_url", audioURL,
					"err", err,
				)
				continue
			}
			if transcript != "" {
				textParts = append(textParts, "[ভয়েস মেসেজ]: "+transcript)
			}
		}
	}

	if len(textParts) == 0 && len(imageURLs) > 0 {
		textParts = append(textParts, "[গ্রাহক একটি পণ্যের ছবি পাঠিয়েছেন]")
	}

	return strings.Join(textParts, "\n"), imageURLs, lastMID
}

func (o *Orchestrator) loadConversationHistory(ctx context.Context, senderID string) []store.ChatMessage {
	if o.redisStore != nil {
		cached, err := o.redisStore.GetCachedChatHistory(ctx, senderID)
		if err == nil && len(cached) > 0 {
			return cached
		}
	}
	if o.pgStore != nil {
		history, err := o.pgStore.GetRecentHistory(ctx, senderID, store.MaxCachedHistoryTurns)
		if err == nil && len(history) > 0 {
			if o.redisStore != nil {
				_ = o.redisStore.AppendChatHistory(ctx, senderID, history...)
			}
			return history
		}
	}
	return nil
}

func (o *Orchestrator) runToolCallingLoop(ctx context.Context, senderID string, messages []TurnMessage) string {
	if o.llm == nil {
		return FallbackBanglaReply
	}

	sysPrompt := BuildSystemPrompt(senderID)
	decls := ToolDeclarations()

	for iter := 0; iter < maxToolIterations; iter++ {
		resp, err := o.llm.Generate(ctx, GenerateRequest{
			SystemPrompt: sysPrompt,
			Messages:     messages,
			Tools:        decls,
		})
		if err != nil {
			o.logger.Error("LLM generate failed",
				"sender_id", senderID,
				"iteration", iter,
				"err", err,
			)
			return FallbackBanglaReply
		}

		if len(resp.ToolCalls) == 0 {
			finalText := strings.TrimSpace(resp.Text)
			if finalText == "" {
				return FallbackBanglaReply
			}
			return finalText
		}

		messages = append(messages, TurnMessage{
			Role:      "assistant",
			Content:   resp.Text,
			ToolCalls: resp.ToolCalls,
		})

		escalated := false
		for _, call := range resp.ToolCalls {
			resultJSON, err := o.tools.Execute(ctx, senderID, call)
			if err != nil {
				o.logger.Warn("tool execution error",
					"sender_id", senderID,
					"tool", call.Name,
					"err", err,
				)
				resultJSON = fmt.Sprintf(`{"error":%q}`, err.Error())
			}
			if call.Name == ToolEscalateToHuman {
				escalated = true
			}
			messages = append(messages, TurnMessage{
				Role:       "tool",
				ToolCallID: call.ID,
				ToolName:   call.Name,
				Content:    resultJSON,
			})
		}

		if escalated {
			// Give LLM one final pass to produce a polite handoff message, or return EscalationConfirmationBangla.
			finalResp, err := o.llm.Generate(ctx, GenerateRequest{
				SystemPrompt: sysPrompt,
				Messages:     messages,
			})
			if err == nil && strings.TrimSpace(finalResp.Text) != "" {
				return strings.TrimSpace(finalResp.Text)
			}
			return EscalationConfirmationBangla
		}
	}

	return FallbackBanglaReply
}

// GeminiClient implements LLMClient using the official Google Gemini Generative Language API.
type GeminiClient struct {
	apiKey         string
	model          string
	embeddingModel string
	baseURL        string
	httpClient     *http.Client
	logger         *slog.Logger
}

// NewGeminiClient constructs a multimodal GeminiClient supporting tool calling, embeddings, and audio transcription.
func NewGeminiClient(apiKey, model, embeddingModel string, httpClient *http.Client, logger *slog.Logger) *GeminiClient {
	if strings.TrimSpace(model) == "" {
		model = "gemini-2.5-flash"
	}
	if strings.TrimSpace(embeddingModel) == "" {
		embeddingModel = "text-embedding-004"
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &GeminiClient{
		apiKey:         strings.TrimSpace(apiKey),
		model:          strings.TrimSpace(model),
		embeddingModel: strings.TrimSpace(embeddingModel),
		baseURL:        "https://generativelanguage.googleapis.com/v1beta",
		httpClient:     httpClient,
		logger:         logger,
	}
}

// Generate invokes Gemini generateContent with systemInstruction, conversation contents, and functionDeclarations.
func (g *GeminiClient) Generate(ctx context.Context, req GenerateRequest) (*GenerateResponse, error) {
	var contents []map[string]any
	for _, m := range req.Messages {
		switch m.Role {
		case "user":
			parts := []map[string]any{}
			if m.Content != "" {
				parts = append(parts, map[string]any{"text": m.Content})
			}
			for _, u := range m.ImageURLs {
				parts = append(parts, map[string]any{
					"text": fmt.Sprintf("[Customer attached image URL: %s]", u),
				})
			}
			if len(parts) > 0 {
				contents = append(contents, map[string]any{
					"role":  "user",
					"parts": parts,
				})
			}
		case "assistant":
			parts := []map[string]any{}
			if m.Content != "" {
				parts = append(parts, map[string]any{"text": m.Content})
			}
			for _, tc := range m.ToolCalls {
				var argsObj map[string]any
				_ = json.Unmarshal(tc.Arguments, &argsObj)
				parts = append(parts, map[string]any{
					"functionCall": map[string]any{
						"name": tc.Name,
						"args": argsObj,
					},
				})
			}
			if len(parts) > 0 {
				contents = append(contents, map[string]any{
					"role":  "model",
					"parts": parts,
				})
			}
		case "tool":
			var respObj map[string]any
			if err := json.Unmarshal([]byte(m.Content), &respObj); err != nil {
				respObj = map[string]any{"result": m.Content}
			}
			contents = append(contents, map[string]any{
				"role": "function",
				"parts": []map[string]any{
					{
						"functionResponse": map[string]any{
							"name":     m.ToolName,
							"response": respObj,
						},
					},
				},
			})
		}
	}

	payload := map[string]any{
		"systemInstruction": map[string]any{
			"parts": []map[string]any{
				{"text": req.SystemPrompt},
			},
		},
		"contents": contents,
	}

	if len(req.Tools) > 0 {
		decls := make([]map[string]any, 0, len(req.Tools))
		for _, t := range req.Tools {
			decls = append(decls, map[string]any{
				"name":        t.Name,
				"description": t.Description,
				"parameters":  t.Parameters,
			})
		}
		payload["tools"] = []map[string]any{
			{"functionDeclarations": decls},
		}
	}

	endpoint := fmt.Sprintf("%s/models/%s:generateContent?key=%s", g.baseURL, g.model, g.apiKey)
	respBytes, err := g.postJSON(ctx, endpoint, payload)
	if err != nil {
		return nil, err
	}

	var parsed struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text         string `json:"text,omitempty"`
					FunctionCall *struct {
						Name string          `json:"name"`
						Args json.RawMessage `json:"args"`
					} `json:"functionCall,omitempty"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
	}
	if err := json.Unmarshal(respBytes, &parsed); err != nil {
		return nil, fmt.Errorf("gemini: decode response: %w", err)
	}
	if len(parsed.Candidates) == 0 {
		return nil, errors.New("gemini: empty candidates in response")
	}

	out := &GenerateResponse{}
	var textBuilder strings.Builder
	for i, part := range parsed.Candidates[0].Content.Parts {
		if part.FunctionCall != nil {
			out.ToolCalls = append(out.ToolCalls, ToolCall{
				ID:        fmt.Sprintf("call_%d", i+1),
				Name:      part.FunctionCall.Name,
				Arguments: part.FunctionCall.Args,
			})
		} else if part.Text != "" {
			if textBuilder.Len() > 0 {
				textBuilder.WriteString("\n")
			}
			textBuilder.WriteString(part.Text)
		}
	}
	out.Text = textBuilder.String()
	return out, nil
}

// EmbedQuery generates a 1536-dimensional (or model-configured) vector embedding for text.
func (g *GeminiClient) EmbedQuery(ctx context.Context, text string) ([]float32, error) {
	payload := map[string]any{
		"model": "models/" + g.embeddingModel,
		"content": map[string]any{
			"parts": []map[string]any{
				{"text": text},
			},
		},
	}
	endpoint := fmt.Sprintf("%s/models/%s:embedContent?key=%s", g.baseURL, g.embeddingModel, g.apiKey)
	respBytes, err := g.postJSON(ctx, endpoint, payload)
	if err != nil {
		return nil, err
	}

	var parsed struct {
		Embedding struct {
			Values []float32 `json:"values"`
		} `json:"embedding"`
	}
	if err := json.Unmarshal(respBytes, &parsed); err != nil {
		return nil, fmt.Errorf("gemini: decode embedding response: %w", err)
	}
	return parsed.Embedding.Values, nil
}

// TranscribeAudio transcribes a voice note into Bangla/English text using Gemini multimodal inlineData.
func (g *GeminiClient) TranscribeAudio(ctx context.Context, audioBytes []byte, mimeType string) (string, error) {
	if len(audioBytes) == 0 {
		return "", errors.New("gemini: empty audio bytes")
	}
	if mimeType == "" {
		mimeType = "audio/mp4"
	}
	payload := map[string]any{
		"contents": []map[string]any{
			{
				"role": "user",
				"parts": []map[string]any{
					{
						"text": "Transcribe this Bangladeshi customer voice note accurately into Bangla or English text. Return only the transcription.",
					},
					{
						"inlineData": map[string]any{
							"mimeType": mimeType,
							"data":     base64.StdEncoding.EncodeToString(audioBytes),
						},
					},
				},
			},
		},
	}
	endpoint := fmt.Sprintf("%s/models/%s:generateContent?key=%s", g.baseURL, g.model, g.apiKey)
	respBytes, err := g.postJSON(ctx, endpoint, payload)
	if err != nil {
		return "", err
	}
	var parsed struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
	}
	if err := json.Unmarshal(respBytes, &parsed); err != nil {
		return "", err
	}
	if len(parsed.Candidates) == 0 || len(parsed.Candidates[0].Content.Parts) == 0 {
		return "", errors.New("gemini: empty transcription response")
	}
	return strings.TrimSpace(parsed.Candidates[0].Content.Parts[0].Text), nil
}

func (g *GeminiClient) postJSON(ctx context.Context, endpoint string, payload any) ([]byte, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("gemini: marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("gemini: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("gemini: http request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, fmt.Errorf("gemini: read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("gemini: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}
