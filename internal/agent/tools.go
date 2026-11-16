package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"messenger-agent/internal/store"
)

const (
	ToolSearchProducts       = "search_products"
	ToolCheckStock           = "check_stock"
	ToolCalculateDeliveryFee = "calculate_delivery_fee"
	ToolCreateOrder          = "create_order"
	ToolEscalateToHuman      = "escalate_to_human"
)

// QueryEmbedder generates a vector embedding for a product search query.
type QueryEmbedder interface {
	EmbedQuery(ctx context.Context, text string) ([]float32, error)
}

// ProductOrderStore abstracts the PostgreSQL repository operations needed by deterministic agent tools.
type ProductOrderStore interface {
	SearchProductsHybrid(ctx context.Context, query string, category string, queryEmbedding []float32, limit int) ([]store.Product, error)
	CheckStock(ctx context.Context, sku string, variant string) (*store.StockStatus, error)
	CreateOrderTx(ctx context.Context, req store.CreateOrderRequest) (*store.OrderConfirmation, error)
	SetCustomerBotPaused(ctx context.Context, senderID string, paused bool) error
}

// ThreadPauseStore abstracts the Redis bot pause flag operations needed by escalate_to_human.
type ThreadPauseStore interface {
	SetBotPaused(ctx context.Context, senderID string, paused bool) error
}

// ToolDefinition describes a deterministic Go tool exposed to the LLM for function calling.
type ToolDefinition struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"`
}

// ToolCall represents a structured tool invocation requested by the LLM.
type ToolCall struct {
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

// EscalationResult is returned by the escalate_to_human tool.
type EscalationResult struct {
	SenderID  string `json:"sender_id"`
	Paused    bool   `json:"paused"`
	Reason    string `json:"reason"`
	MessageBN string `json:"message_bn"`
}

// ToolDeclarations returns the JSON Schema declarations for all 5 Bangladeshi commerce tools.
func ToolDeclarations() []ToolDefinition {
	return []ToolDefinition{
		{
			Name:        ToolSearchProducts,
			Description: "Search products in the catalog using hybrid semantic vector and Bangla/Banglish/English text matching.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"query": map[string]any{
						"type":        "string",
						"description": "Product search query in English, Bangla, or Banglish (e.g. 'black hoodie', 'panjabi').",
					},
					"category": map[string]any{
						"type":        "string",
						"description": "Optional product category filter (e.g. 'hoodie', 'panjabi', 'saree').",
					},
				},
				"required": []string{"query"},
			},
		},
		{
			Name:        ToolCheckStock,
			Description: "Check real-time stock quantity and BDT price for a specific product SKU and optional size/color variant.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"product_sku": map[string]any{
						"type":        "string",
						"description": "Exact product SKU code (e.g. 'HD-BLK-001').",
					},
					"variant": map[string]any{
						"type":        "string",
						"description": "Optional size or variant (e.g. 'M', 'L', 'XL', 'XXL', '42').",
					},
				},
				"required": []string{"product_sku"},
			},
		},
		{
			Name:        ToolCalculateDeliveryFee,
			Description: "Calculate deterministic delivery charge in BDT and estimated delivery time for a Bangladeshi district or area.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"district": map[string]any{
						"type":        "string",
						"description": "Customer district or area name in Bangla or English (e.g. 'Dhaka', 'Mirpur', 'Gazipur', 'Chattogram').",
					},
				},
				"required": []string{"district"},
			},
		},
		{
			Name:        ToolCreateOrder,
			Description: "Create a confirmed Cash-on-Delivery customer order in PostgreSQL, validating Bangladeshi phone number and decrementing live stock.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"sender_id": map[string]any{
						"type":        "string",
						"description": "Customer Messenger PSID.",
					},
					"customer_name": map[string]any{
						"type":        "string",
						"description": "Customer full name.",
					},
					"phone": map[string]any{
						"type":        "string",
						"description": "Bangladeshi mobile phone number (01XXXXXXXXX or +8801XXXXXXXXX).",
					},
					"address": map[string]any{
						"type":        "string",
						"description": "Full delivery street/house/area address.",
					},
					"district": map[string]any{
						"type":        "string",
						"description": "Delivery district (e.g. 'Dhaka', 'Gazipur', 'Sylhet').",
					},
					"items": map[string]any{
						"type":        "array",
						"description": "List of ordered items with SKU, optional variant, and quantity.",
						"items": map[string]any{
							"type": "object",
							"properties": map[string]any{
								"sku":      map[string]any{"type": "string"},
								"variant":  map[string]any{"type": "string"},
								"quantity": map[string]any{"type": "integer"},
							},
							"required": []string{"sku", "quantity"},
						},
					},
				},
				"required": []string{"customer_name", "phone", "address", "district", "items"},
			},
		},
		{
			Name:        ToolEscalateToHuman,
			Description: "Pause AI responses for this thread and escalate to a human customer support agent when the customer complains, requests a refund, or asks for a human.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"sender_id": map[string]any{
						"type":        "string",
						"description": "Customer Messenger PSID.",
					},
					"reason": map[string]any{
						"type":        "string",
						"description": "Reason for human handoff (e.g. 'customer complaint', 'refund request', 'explicit human request').",
					},
				},
				"required": []string{"reason"},
			},
		},
	}
}

// ToolExecutor executes deterministic commerce tool calls backed by PostgreSQL and Redis.
type ToolExecutor struct {
	pgStore    ProductOrderStore
	redisStore ThreadPauseStore
	embedder   QueryEmbedder
	logger     *slog.Logger
}

// NewToolExecutor creates a ToolExecutor with explicit dependency injection.
func NewToolExecutor(
	pgStore ProductOrderStore,
	redisStore ThreadPauseStore,
	embedder QueryEmbedder,
	logger *slog.Logger,
) *ToolExecutor {
	if logger == nil {
		logger = slog.Default()
	}
	return &ToolExecutor{
		pgStore:    pgStore,
		redisStore: redisStore,
		embedder:   embedder,
		logger:     logger,
	}
}

// Execute runs the requested ToolCall deterministically and returns a JSON result payload.
func (e *ToolExecutor) Execute(ctx context.Context, sessionSenderID string, call ToolCall) (string, error) {
	switch strings.TrimSpace(call.Name) {
	case ToolSearchProducts:
		return e.execSearchProducts(ctx, call.Arguments)
	case ToolCheckStock:
		return e.execCheckStock(ctx, call.Arguments)
	case ToolCalculateDeliveryFee:
		return e.execCalculateDeliveryFee(call.Arguments)
	case ToolCreateOrder:
		return e.execCreateOrder(ctx, sessionSenderID, call.Arguments)
	case ToolEscalateToHuman:
		return e.execEscalateToHuman(ctx, sessionSenderID, call.Arguments)
	default:
		return "", fmt.Errorf("agent: unknown tool %q", call.Name)
	}
}

func (e *ToolExecutor) execSearchProducts(ctx context.Context, rawArgs json.RawMessage) (string, error) {
	var args struct {
		Query    string `json:"query"`
		Category string `json:"category"`
	}
	if err := json.Unmarshal(rawArgs, &args); err != nil {
		return "", fmt.Errorf("agent: decode search_products args: %w", err)
	}

	normalizedQuery := NormalizeBanglishQuery(args.Query)
	var queryVec []float32
	if e.embedder != nil && normalizedQuery != "" {
		vec, err := e.embedder.EmbedQuery(ctx, normalizedQuery)
		if err != nil {
			e.logger.Warn("embedding generation failed, falling back to pg_trgm search",
				"query", normalizedQuery,
				"err", err,
			)
		} else {
			queryVec = vec
		}
	}

	products, err := e.pgStore.SearchProductsHybrid(ctx, normalizedQuery, args.Category, queryVec, 5)
	if err != nil {
		return "", fmt.Errorf("agent: search_products: %w", err)
	}
	return marshalToolResult(map[string]any{
		"normalized_query": normalizedQuery,
		"count":            len(products),
		"products":         products,
	})
}

func (e *ToolExecutor) execCheckStock(ctx context.Context, rawArgs json.RawMessage) (string, error) {
	var args struct {
		ProductSKU string `json:"product_sku"`
		SKU        string `json:"sku"`
		Variant    string `json:"variant"`
	}
	if err := json.Unmarshal(rawArgs, &args); err != nil {
		return "", fmt.Errorf("agent: decode check_stock args: %w", err)
	}
	sku := strings.TrimSpace(args.ProductSKU)
	if sku == "" {
		sku = strings.TrimSpace(args.SKU)
	}

	status, err := e.pgStore.CheckStock(ctx, sku, args.Variant)
	if err != nil {
		if errors.Is(err, store.ErrProductNotFound) {
			return marshalToolResult(map[string]any{
				"found":    false,
				"in_stock": false,
				"sku":      sku,
				"error":    "product SKU not found",
			})
		}
		return "", fmt.Errorf("agent: check_stock: %w", err)
	}
	return marshalToolResult(status)
}

func (e *ToolExecutor) execCalculateDeliveryFee(rawArgs json.RawMessage) (string, error) {
	var args struct {
		District string `json:"district"`
	}
	if err := json.Unmarshal(rawArgs, &args); err != nil {
		return "", fmt.Errorf("agent: decode calculate_delivery_fee args: %w", err)
	}
	quote := store.CalculateDeliveryFee(args.District)
	return marshalToolResult(quote)
}

func (e *ToolExecutor) execCreateOrder(ctx context.Context, sessionSenderID string, rawArgs json.RawMessage) (string, error) {
	var args struct {
		SenderID     string                 `json:"sender_id"`
		CustomerName string                 `json:"customer_name"`
		Phone        string                 `json:"phone"`
		Address      string                 `json:"address"`
		District     string                 `json:"district"`
		Items        []store.OrderItemInput `json:"items"`
	}
	if err := json.Unmarshal(rawArgs, &args); err != nil {
		return "", fmt.Errorf("agent: decode create_order args: %w", err)
	}

	// Always prefer the authenticated webhook session sender_id to prevent cross-thread spoofing.
	senderID := strings.TrimSpace(sessionSenderID)
	if senderID == "" {
		senderID = strings.TrimSpace(args.SenderID)
	}

	confirmation, err := e.pgStore.CreateOrderTx(ctx, store.CreateOrderRequest{
		SenderID:     senderID,
		CustomerName: args.CustomerName,
		Phone:        args.Phone,
		Address:      args.Address,
		District:     args.District,
		Items:        args.Items,
	})
	if err != nil {
		if errors.Is(err, store.ErrInvalidPhone) || errors.Is(err, store.ErrInsufficientStock) || errors.Is(err, store.ErrProductNotFound) {
			return marshalToolResult(map[string]any{
				"success": false,
				"error":   err.Error(),
			})
		}
		return "", fmt.Errorf("agent: create_order: %w", err)
	}
	return marshalToolResult(map[string]any{
		"success":      true,
		"confirmation": confirmation,
	})
}

func (e *ToolExecutor) execEscalateToHuman(ctx context.Context, sessionSenderID string, rawArgs json.RawMessage) (string, error) {
	var args struct {
		SenderID string `json:"sender_id"`
		Reason   string `json:"reason"`
	}
	if err := json.Unmarshal(rawArgs, &args); err != nil {
		return "", fmt.Errorf("agent: decode escalate_to_human args: %w", err)
	}

	senderID := strings.TrimSpace(sessionSenderID)
	if senderID == "" {
		senderID = strings.TrimSpace(args.SenderID)
	}
	if senderID == "" {
		return "", errors.New("agent: escalate_to_human: sender_id is required")
	}

	reason := strings.TrimSpace(args.Reason)
	if reason == "" {
		reason = "customer requested human support"
	}

	if e.redisStore != nil {
		if err := e.redisStore.SetBotPaused(ctx, senderID, true); err != nil {
			return "", fmt.Errorf("agent: pause bot in redis for %s: %w", senderID, err)
		}
	}
	if e.pgStore != nil {
		if err := e.pgStore.SetCustomerBotPaused(ctx, senderID, true); err != nil {
			e.logger.Warn("failed updating customer bot_paused flag in postgres",
				"sender_id", senderID,
				"err", err,
			)
		}
	}

	e.logger.Warn("thread escalated to human operator",
		"sender_id", senderID,
		"reason", reason,
	)

	res := EscalationResult{
		SenderID:  senderID,
		Paused:    true,
		Reason:    reason,
		MessageBN: EscalationConfirmationBangla,
	}
	return marshalToolResult(res)
}

func marshalToolResult(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("agent: marshal tool result: %w", err)
	}
	return string(b), nil
}
