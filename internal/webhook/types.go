package webhook

// Payload represents the top-level Meta Graph API webhook event body.
type Payload struct {
	Object string  `json:"object"`
	Entry  []Entry `json:"entry"`
}

// Entry represents a single page webhook entry batch.
type Entry struct {
	ID        string           `json:"id"`
	Time      int64            `json:"time"`
	Messaging []MessagingEvent `json:"messaging"`
}

// MessagingEvent represents an individual Messenger event inside an Entry.
type MessagingEvent struct {
	Sender    UserRef          `json:"sender"`
	Recipient UserRef          `json:"recipient"`
	Timestamp int64            `json:"timestamp"`
	Message   *MessagePayload  `json:"message,omitempty"`
	Postback  *PostbackPayload `json:"postback,omitempty"`
}

// UserRef identifies a Messenger PSID (sender) or Page ID (recipient).
type UserRef struct {
	ID string `json:"id"`
}

// MessagePayload represents the message object in a webhook event.
type MessagePayload struct {
	MID         string       `json:"mid"`
	Text        string       `json:"text,omitempty"`
	IsEcho      bool         `json:"is_echo,omitempty"`
	Attachments []Attachment `json:"attachments,omitempty"`
}

// Attachment represents a media attachment (image, audio, video, file) on a Messenger message.
type Attachment struct {
	Type    string            `json:"type"`
	Payload AttachmentPayload `json:"payload"`
}

// AttachmentPayload holds the CDN URL of the attachment.
type AttachmentPayload struct {
	URL string `json:"url"`
}

// PostbackPayload represents a button postback interaction.
type PostbackPayload struct {
	MID     string `json:"mid"`
	Title   string `json:"title,omitempty"`
	Payload string `json:"payload"`
}
