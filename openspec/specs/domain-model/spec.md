# Purpose

The unified domain model that all message providers normalize into. Defines the
canonical shapes for messages, contacts, and threads that are source-agnostic.

# Requirements

### Requirement: Unified Message Model

The system must define a `Message` type that represents a normalized message
from any source.

- **Fields:** id (UUID), thread_id (UUID), source (string), source_id (string),
  sender (Contact), kind (MessageKind), body (string), attachments ([]Attachment),
  timestamp (DateTime), is_outbound (bool), metadata (JSON)
- **Source field** records which provider the message originated from.
- **Metadata field** preserves provider-specific fields that don't map to core columns.

#### Scenario: Text message from Telegram

When a text message arrives from Telegram, the system normalizes it to a Message
with kind=Text, source="telegram", and body containing the message text.

#### Scenario: Image message with caption

When an image arrives with a caption, the system sets kind=Image, body=caption,
and attachments contains the image URL.

#### Scenario: Unknown message type preserved

When a message type is not recognized, the system sets kind=Unknown and preserves
the raw data in metadata.

### Requirement: Unified Contact Model

The system must define a `Contact` type representing any entity that sends or
receives messages.

- **Fields:** id (UUID), source (string), source_id (string), display_name (string?),
  avatar_url (string?), metadata (JSON)

### Requirement: Unified Thread Model

The system must define a `Thread` type representing a conversation.

- **Fields:** id (UUID), source (string), source_id (string), title (string?),
  participants ([]Contact), last_message_at (DateTime), unread_count (u32?),
  metadata (JSON)

### Requirement: Message Kind Taxonomy

The system must define a fixed set of message kinds: Text, RichText, Image, Audio,
Video, File, Sticker, Location, System, Unknown.
