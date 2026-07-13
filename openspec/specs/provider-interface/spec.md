# Purpose

Defines the `MessageProvider` trait — the contract every messaging source connector
must implement. This trait is the single integration point between Iris core and
external messaging platforms.

# Requirements

### Requirement: MessageProvider Trait

The system must define an async `MessageProvider` trait that all source connectors
implement.

- **metadata()**: Returns static provider metadata (id, name, capabilities).
- **list_threads()**: Returns threads ordered by last_message_at descending.
- **list_messages()**: Returns messages in a thread, optionally paginated by
  time cursor.
- **list_contacts()**: Returns contacts known to the provider.
- **send_message()**: Sends a message (if SendMessages capability is present).

#### Scenario: Provider lists threads with limit

When list_threads is called with limit=20, the provider returns at most 20 threads.

#### Scenario: Provider paginates messages by time

When list_messages is called with a `before` cursor, only messages older than that
timestamp are returned.

#### Scenario: Provider rejects unsupported send

When send_message is called on a provider without SendMessages capability, the
system returns an UnsupportedCapability error.

### Requirement: Provider Capability Advertisement

Each provider must statically declare its capabilities via ProviderMetadata.

- Capabilities: ListMessages, SendMessages, ListThreads, ListContacts,
  ReceiveRealtime, MarkRead, DeleteMessages.

#### Scenario: Capability-based feature gating

When a consumer queries a provider's capabilities, they can determine at runtime
which operations are supported before calling them.

### Requirement: Provider Registration

The system must allow providers to be registered and looked up by their string ID.

#### Scenario: Multiple providers coexist

When two providers (e.g., telegram, email) are registered, queries can target a
specific provider or aggregate across all providers.
