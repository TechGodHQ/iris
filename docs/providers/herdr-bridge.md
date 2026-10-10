# Herdr → Iris bridge

`iris-herdr-bridge` is a separate hosted daemon for the installed Herdr **0.8.0 / protocol 19**. It subscribes to Herdr's local newline-delimited JSON Unix socket, maps events with the existing pure Iris Herdr mapper, and writes source-agnostic `IngestBatch` requests to Iris `POST /ingest`. It does not add a provider-specific Iris route or change generated surfaces.

The existing `AppendMessage` mutation's internally tagged newtype collided with `Message::kind`, making message batches impossible to deserialize. This change represents that existing mutation as `{ "kind": "append_message", "message": { ... } }`, with a core round-trip test and an authenticated-router/retry test. The `/ingest` route, OpenAPI source, and generated public surfaces are unchanged.

The process verifies the exact Herdr version/protocol on every connection. It exits neither on a missing socket nor on an Iris outage: socket failures reconnect every two seconds, while a planned pane-set refresh reconnects immediately so a new pane's status subscription is not delayed. HTTP retries use capped exponential backoff.

The bridge refreshes `workspace.list` for each workspace/tab/pane/status event that can mutate a thread and refreshes `pane.list` for pane/status events before normalizing them. Herdr v0.8.0's unfiltered pane-status subscription emits transitions after its baseline but no initial current-status event; an optional `agent_status` filter would also suppress later transitions to other statuses. After each successful subscription acknowledgement, the bridge refreshes pane/workspace snapshots and durably captures one synthetic current-status observation per active pane before reading the stream. This establishes current status on startup/reconnect while leaving the ongoing subscription unfiltered. The observation can repeat after reconnect; it is deliberately not payload-deduplicated. Live transition frames carry their own workspace ID, status, and optional agent/display/state-label context; those source-time fields are preserved, and omitted optional fields are not borrowed from a later pane snapshot. Synthetic startup/reconnect observations are enriched only from the fresh pane snapshot used to create them. Cross-workspace `pane.moved` events preserve both previous and current workspace IDs and attach snapshots for both, so the old open workspace removes the moved pane while the new one includes it. Workspace thread metadata is a fixed allowlist of workspace summary fields plus current pane IDs, tab IDs, agent/status, and state labels; raw workspace/pane objects are not forwarded. Source event frames are projected to an allowlist before they enter the spool; titles, paths, terminal contents, token maps, and unknown fields are excluded. Before delivery, every thread upsert is replaced with the current normalized snapshot so Iris's replace-on-upsert behavior cannot erase workspace/tab/pane metadata. If no active snapshot exists, stale thread upserts are suppressed (and stale status events are dropped as a whole rather than creating orphan messages).

## Delivery and recovery contract

- Protocol 19 events have no upstream event ID, global sequence, or resume cursor. Herdr polls its independent subscriptions in request order, so frames from different event kinds/panes are not guaranteed to be source-chronological; the bridge preserves wire-arrival order and cannot reconstruct a total source order. Each event receives a bridge-owned UUID when durably captured. Non-status subscriptions may replay retained history; the status subscription starts at its current sequence and emits future transitions. Reconnect status snapshots may repeat an identical status message; this is accepted. The bridge does not deduplicate by payload or claim exactly-once upstream recovery, because identical events can be legitimate.
- The bridge persists each event before acknowledging it to its own delivery loop. It converts up to 20 events into one immutable request and flushes when the batch reaches 20 events or its oldest event is five seconds old.
- Each source frame is projected to an explicit event/data allowlist and durably persisted with `mapping_ready=false` before per-event IPC. Raw titles, paths, terminal details, token maps, arbitrary envelope fields, and unknown payload fields are not stored. After fresh workspace/pane snapshot lookups succeed, the normalized record is atomically rewritten as ready. If a lookup fails, that normalized record blocks later ordered sealing; the next connection resolves it from fresh startup snapshots before subscribing. This avoids relying on Herdr's bounded replay buffer to recover an event already read from the socket.
- The complete serialized `IngestBatch`, including its stable replay key, is atomically persisted and synced before any HTTP request. Retried/restarted delivery submits the exact same request body and identity, allowing Iris's transactional ingest replay check to make an uncertain HTTP retry idempotent. Only normalized event projections and allowlisted workspace/pane summaries remain in the private spool. The bridge never logs or copies the raw Herdr event into Iris message metadata or the audit record.
- Herdr v0.8.0 retains at most 512 events. If the bridge is disconnected long enough for older events to rotate out before it reconnects, protocol 19 supplies no resume cursor or loss marker; that upstream offline loss cannot be repaired by this bridge.
- The local spool is mode `0700`, its files are mode `0600`, and it is bounded to 10,000 queued events (including events in pending batches). On overflow it drops the oldest queued event/batch and logs only the count. An Iris outage longer than the spool can absorb therefore causes explicitly logged local loss.
- Iris `/ingest` accepts request bodies up to 1 MiB. If one normalized source event alone exceeds that limit, the bridge retains it as the oldest spool record and blocks sealing later events rather than retiring it as a successful audit-only delivery. It emits one content-free warning per process; if the queue eventually reaches its approved 10,000-event bound, normal oldest-first spool eviction applies and logs only the aggregate loss count. Reducing the source event or changing the Iris request limit requires a separately reviewed change.
- When no upstream event has arrived for an hour, the bridge enqueues a synthetic `bridge_heartbeat` as a system message in the dedicated Herdr bridge-health thread. This is ordinary authenticated ingest, not a Herdr event.
- Request/response bodies, bearer tokens, workspace names, terminal contents, and event IDs are not written to logs. Logs contain only aggregate loss counts, the local spool sequence and configured body limit for a blocked oversized record, protocol error classes, and HTTP status codes.

## Build and isolated verification

From the repository root:

```sh
cargo test -p iris-herdr-bridge --locked
cargo build --locked -p iris-herdr-bridge
```

The crate tests use Herdr v0.8.0/protocol-19 subscribe-ack, workspace-list, pane-created, and pane-status fixtures, a synthetic Unix socket, and a loopback HTTP mock. Fixtures include synthetic tokens, private titles, and worktree paths and verify those values never reach the durable spool or Iris metadata. Tests do not connect to endver, install secrets, or call the configured Iris host.

## User-unit installation (manual/local acceptance only)

Build and install the binary on the target host after reviewing the PR:

```sh
cargo build --release --locked -p iris-herdr-bridge
install -d -m 0700 "$HOME/.local/bin" "$HOME/.config/herdr-iris-bridge" \
  "$HOME/.local/state/herdr-iris-bridge" "$HOME/.config/systemd/user"
install -m 0755 target/release/iris-herdr-bridge "$HOME/.local/bin/iris-herdr-bridge"
install -m 0644 contrib/systemd/herdr-iris-bridge.service \
  "$HOME/.config/systemd/user/herdr-iris-bridge.service"
```

Create `$HOME/.config/herdr-iris-bridge/env` in a trusted local session. Never commit this file or copy its contents into issue comments/logs. The bridge defaults `HERDR_SOCKET` to `$HOME/.config/herdr/herdr.sock`; omit that variable for the default. If an alternate socket path is necessary, use a literal absolute path: systemd `EnvironmentFile` entries are not shell-expanded, so `$HOME` would be passed literally.

```sh
IRIS_INGEST_TOKEN="<the locally provisioned Iris ingest bearer token>"
```

Then restrict permissions and start the user unit:

```sh
chmod 0600 "$HOME/.config/herdr-iris-bridge/env"
systemctl --user daemon-reload
systemctl --user enable --now herdr-iris-bridge.service
systemctl --user status herdr-iris-bridge.service
journalctl --user -u herdr-iris-bridge.service
```

The Iris endpoint is deliberately fixed to `http://100.66.233.79:9876/ingest`, the approved ticket target; only the bearer credential is provisioned locally. The environment file must contain a non-empty single-line token. The service unit has no secret value and does not install itself. For local acceptance, separately verify service restart, Herdr restart/reconnect, batch-flush journal lines, and real Iris delivery. Hosted fixtures/build tests are not evidence that installation, credentials, live delivery, or deployment occurred.
