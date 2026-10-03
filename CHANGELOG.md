## 1.5.0

* A connect attempt now counts as successful only once the handshake completes (TLS upgrade, CONNECT and the first PONG), not as soon as the TCP or WebSocket connection is open ([#56](https://github.com/dart-nats/dart-nats/issues/56)). Previously, a failure after the socket opened (a TLS certificate that does not verify, `-ERR` such as `maximum connections exceeded`, a server closing the connection or never sending INFO):
  * was not retried on the first connect, even with `retry: true`, and `connect()` failed with a generic `Connection closed or disconnected` error;
  * after a reconnect, started the next attempt immediately, ignoring `retryInterval` and `retryCount`. Against a server whose certificate fails verification this meant thousands of connections per second.

  Such failures are now ordinary failed attempts: reported through `onError` with the real cause, counted toward `retryCount` and spaced by `retryInterval`. `connect(timeout: ...)` now also bounds the handshake, so a server that accepts the connection but never sends INFO no longer hangs the client.
* **Behavior changes:**
  * With `retry: true`, `connect()` keeps retrying through handshake failures instead of failing on the first one. With `retryCount: -1` it waits until a connection succeeds.
  * A connection that drops during the handshake no longer emits `Status.disconnected` or calls `onDisconnect`; the attempt is retried instead.
  * A TLS error on an established connection is now an ordinary disconnect and is retried, instead of closing the client and turning off retries. Authorization violations still close the client.
* Many thanks to contributor [@amalic](https://github.com/amalic) for the detailed report.

## 1.4.3

* Fix a `tls://` client never reconnecting after the server closed an established connection, for example on a nats-server restart ([#55](https://github.com/dart-nats/dart-nats/issues/55)). The client kept a reference to the dead secure socket, and the next attempt's TCP listener only forwards data while there is none, so the new server's INFO was dropped and the TLS upgrade never started. The client looped through TCP connects that the server closed with `TLS Handshake Failure`. Every connect attempt now starts with no references to the previous connection. This also affects `nats://` connections to a server that requires TLS.
* Fix a rejected `connect()` call changing the live connection. `connect()` applied its arguments (server pool, retry settings, a new connect completer) before checking whether the client was already in use, so a call that then threw `client in use` still swapped in its servers and retry settings, and its orphaned completer later failed as an unhandled zone error.
* Fix a TLS error on an established connection escaping as an unhandled zone error. The secure socket's error handler rethrew the `TlsException` after reporting it through `onError`, and an exception thrown from a stream handler has no caller to reach. The library also no longer prints socket errors to stdout; they are reported through `onError` only.
* Many thanks to contributor [@amalic](https://github.com/amalic) for the detailed report.

## 1.4.2

* Fix a failed socket write escaping as an unhandled zone error when the peer aborts the connection. `Socket` is an `IOSink`, so `add()` reports a write failure asynchronously on `done` instead of throwing -- the `try`/`catch` around the write never saw it and nothing was listening for it, so a keepalive ping landing on an already-aborted socket had no handler anywhere. Write failures now take the same path a read error does, `onError` followed by a `disconnected` status, on the `nats:`, `tls:`, and secure transports. On `tls://` such a failure previously reached the application as nothing at all beyond a bare status change.
* Many thanks to contributor [@amalic](https://github.com/amalic) for reporting and fixing this.

## 1.4.1

* Fix heartbeat pings raising an unhandled zone error on every disconnect. The PONG-tracking callback added in 1.3.0 used an expression body, so type inference picked `then<int>` and the null-returning `onError` handler could not satisfy `FutureOr<int>` -- visible on the Dart VM and dart2wasm (dart2js elides the cast).

## 1.4.0

* Add filtered/keep/seq-bounded stream purge: `JsStream.purge()`/`JetStream.purgeStream()` now accept optional `filter`, `keep`, and `seq` parameters (`$JS.API.STREAM.PURGE`'s own options), instead of only supporting an all-or-nothing purge.
* Add `ObjectStore.putStream()`/`getStream()` for uploading/downloading objects as a `Stream<List<int>>`/`Stream<Uint8List>` without buffering the whole payload in memory, computing the SHA-256 digest incrementally as chunks arrive.
* Fix `ObjectStore.put()`/`putStream()` leaving a previous object's chunks orphaned in the backing stream when overwriting an existing name -- the old version's chunks are now purged once the new version is safely written.
* Bump `actions/checkout`/`actions/cache` in CI to versions that don't trigger GitHub's Node 20 deprecation warning.

## 1.3.0

* Add consumer pause/resume support (NATS 2.11+): `JetStream.pauseConsumer(stream, consumer, pauseUntil)` and `JetStream.resumeConsumer(stream, consumer)`, calling `$JS.API.CONSUMER.PAUSE`. `ConsumerInfo` also gains `paused`/`pauseUntil` fields reflecting the consumer's current pause state.
* Add GitHub Actions CI (test suite against local NATS containers, analyze/format checks, cross-platform analyze jobs, publish dry-run) and automated `pub.dev` publishing on tagged releases.
* Fix heartbeat pings (`pingInterval`/`maxPingsOut`) never confirming a PONG reply actually arrived before counting toward the missed-ping limit, so a connection that was responding perfectly fine still got force-disconnected after `maxPingsOut` heartbeat ticks. The periodic heartbeat now tracks its PONG the same way `Client.ping()` does and resets the missed-ping counter on receipt.

## 1.2.4

* Update package homepage and repository URLs to point to the new `dart-nats` GitHub organization.

## 1.2.3

* Fix background reconnection loop getting stuck or stopping prematurely after network loss by evaluating connection and closed states more robustly.
* Fix socket cleanups hanging on zombie connections during abrupt network loss by introducing a soft cleanup with a 2-second timeout window.
* Correct connection callback trigger logic, ensuring `onReconnect` correctly fires during a reconnection cycle instead of `onConnect`.
* Many thanks to contributor [@JuanCordovaLazo](https://github.com/JuanCordovaLazo) for these crucial reconnect fixes.

## 1.2.2

* Fix `JetStream.listStreams()`/`listConsumers()` silently truncating at the server's page size (256 items) instead of paginating via `offset`/`total` — accounts with more streams/consumers than that lost visibility into everything past the first page, with no error or truncation flag.
* Fix `Consumer.messages()`'s pull loop and `OrderedConsumer` swallowing every delivery failure into a silent retry loop instead of surfacing it through the stream's `addError`. For pull consumers specifically, a deleted server-side consumer never actually threw from `fetch()` — it silently timed out with an empty batch, indistinguishable from a merely idle consumer — so the loop now also confirms the consumer still exists whenever a batch comes back empty.
* Fix `KeyValue.watch()`/`history()`/`keys()` leaking their ephemeral push consumer on every call — the `Consumer` returned by `createConsumer()` was discarded and never deleted once the caller cancelled or the call finished. `ConsumerConfig` also gains an `inactiveThreshold` field (serialized as `inactive_threshold`, same convention as `idleHeartbeat`) so ephemeral consumers can be given a server-side reap window as a safety net.
* Add `Message.nakSync()`/`termSync()`/`inProgressSync()`, awaitable twins of the existing `ackSync()` for `nak()`/`term()`/`inProgress()`. The originals call `pub()` without awaiting its own `Future<bool>`, so a disconnected client still reports success even though the ack/nak/term never reached the server — prefer the `*Sync` variants when that needs to be known.

## 1.2.1

* Update README.md with Microservices Framework (ADR-32) and Client Discovery documentation.

## 1.2.0

* Add heartbeats, clustering, robust reconnection, buffer limits, pull flow control, and NATS Microservices Framework (ADR-32).
* Add client-side service discovery for the ADR-32 Microservices framework, complementing the `addService`/`MicroService` hosting side:
  * `Client.discoverServices()`, `getServicesInfo()`, and `getServicesStats()` fan a request out to `$SRV.*` (optionally scoped to service name and ID) and collect all replies within a bounded timeout window.
  * Adds `PingResponse`, `InfoResponse`, `EndpointInfo`, `StatsResponse`, and `EndpointStatsInfo` to parse standard response payloads.
  * Thanks [nverbeek](https://github.com/nverbeek) for the contribution.

## 1.1.2

* Fix `JetStream.accountInfo()` returning all-zero `Tier` for single-tier accounts by parsing usage fields from the top-level response instead of a nonexistent nested `tier` key. Also fix `Tier.fromJson` crash on uint64 sentinel values by reading fields as `num`. Thanks [nverbeek](https://github.com/nverbeek) for the contribution.

## 1.1.1

* Fix defect #42: Ensure `newInbox` always appends a dot suffix (`.`) to `inboxPrefix` to conform to hierarchical NATS subject structures (`_INBOX.>` gateway callout authorization policy).
* Retain backward compatibility: Do not append double dots (`..`) if the prefix argument already ends with a dot.

## 1.1.0

* Add JetStream, Key-Value, and Object Store API enhancements and handle-based classes:
  * Introduce `JsStream` class wrapping JetStream stream operations (`info`, `purge`, `consumer`, `createConsumer`).
  * Introduce generic `Consumer<T>` class with `fetch` (for pull batching) and `messages()` (stream for push deliver or continuous pull polling in the background).
  * Introduce `AccountInfo` API mapping JetStream tier resource usage and account details.
  * Add support for `numReplicas` and `maxAge` properties in `StreamConfig`.
  * Support flexible payload publishing using `publishPayload<T>` on the `JetStream` class.
* Key-Value Store API updates:
  * Add `create` and `update` methods supporting optimistic concurrency control (via `PubOpts`).
  * Add `getRevision` to retrieve historical values.
  * Support tracking and emitting key-value entry operations (`KeyValueOp` like `put`, `delete`, and `purge`).
  * Filter out deleted and purged keys automatically in `KeyValue.get()`.
* Object Store API updates:
  * Introduce `ObjectStoreConfig` supporting `replicas`, `maxBytes`, and `ttl`.
  * Add `addLink` and `addBucketLink` methods to align with standard NATS conventions (aliasing/replacing old `putLink` and `putBucketLink`).
  * Update `list()` to correctly track tombstones and filter deleted object metadata.
  * Add helper methods `putBytes` and `getBytes`.
* Performance and refactoring improvements:
  * Optimize NATS protocol parser by replacing dynamic List growth/sublisting with direct byte processing on a manual buffer manager.
  * Clean up and document API definitions, including configuration options, exception models, and platform stubs.

## 1.0.0

* Add NATS JetStream Support (Stream/Consumer management, Publish/Subscribe, Pull mode with Ack/Nak/Term)
* Add NATS Key-Value (KV) Store (Put, Get, Delete, Purge, Watch)
* Add NATS Object Store (128 KiB chunking, automatic SHA-256 validation)

## 0.6.5
* Issue resolved when app is in background. Thanks nileshsoni97 for contribution.

## 0.6.4

* Fix Uint8List and List<int> inconsistency. Thank myxzlpltk for contribution.

## 0.6.3

* fix Exposing Security Context#31.
* Fix missing headers in request response.


## 0.6.2

* fix a bug that does not correctly parse headers containing the ':' character. Thank https://github.com/CoryHagerman for contribution.

## 0.6.1

* fix nkeys decode issue

## 0.6.0

* Support verbos acknowledge
* Chang pub, pubString to async
* Header support (hpub and hmsg)

## 0.5.1

* Add retry

## 0.5.0

* Add nkeys publicKey privateKey functions
* Revamp rqeust
* Custom inbox prefix
* Inbound structure data

## 0.4.9

* fix #22 error when connect to nats://demo.nats.io

## 0.4.8

* fix #23 Unsupported operation: Platform._version with WebSocket

## 0.4.7

* add generic type to client.request()
* fix reconnect issue
* fix retry issue

## 0.4.6

* fix bug #16 Connect to invalid ws connection does not give error

## 0.4.5

* fix bug wss: connecting bug

## 0.4.4

* fix bug #20 larger MSG payloads not always working, check if full payload present in buffer

## 0.4.2

* TLS support

## 0.4.1

* fix wss://host:port

## 0.4.0

* client.connect() support with url schema example ws://host:port or nats://nost:port
* tls:// not support yet
* discontinue client.tcpConnect()
* add nkey authentication
* add jwt authentication

## 0.3.5

* Update readme

## 0.3.4

* Support TCP socket as 0.2.x by client.tcpConnect()

## 0.3.3

* Update package dependencies

## 0.3.2

* Fix flutter web Nuid() error

## 0.3.1

* Add statusStream
* Add request timeout

## 0.3.0

* Change transport from socket to WebSock
* Support Flutter Web

## 0.2.0

* Add user passwor authentication
* Add token authentication
* Convert to Null safety
* Dart SDK version 2.12.0 - 3.0.0
* fix inbox security

## 0.1.8

* fix request error on second request

## 0.1.7

* add async support for ping()
* add message.respondString

## 0.1.6+1

* Improve receive buffer handling

## 0.1.6

* async connect
* fix defect message delay when sub receive continuous message

## 0.1.5+1

* fix defect

## 0.1.5

* request/respond function
* change some wording from payload to data

## 0.1.4+1

* add inbox to generate unique inbox subject
* add nuid to generate unique id

## 0.1.3+4

* refactor code
* add commend

## 0.1.3+1

* add string api client.pubString and message.string
* fix defect: pub sub non ascii
* fix defect: message include \r or \n
* revamp message decoding

## 0.1.2

* change api from string to byte array

## 0.1.1

* publish can be buffered.

## 0.1.0+4

* Update sample code

## 0.1.0+3

* Update sample code

## 0.1.0+2

* Update readme

## 0.1.0+1

* Add readme

## 0.0.2+1

* Add change log

## 0.0.2

* Refactor code

## 0.0.1

* Initial experimental version
