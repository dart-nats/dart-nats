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
