import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';
import 'client.dart';
import 'common.dart';
import 'inbox.dart';
import 'message.dart';
import 'kv.dart';
import 'object_store.dart';
import 'subscription.dart';

/// Publish acknowledgment returned by JetStream server
class PubAck {
  /// Stream name
  final String stream;

  /// Sequence number
  final int sequence;

  /// Optional JetStream domain name
  final String? domain;

  /// Whether the message is identified as a duplicate
  final bool duplicate;

  /// Constructor for PubAck
  PubAck({
    required this.stream,
    required this.sequence,
    this.domain,
    this.duplicate = false,
  });

  /// Factory from JSON map
  factory PubAck.fromJson(Map<String, dynamic> json) {
    return PubAck(
      stream: json['stream'] as String,
      sequence: json['seq'] as int,
      domain: json['domain'] as String?,
      duplicate: json['duplicate'] as bool? ?? false,
    );
  }
}

/// JetStream Stream Configuration
class StreamConfig {
  /// Stream name
  final String name;

  /// Subjects bound to the stream
  final List<String> subjects;

  /// Storage type: 'file' or 'memory'
  final String storage;

  /// Retention policy: 'limits', 'interest', 'workqueue'
  final String retention;

  /// Maximum messages stored (-1 for unlimited)
  final int maxMsgs;

  /// Maximum bytes stored (-1 for unlimited)
  final int maxBytes;

  /// Allow rollup headers
  final bool? allowRollup;

  /// Deny delete
  final bool? denyDelete;

  /// Deny purge
  final bool? denyPurge;

  /// Discard policy: 'old', 'new'
  final String? discard;

  /// Allow direct message gets
  final bool? allowDirect;

  /// Max messages per subject
  final int? maxMsgsPerSubject;

  /// Max message size
  final int? maxMsgSize;

  /// Maximum age of messages in the stream
  final Duration? maxAge;

  /// Number of replicas for the stream configuration
  final int? numReplicas;

  /// Constructor for StreamConfig
  StreamConfig({
    required this.name,
    required this.subjects,
    this.storage = 'file',
    this.retention = 'limits',
    this.maxMsgs = -1,
    this.maxBytes = -1,
    this.allowRollup,
    this.denyDelete,
    this.denyPurge,
    this.discard,
    this.allowDirect,
    this.maxMsgsPerSubject,
    this.maxMsgSize,
    this.maxAge,
    this.numReplicas,
  });

  /// Export configuration to JSON map
  Map<String, dynamic> toJson() {
    final map = <String, dynamic>{
      'name': name,
      'subjects': subjects,
      'storage': storage,
      'retention': retention,
      'max_msgs': maxMsgs,
      'max_bytes': maxBytes,
    };
    if (allowRollup != null) {
      map['allow_rollup_hdrs'] = allowRollup;
    }
    if (denyDelete != null) {
      map['deny_delete'] = denyDelete;
    }
    if (denyPurge != null) {
      map['deny_purge'] = denyPurge;
    }
    if (discard != null) {
      map['discard'] = discard;
    }
    if (allowDirect != null) {
      map['allow_direct'] = allowDirect;
    }
    if (maxMsgsPerSubject != null) {
      map['max_msgs_per_subject'] = maxMsgsPerSubject;
    }
    if (maxMsgSize != null) {
      map['max_msg_size'] = maxMsgSize;
    }
    if (maxAge != null) {
      map['max_age'] = maxAge!.inMicroseconds * 1000;
    }
    if (numReplicas != null) {
      map['num_replicas'] = numReplicas;
    }
    return map;
  }
}

/// JetStream Consumer Configuration
class ConsumerConfig {
  /// Durable consumer name (required for Pull Mode durability)
  final String? durable;

  /// Delivery subject (required for Push Mode; omit/null for Pull Mode)
  final String? deliverSubject;

  /// Filter subject to bind to
  final String? filterSubject;

  /// Acknowledgment policy: 'explicit', 'none', 'all'
  final String ackPolicy;

  /// Delivery policy: 'all', 'last', 'new'
  final String deliverPolicy;

  /// Optional start sequence
  final int? optStartSeq;

  /// Flow control enabled
  final bool? flowControl;

  /// Idle heartbeat duration
  final Duration? idleHeartbeat;

  /// How long the server waits with no interest (no pull requests, no
  /// connected push subscriber) before automatically deleting this
  /// consumer. Chiefly useful as a safety net for ephemeral consumers: if
  /// the owning client vanishes uncleanly (crash, network drop) without
  /// ever calling `deleteConsumer`, the server reaps it on its own instead
  /// of leaving it orphaned indefinitely.
  final Duration? inactiveThreshold;

  /// Constructor for ConsumerConfig
  ConsumerConfig({
    this.durable,
    this.deliverSubject,
    this.filterSubject,
    this.ackPolicy = 'explicit',
    this.deliverPolicy = 'all',
    this.optStartSeq,
    this.flowControl,
    this.idleHeartbeat,
    this.inactiveThreshold,
  });

  /// Export configuration to JSON map
  Map<String, dynamic> toJson() {
    final map = <String, dynamic>{
      'ack_policy': ackPolicy,
      'deliver_policy': deliverPolicy,
    };
    if (durable != null) map['durable_name'] = durable;
    if (deliverSubject != null) map['deliver_subject'] = deliverSubject;
    if (filterSubject != null) map['filter_subject'] = filterSubject;
    if (optStartSeq != null) map['opt_start_seq'] = optStartSeq;
    if (flowControl != null) map['flow_control'] = flowControl;
    if (idleHeartbeat != null) {
      map['idle_heartbeat'] =
          idleHeartbeat!.inMicroseconds * 1000; // in nanoseconds
    }
    if (inactiveThreshold != null) {
      map['inactive_threshold'] =
          inactiveThreshold!.inMicroseconds * 1000; // in nanoseconds
    }
    return map;
  }
}

/// Publish options for JetStream de-duplication and optimistic concurrency control
class PubOpts {
  /// Expected message ID
  final String? msgId;

  /// Expected stream name
  final String? expectStream;

  /// Expected last sequence number
  final int? expectLastSeq;

  /// Expected last message ID
  final String? expectLastMsgId;

  /// Expected last subject sequence number
  final int? expectLastSubjectSeq;

  /// Constructor for PubOpts
  PubOpts({
    this.msgId,
    this.expectStream,
    this.expectLastSeq,
    this.expectLastMsgId,
    this.expectLastSubjectSeq,
  });
}

/// JetStream Stream State
class StreamState {
  /// Number of messages in the stream
  final int messages;

  /// Number of bytes in the stream
  final int bytes;

  /// First sequence number in the stream
  final int firstSeq;

  /// First timestamp in the stream
  final String firstTs;

  /// Last sequence number in the stream
  final int lastSeq;

  /// Last timestamp in the stream
  final String lastTs;

  /// Number of consumers bound to the stream
  final int consumerCount;

  /// Constructor for StreamState
  StreamState({
    required this.messages,
    required this.bytes,
    required this.firstSeq,
    required this.firstTs,
    required this.lastSeq,
    required this.lastTs,
    required this.consumerCount,
  });

  /// Factory from JSON map
  factory StreamState.fromJson(Map<String, dynamic> json) {
    return StreamState(
      messages: json['messages'] as int? ?? 0,
      bytes: json['bytes'] as int? ?? 0,
      firstSeq: json['first_seq'] as int? ?? 0,
      firstTs: json['first_ts'] as String? ?? '',
      lastSeq: json['last_seq'] as int? ?? 0,
      lastTs: json['last_ts'] as String? ?? '',
      consumerCount: json['consumer_count'] as int? ?? 0,
    );
  }
}

/// JetStream Stream Information
class StreamInfo {
  /// Type of response/event
  final String type;

  /// Stream configuration
  final StreamConfig config;

  /// Created timestamp
  final String created;

  /// Stream state detail
  final StreamState state;

  /// Constructor for StreamInfo
  StreamInfo({
    required this.type,
    required this.config,
    required this.created,
    required this.state,
  });

  /// Factory from JSON map
  factory StreamInfo.fromJson(Map<String, dynamic> json) {
    final cfg = json['config'] as Map<String, dynamic>? ?? {};
    return StreamInfo(
      type: json['type'] as String? ?? '',
      config: StreamConfig(
        name: cfg['name'] as String? ?? '',
        subjects: List<String>.from(cfg['subjects'] as List? ?? []),
        storage: cfg['storage'] as String? ?? 'file',
        retention: cfg['retention'] as String? ?? 'limits',
        maxMsgs: cfg['max_msgs'] as int? ?? -1,
        maxBytes: cfg['max_bytes'] as int? ?? -1,
      ),
      created: json['created'] as String? ?? '',
      state: StreamState.fromJson(json['state'] as Map<String, dynamic>? ?? {}),
    );
  }
}

/// JetStream Consumer Information
class ConsumerInfo {
  /// Type of response/event
  final String type;

  /// Stream name bound to consumer
  final String streamName;

  /// Consumer name
  final String name;

  /// Created timestamp
  final String created;

  /// Consumer configuration
  final ConsumerConfig config;

  /// Number of pending messages
  final int numPending;

  /// Number of waiting messages
  final int numWaiting;

  /// Number of ack pending messages
  final int numAckPending;

  /// Number of redelivered messages
  final int numRedelivered;

  /// Whether the consumer is currently paused (NATS 2.11+, via [JetStream.pauseConsumer])
  final bool paused;

  /// When the current pause expires, if [paused] is true
  final DateTime? pauseUntil;

  /// Constructor for ConsumerInfo
  ConsumerInfo({
    required this.type,
    required this.streamName,
    required this.name,
    required this.created,
    required this.config,
    required this.numPending,
    required this.numWaiting,
    required this.numAckPending,
    required this.numRedelivered,
    this.paused = false,
    this.pauseUntil,
  });

  /// Factory from JSON map
  factory ConsumerInfo.fromJson(Map<String, dynamic> json) {
    final cfg = json['config'] as Map<String, dynamic>? ?? {};
    return ConsumerInfo(
      type: json['type'] as String? ?? '',
      streamName: json['stream_name'] as String? ?? '',
      name: json['name'] as String? ?? '',
      created: json['created'] as String? ?? '',
      config: ConsumerConfig(
        durable: cfg['durable_name'] as String?,
        deliverSubject: cfg['deliver_subject'] as String?,
        filterSubject: cfg['filter_subject'] as String?,
        ackPolicy: cfg['ack_policy'] as String? ?? 'explicit',
        deliverPolicy: cfg['deliver_policy'] as String? ?? 'all',
      ),
      numPending: json['num_pending'] as int? ?? 0,
      numWaiting: json['num_waiting'] as int? ?? 0,
      numAckPending: json['num_ack_pending'] as int? ?? 0,
      numRedelivered: json['num_redelivered'] as int? ?? 0,
      paused: json['paused'] as bool? ?? false,
      pauseUntil: json['pause_until'] != null
          ? DateTime.tryParse(json['pause_until'] as String)
          : null,
    );
  }
}

/// Response to [JetStream.pauseConsumer]/[JetStream.resumeConsumer], mirroring
/// the server's `io.nats.jetstream.api.v1.consumer_pause_response`.
class ConsumerPauseResponse {
  /// Whether the consumer is now paused
  final bool paused;

  /// When the pause expires, if [paused] is true
  final DateTime? pauseUntil;

  /// Time remaining on the pause, if [paused] is true
  final Duration? pauseRemaining;

  /// Constructor for ConsumerPauseResponse
  ConsumerPauseResponse({
    required this.paused,
    this.pauseUntil,
    this.pauseRemaining,
  });

  /// Factory from JSON map
  factory ConsumerPauseResponse.fromJson(Map<String, dynamic> json) {
    final pauseRemainingNanos = json['pause_remaining'] as num?;
    return ConsumerPauseResponse(
      paused: json['paused'] as bool? ?? false,
      pauseUntil: json['pause_until'] != null
          ? DateTime.tryParse(json['pause_until'] as String)
          : null,
      pauseRemaining: pauseRemainingNanos != null
          ? Duration(microseconds: (pauseRemainingNanos / 1000).round())
          : null,
    );
  }
}

/// EXPERIMENTAL: JetStream APIs are experimental and subject to change in future releases.
///
/// NATS JetStream Context
class JetStream {
  /// NATS core client instance
  final Client client;

  /// Constructor for JetStream context
  JetStream(this.client);

  /// Publish a byte message to a JetStream subject and wait for acknowledgement
  Future<PubAck> publish(
    String subject,
    Uint8List data, {
    Duration timeout = const Duration(seconds: 2),
    PubOpts? opts,
    Header? header,
  }) async {
    final h = header ?? Header();
    if (opts != null) {
      if (opts.msgId != null) {
        h.add('Nats-Msg-Id', opts.msgId!);
      }
      if (opts.expectStream != null) {
        h.add('Nats-Expected-Stream', opts.expectStream!);
      }
      if (opts.expectLastSeq != null) {
        h.add('Nats-Expected-Last-Sequence', opts.expectLastSeq!.toString());
      }
      if (opts.expectLastMsgId != null) {
        h.add('Nats-Expected-Last-Msg-Id', opts.expectLastMsgId!);
      }
      if (opts.expectLastSubjectSeq != null) {
        h.add('Nats-Expected-Last-Subject-Sequence',
            opts.expectLastSubjectSeq!.toString());
      }
    }

    final response =
        await client.request(subject, data, timeout: timeout, header: h);
    final map = jsonDecode(response.string);
    if (map['error'] != null) {
      throw NatsException(map['error']['description'] as String);
    }
    return PubAck.fromJson(map as Map<String, dynamic>);
  }

  /// Publish a string message to a JetStream subject and wait for acknowledgement
  Future<PubAck> publishString(
    String subject,
    String str, {
    Duration timeout = const Duration(seconds: 2),
    PubOpts? opts,
    Header? header,
  }) {
    return publish(
      subject,
      Uint8List.fromList(utf8.encode(str)),
      timeout: timeout,
      opts: opts,
      header: header,
    );
  }

  /// Publish a generic payload to a JetStream subject and wait for acknowledgement.
  ///
  /// The payload is serialized using [serializer] if provided. If not provided:
  /// - If `payload` is `Uint8List`, it's sent directly.
  /// - If `payload` is `List<int>`, it's converted to `Uint8List`.
  /// - If `payload` is `String`, it's encoded to UTF-8.
  /// - Otherwise, it falls back to JSON encoding using `jsonEncode(payload)`.
  Future<PubAck> publishPayload<T>(
    String subject,
    T payload, {
    Duration timeout = const Duration(seconds: 2),
    PubOpts? opts,
    Header? header,
    List<int> Function(T)? serializer,
  }) {
    Uint8List bytes;
    if (serializer != null) {
      bytes = Uint8List.fromList(serializer(payload));
    } else if (payload is Uint8List) {
      bytes = payload;
    } else if (payload is List<int>) {
      bytes = Uint8List.fromList(payload);
    } else if (payload is String) {
      bytes = Uint8List.fromList(utf8.encode(payload));
    } else {
      bytes = Uint8List.fromList(utf8.encode(jsonEncode(payload)));
    }
    return publish(subject, bytes,
        timeout: timeout, opts: opts, header: header);
  }

  /// Get a handle to a stream
  JsStream stream(String name) {
    return JsStream(this, name);
  }

  /// Get a handle to a consumer
  Consumer<T> consumer<T>(String streamName, String consumerName,
      {T Function(String)? jsonDecoder}) {
    return Consumer<T>(this, streamName, consumerName,
        jsonDecoder: jsonDecoder);
  }

  /// Get information about a stream
  Future<StreamInfo> streamInfo(String streamName,
      {Duration timeout = const Duration(seconds: 2)}) async {
    final subject = '\$JS.API.STREAM.INFO.$streamName';
    final response =
        await client.request(subject, Uint8List.fromList([]), timeout: timeout);
    final map = jsonDecode(response.string);
    if (map['error'] != null) {
      throw NatsException(map['error']['description'] as String);
    }
    return StreamInfo.fromJson(map as Map<String, dynamic>);
  }

  /// Get information about a stream (Deprecated: Use streamInfo instead)
  @Deprecated('Use streamInfo instead')
  Future<StreamInfo> getStream(String streamName,
      {Duration timeout = const Duration(seconds: 2)}) {
    return streamInfo(streamName, timeout: timeout);
  }

  /// Update an existing stream
  Future<JsStream> updateStream(StreamConfig config,
      {Duration timeout = const Duration(seconds: 2)}) async {
    final subject = '\$JS.API.STREAM.UPDATE.${config.name}';
    final payload = utf8.encode(jsonEncode(config.toJson()));
    final response = await client.request(subject, Uint8List.fromList(payload),
        timeout: timeout);
    final map = jsonDecode(response.string);
    if (map['error'] != null) {
      throw NatsException(map['error']['description'] as String);
    }
    return JsStream(this, config.name);
  }

  /// Create a stream
  Future<JsStream> createStream(StreamConfig config,
      {Duration timeout = const Duration(seconds: 2)}) async {
    final subject = '\$JS.API.STREAM.CREATE.${config.name}';
    final payload = utf8.encode(jsonEncode(config.toJson()));
    final response = await client.request(subject, Uint8List.fromList(payload),
        timeout: timeout);
    final map = jsonDecode(response.string);
    if (map['error'] != null) {
      throw NatsException(map['error']['description'] as String);
    }
    return JsStream(this, config.name);
  }

  /// Create or update a stream
  Future<JsStream> createOrUpdateStream(StreamConfig config,
      {Duration timeout = const Duration(seconds: 2)}) async {
    try {
      await streamInfo(config.name, timeout: timeout);
      return await updateStream(config, timeout: timeout);
    } on NatsException catch (_) {
      return await createStream(config, timeout: timeout);
    }
  }

  /// Create or update a stream (Deprecated: Use createStream instead)
  @Deprecated('Use createStream instead')
  Future<bool> addStream(StreamConfig config,
      {Duration timeout = const Duration(seconds: 2)}) async {
    await createStream(config, timeout: timeout);
    return true;
  }

  /// Delete a stream
  Future<bool> deleteStream(String streamName,
      {Duration timeout = const Duration(seconds: 2)}) async {
    final subject = '\$JS.API.STREAM.DELETE.$streamName';
    final response =
        await client.request(subject, Uint8List.fromList([]), timeout: timeout);
    final map = jsonDecode(response.string);
    if (map['error'] != null) {
      throw NatsException(map['error']['description'] as String);
    }
    return true;
  }

  /// Purge a stream (Deprecated: Use stream(streamName).purge() instead)
  @Deprecated('Use stream(streamName).purge() instead')
  Future<bool> purgeStream(String streamName,
      {String? filter,
      int? keep,
      int? seq,
      Duration timeout = const Duration(seconds: 2)}) {
    return _purgeStream(streamName,
        filter: filter, keep: keep, seq: seq, timeout: timeout);
  }

  /// Internal purge helper.
  ///
  /// [filter] limits the purge to messages on a matching subject. [keep]
  /// retains the newest N messages (of those matching [filter], if given)
  /// instead of purging everything. [seq] purges all messages up to but
  /// not including that sequence number. [keep] and [seq] are mutually
  /// exclusive per the server's own API.
  Future<bool> _purgeStream(String streamName,
      {String? filter,
      int? keep,
      int? seq,
      Duration timeout = const Duration(seconds: 2)}) async {
    final subject = '\$JS.API.STREAM.PURGE.$streamName';
    final body = <String, dynamic>{};
    if (filter != null) body['filter'] = filter;
    if (keep != null) body['keep'] = keep;
    if (seq != null) body['seq'] = seq;
    final payload = body.isEmpty
        ? Uint8List.fromList([])
        : Uint8List.fromList(utf8.encode(jsonEncode(body)));
    final response = await client.request(subject, payload, timeout: timeout);
    final map = jsonDecode(response.string);
    if (map['error'] != null) {
      throw NatsException(map['error']['description'] as String);
    }
    return true;
  }

  /// List all streams visible to the account.
  ///
  /// Paginates through `\$JS.API.STREAM.LIST` as needed: nats-server caps
  /// each response to a fixed page (256 items) regardless of how many
  /// streams actually exist, reporting the true count in the same
  /// response's `total` field. A single unpaginated request silently drops
  /// everything past the first page on accounts with more streams than
  /// that -- no error, no truncation flag.
  Future<List<StreamInfo>> listStreams(
      {Duration timeout = const Duration(seconds: 2)}) async {
    const subject = '\$JS.API.STREAM.LIST';
    final streams = <StreamInfo>[];
    var offset = 0;
    while (true) {
      final payload = utf8.encode(jsonEncode({'offset': offset}));
      final response = await client
          .request(subject, Uint8List.fromList(payload), timeout: timeout);
      final map = jsonDecode(response.string);
      if (map['error'] != null) {
        throw NatsException(map['error']['description'] as String);
      }
      final page = (map['streams'] as List? ?? [])
          .map((item) => StreamInfo.fromJson(item as Map<String, dynamic>))
          .toList();
      streams.addAll(page);
      final total = map['total'] as int? ?? streams.length;
      offset += page.length;
      if (page.isEmpty || offset >= total) break;
    }
    return streams;
  }

  /// Find a stream name by a subject it covers
  Future<String> streamNameBySubject(String subject,
      {Duration timeout = const Duration(seconds: 2)}) async {
    const apiSubject = '\$JS.API.STREAM.NAMES';
    final payload = utf8.encode(jsonEncode({'subject': subject}));
    final response = await client
        .request(apiSubject, Uint8List.fromList(payload), timeout: timeout);
    final map = jsonDecode(response.string);
    if (map['error'] != null) {
      throw NatsException(map['error']['description'] as String);
    }
    final streams = map['streams'] as List?;
    if (streams == null || streams.isEmpty) {
      throw NatsException('no stream matches subject');
    }
    return streams.first as String;
  }

  /// Get information about a consumer
  Future<ConsumerInfo> consumerInfo(String streamName, String consumerName,
      {Duration timeout = const Duration(seconds: 2)}) async {
    final subject = '\$JS.API.CONSUMER.INFO.$streamName.$consumerName';
    final response =
        await client.request(subject, Uint8List.fromList([]), timeout: timeout);
    final map = jsonDecode(response.string);
    if (map['error'] != null) {
      throw NatsException(map['error']['description'] as String);
    }
    return ConsumerInfo.fromJson(map as Map<String, dynamic>);
  }

  /// Get information about a consumer (Deprecated: Use consumerInfo instead)
  @Deprecated('Use consumerInfo instead')
  Future<ConsumerInfo> getConsumer(String streamName, String consumerName,
      {Duration timeout = const Duration(seconds: 2)}) {
    return consumerInfo(streamName, consumerName, timeout: timeout);
  }

  /// List consumers on a stream, paginating through
  /// `\$JS.API.CONSUMER.LIST.<stream>` the same way [listStreams] does --
  /// see its doc comment for why an unpaginated request isn't safe.
  Future<List<ConsumerInfo>> listConsumers(String streamName,
      {Duration timeout = const Duration(seconds: 2)}) async {
    final subject = '\$JS.API.CONSUMER.LIST.$streamName';
    final consumers = <ConsumerInfo>[];
    var offset = 0;
    while (true) {
      final payload = utf8.encode(jsonEncode({'offset': offset}));
      final response = await client
          .request(subject, Uint8List.fromList(payload), timeout: timeout);
      final map = jsonDecode(response.string);
      if (map['error'] != null) {
        throw NatsException(map['error']['description'] as String);
      }
      final page = (map['consumers'] as List? ?? [])
          .map((item) => ConsumerInfo.fromJson(item as Map<String, dynamic>))
          .toList();
      consumers.addAll(page);
      final total = map['total'] as int? ?? consumers.length;
      offset += page.length;
      if (page.isEmpty || offset >= total) break;
    }
    return consumers;
  }

  /// Create a consumer (durable or ephemeral) on a stream
  Future<Consumer<T>> createConsumer<T>(
      String streamName, ConsumerConfig config,
      {Duration timeout = const Duration(seconds: 2),
      T Function(String)? jsonDecoder}) async {
    String subject;
    if (config.durable != null) {
      subject =
          '\$JS.API.CONSUMER.DURABLE.CREATE.$streamName.${config.durable}';
    } else {
      subject = '\$JS.API.CONSUMER.CREATE.$streamName';
    }
    final payload = utf8.encode(jsonEncode({
      'stream_name': streamName,
      'config': config.toJson(),
    }));
    final response = await client.request(subject, Uint8List.fromList(payload),
        timeout: timeout);
    final map = jsonDecode(response.string);
    if (map['error'] != null) {
      throw NatsException(map['error']['description'] as String);
    }
    final consumerName = config.durable ?? map['name'] as String? ?? '';
    return Consumer<T>(this, streamName, consumerName,
        jsonDecoder: jsonDecoder);
  }

  /// Create or update a consumer
  Future<Consumer<T>> createOrUpdateConsumer<T>(
      String streamName, ConsumerConfig config,
      {Duration timeout = const Duration(seconds: 2),
      T Function(String)? jsonDecoder}) {
    return createConsumer<T>(streamName, config,
        timeout: timeout, jsonDecoder: jsonDecoder);
  }

  /// Create a consumer (durable or ephemeral) on a stream (Deprecated: Use createConsumer instead)
  @Deprecated('Use createConsumer instead')
  Future<bool> addConsumer(String streamName, ConsumerConfig config,
      {Duration timeout = const Duration(seconds: 2)}) async {
    await createConsumer(streamName, config, timeout: timeout);
    return true;
  }

  /// Delete a consumer from a stream
  Future<bool> deleteConsumer(String streamName, String consumerName,
      {Duration timeout = const Duration(seconds: 2)}) async {
    final subject = '\$JS.API.CONSUMER.DELETE.$streamName.$consumerName';
    final response =
        await client.request(subject, Uint8List.fromList([]), timeout: timeout);
    final map = jsonDecode(response.string);
    if (map['error'] != null) {
      throw NatsException(map['error']['description'] as String);
    }
    return true;
  }

  /// Pause a consumer until [pauseUntil] (NATS 2.11+), suspending message
  /// delivery/pull-ability without deleting the consumer. Calls
  /// `\$JS.API.CONSUMER.PAUSE.<stream>.<consumer>` with a `pause_until` time.
  /// Use [resumeConsumer] (or pass a time already in the past) to unpause.
  Future<ConsumerPauseResponse> pauseConsumer(
      String streamName, String consumerName, DateTime pauseUntil,
      {Duration timeout = const Duration(seconds: 2)}) async {
    final subject = '\$JS.API.CONSUMER.PAUSE.$streamName.$consumerName';
    final payload = utf8.encode(
        jsonEncode({'pause_until': pauseUntil.toUtc().toIso8601String()}));
    final response = await client.request(subject, Uint8List.fromList(payload),
        timeout: timeout);
    final map = jsonDecode(response.string);
    if (map['error'] != null) {
      throw NatsException(map['error']['description'] as String);
    }
    return ConsumerPauseResponse.fromJson(map as Map<String, dynamic>);
  }

  /// Resume a previously-[pauseConsumer]-paused consumer immediately, by
  /// sending the same `\$JS.API.CONSUMER.PAUSE` request with no `pause_until`
  /// (the server's documented way to clear an active pause).
  Future<ConsumerPauseResponse> resumeConsumer(
      String streamName, String consumerName,
      {Duration timeout = const Duration(seconds: 2)}) async {
    final subject = '\$JS.API.CONSUMER.PAUSE.$streamName.$consumerName';
    final response = await client.request(
        subject, Uint8List.fromList(utf8.encode(jsonEncode({}))),
        timeout: timeout);
    final map = jsonDecode(response.string);
    if (map['error'] != null) {
      throw NatsException(map['error']['description'] as String);
    }
    return ConsumerPauseResponse.fromJson(map as Map<String, dynamic>);
  }

  /// Pull a batch of messages from a pull consumer (Deprecated: Use consumer(stream, consumer).fetch() instead)
  @Deprecated('Use consumer(stream, consumer).fetch() instead')
  Future<List<Message>> pull(String stream, String consumer,
      {int batch = 1, Duration timeout = const Duration(seconds: 2)}) {
    return _pull<dynamic>(stream, consumer, batch: batch, timeout: timeout);
  }

  /// Internal pull implementation
  Future<List<Message<T>>> _pull<T>(String stream, String consumer,
      {int batch = 1,
      Duration timeout = const Duration(seconds: 2),
      T Function(String)? jsonDecoder}) async {
    final inbox = client.inboxPrefix + '.' + Nuid().next();
    final sub = client.sub<T>(inbox, jsonDecoder: jsonDecoder);

    final subject = '\$JS.API.CONSUMER.MSG.NEXT.$stream.$consumer';
    final payload = utf8.encode(jsonEncode({
      'batch': batch,
      'expires': timeout.inMicroseconds * 1000, // nanoseconds
    }));

    // Publish the pull request pointing responses to our inbox
    client.pub(subject, Uint8List.fromList(payload), replyTo: inbox);

    final messages = <Message<T>>[];
    final completer = Completer<List<Message<T>>>();

    StreamSubscription<Message<T>>? streamSub;
    Timer? timer;

    void cleanup() {
      timer?.cancel();
      streamSub?.cancel();
      client.unSub(sub);
    }

    timer = Timer(timeout, () {
      cleanup();
      if (!completer.isCompleted) {
        completer.complete(messages);
      }
    });

    streamSub = sub.stream.listen((msg) {
      // Check if we got a NATS status header (e.g. 100 Idle Heartbeat, 404 No Messages, 408 Request Timeout)
      final statusHeader = msg.header?.status;
      if (statusHeader != null) {
        if (statusHeader >= 400) {
          cleanup();
          if (!completer.isCompleted) {
            completer.complete(messages);
          }
          return;
        }
        if (statusHeader >= 100 && statusHeader < 300) {
          // Idle Heartbeat or Flow Control
          // Under flow control, respond if replyTo is present
          if (msg.replyTo != null && msg.replyTo!.isNotEmpty) {
            client.pub(msg.replyTo!, Uint8List(0));
          }
          return;
        }
      }

      messages.add(msg);
      if (messages.length >= batch) {
        cleanup();
        if (!completer.isCompleted) {
          completer.complete(messages);
        }
      }
    }, onError: (err) {
      cleanup();
      if (!completer.isCompleted) {
        completer.completeError(err);
      }
    }, onDone: () {
      cleanup();
      if (!completer.isCompleted) {
        completer.complete(messages);
      }
    });

    return completer.future;
  }

  /// Create a new Key-Value bucket
  Future<KeyValue> createKeyValue(KeyValueConfig config) async {
    await createStream(config.toStreamConfig());
    return KeyValue(client, config.bucket);
  }

  /// Bind to an existing Key-Value bucket
  Future<KeyValue> keyValue(String bucket,
      {bool create = false, String? storage = 'file', int history = 1}) async {
    if (create) {
      final config = KeyValueConfig(
        bucket: bucket,
        storage: storage ?? 'file',
        history: history,
      );
      await createKeyValue(config);
    }
    return KeyValue(client, bucket);
  }

  /// Delete a Key-Value bucket
  Future<bool> deleteKeyValue(String bucket,
      {Duration timeout = const Duration(seconds: 2)}) {
    return deleteStream('KV_$bucket', timeout: timeout);
  }

  /// Create a new Object Store bucket
  Future<ObjectStore> createObjectStore(ObjectStoreConfig config) async {
    await createStream(config.toStreamConfig());
    return ObjectStore(client, config.bucket);
  }

  /// Bind to an existing Object Store bucket
  Future<ObjectStore> objectStore(String bucket,
      {bool create = false, String? storage = 'file'}) async {
    if (create) {
      final config = ObjectStoreConfig(
        bucket: bucket,
        storage: storage ?? 'file',
      );
      await createObjectStore(config);
    }
    return ObjectStore(client, bucket);
  }

  /// Delete an Object Store bucket
  Future<bool> deleteObjectStore(String bucket,
      {Duration timeout = const Duration(seconds: 2)}) {
    return deleteStream('OBJ_$bucket', timeout: timeout);
  }

  /// Get a specific message from a stream by its sequence number
  Future<Message> getMsg(String stream, int seq,
      {Duration timeout = const Duration(seconds: 2)}) async {
    final apiSubject = '\$JS.API.STREAM.MSG.GET.$stream';
    final payload = utf8.encode(jsonEncode({
      'seq': seq,
    }));
    final response = await client
        .request(apiSubject, Uint8List.fromList(payload), timeout: timeout);
    final map = jsonDecode(response.string);
    if (map['error'] != null) {
      throw NatsException(map['error']['description'] as String);
    }
    return _parseRawStreamMsg(stream, map['message'] as Map<String, dynamic>);
  }

  /// Get the last message in a stream for a specific subject
  Future<Message> getLastMsg(String stream, String subject,
      {Duration timeout = const Duration(seconds: 2)}) async {
    final apiSubject = '\$JS.API.STREAM.MSG.GET.$stream';
    final payload = utf8.encode(jsonEncode({
      'last_by_subj': subject,
    }));
    final response = await client
        .request(apiSubject, Uint8List.fromList(payload), timeout: timeout);
    final map = jsonDecode(response.string);
    if (map['error'] != null) {
      throw NatsException(map['error']['description'] as String);
    }
    return _parseRawStreamMsg(stream, map['message'] as Map<String, dynamic>);
  }

  Message _parseRawStreamMsg(String stream, Map<String, dynamic> jsonMsg) {
    final subject = jsonMsg['subject'] as String;
    final seq = jsonMsg['seq'] as int;
    final dataStr = jsonMsg['data'] as String? ?? '';
    final value = base64.decode(dataStr);

    Header? header;
    final hdrs = jsonMsg['hdrs'] as String?;
    if (hdrs != null && hdrs.isNotEmpty) {
      final decodedHdrs = base64.decode(hdrs);
      header = Header.fromBytes(decodedHdrs);
    }

    // Embed the sequence info into a dummy replyTo subject to support msg.streamSequence and msg.consumerSequence
    final dummyReply = '\$JS.ACK.$stream.dummy.1.$seq.$seq.0.0';

    return Message(subject, 0, value, client,
        replyTo: dummyReply, header: header);
  }

  /// Subscribe to a JetStream subject using a push consumer. (Deprecated: Use createConsumer and pull instead)
  @Deprecated('Use createConsumer and pull instead')
  Future<Subscription> subscribe(
    String subject, {
    String? stream,
    String? durable,
    String? queueGroup,
    bool manualAck = false,
    String deliverPolicy = 'all',
  }) async {
    String? targetStream = stream;
    if (targetStream == null) {
      final streams = await listStreams();
      for (final s in streams) {
        if (s.config.subjects.contains(subject)) {
          targetStream = s.config.name;
          break;
        }
      }
    }
    if (targetStream == null) {
      throw NatsException('Could not find a stream for subject: $subject');
    }

    final deliverSubject = client.inboxPrefix + '.' + Nuid().next();

    final consumerConfig = ConsumerConfig(
      durable: durable,
      deliverSubject: deliverSubject,
      filterSubject: subject,
      ackPolicy: manualAck ? 'explicit' : 'none',
      deliverPolicy: deliverPolicy,
    );

    await createConsumer(targetStream, consumerConfig);

    return client.sub(deliverSubject, queueGroup: queueGroup);
  }

  /// Create an Ordered Consumer on a stream.
  OrderedConsumer orderedConsumer(String stream, OrderedConsumerConfig config) {
    return OrderedConsumer(this, stream, config);
  }

  /// Get JetStream account usage information and statistics
  Future<AccountInfo> accountInfo(
      {Duration timeout = const Duration(seconds: 2)}) async {
    const subject = '\$JS.API.INFO';
    final response =
        await client.request(subject, Uint8List.fromList([]), timeout: timeout);
    final map = jsonDecode(response.string);
    if (map['error'] != null) {
      throw NatsException(map['error']['description'] as String);
    }
    return AccountInfo.fromJson(map as Map<String, dynamic>);
  }
}

/// Configuration options for an Ordered Consumer.
class OrderedConsumerConfig {
  /// Subject to filter messages from the stream
  final String? filterSubject;

  /// Starting point for message delivery: 'all', 'last', 'new', 'by_start_sequence', etc.
  final String deliverPolicy;

  /// Starting sequence number (used if deliverPolicy is 'by_start_sequence')
  final int? optStartSeq;

  /// Starting time (used if deliverPolicy is 'by_start_time')
  final DateTime? optStartTime;

  /// Constructor
  OrderedConsumerConfig({
    this.filterSubject,
    this.deliverPolicy = 'all',
    this.optStartSeq,
    this.optStartTime,
  });
}

/// Managed pull/push consumer providing ordered delivery guarantees.
class OrderedConsumer {
  /// The JetStream context
  final JetStream js;

  /// The Stream name
  final String stream;

  /// The configuration options
  final OrderedConsumerConfig config;

  int _lastSeq = 0;
  String? _currentConsumerName;
  StreamController<Message>? _controller;
  StreamSubscription? _sub;
  bool _active = true;

  /// Constructor for OrderedConsumer
  OrderedConsumer(this.js, this.stream, this.config) {
    if (config.deliverPolicy == 'by_start_sequence' &&
        config.optStartSeq != null) {
      _lastSeq = config.optStartSeq! - 1;
    }
  }

  /// Get a Stream of ordered messages.
  Stream<Message> messages() {
    _controller = StreamController<Message>(
      onCancel: () {
        stop();
      },
    );
    _start();
    return _controller!.stream;
  }

  /// Stop the Ordered Consumer and release NATS resources.
  void stop() {
    _active = false;
    _cleanup();
    if (_controller != null && !_controller!.isClosed) {
      _controller!.close();
    }
  }

  void _cleanup() {
    _sub?.cancel();
    if (_currentConsumerName != null) {
      final name = _currentConsumerName!;
      js.deleteConsumer(stream, name).catchError((_) => false);
      _currentConsumerName = null;
    }
  }

  Future<void> _start() async {
    int expectedConsumerSeq = 1;

    while (_active) {
      try {
        final name = 'OC_${Nuid().next()}';
        _currentConsumerName = name;

        final deliverSubject = js.client.inboxPrefix + '.' + Nuid().next();
        final coreSub = js.client.sub(deliverSubject);

        // Build consumer configuration
        final consumerConfig = ConsumerConfig(
          durable: null, // Ephemeral
          deliverSubject: deliverSubject,
          filterSubject: config.filterSubject,
          ackPolicy: 'none',
          deliverPolicy:
              _lastSeq == 0 ? config.deliverPolicy : 'by_start_sequence',
          optStartSeq: _lastSeq == 0 ? config.optStartSeq : _lastSeq + 1,
          flowControl: true,
          idleHeartbeat: const Duration(seconds: 5),
        );

        await js.createConsumer(stream, consumerConfig);

        expectedConsumerSeq = 1;
        final completer = Completer<void>();

        _sub = coreSub.stream.listen(
          (msg) {
            // Check for heartbeats (Status header >= 100)
            final status = msg.header?.status;
            if (status != null && status >= 100 && status < 300) {
              // Idle heartbeat or flow control
              // Under flow control, respond if replyTo is present
              if (msg.replyTo != null && msg.replyTo!.isNotEmpty) {
                js.client.pub(msg.replyTo!, Uint8List(0));
              }
              return;
            }

            final seq = msg.streamSequence;
            final consSeq = msg.consumerSequence;

            if (seq == null || consSeq == null) return;

            // Check if there is a gap in the consumer sequence
            if (consSeq != expectedConsumerSeq) {
              // Sequence gap detected! Reset consumer.
              _sub?.cancel();
              js.deleteConsumer(stream, name).catchError((_) => false);
              completer.complete();
              return;
            }

            _lastSeq = seq;
            expectedConsumerSeq++;

            if (_controller != null && !_controller!.isClosed) {
              _controller!.add(msg);
            }
          },
          onError: (err) {
            _sub?.cancel();
            js.deleteConsumer(stream, name).catchError((_) => false);
            if (!completer.isCompleted) completer.complete();
          },
          onDone: () {
            _sub?.cancel();
            js.deleteConsumer(stream, name).catchError((_) => false);
            if (!completer.isCompleted) completer.complete();
          },
        );

        // Wait until consumer resets or closes
        await completer.future;
      } catch (e) {
        // Surface the failure to listeners (e.g. createConsumer rejected the
        // config -- a workqueue-retention stream requires explicit ack,
        // for example) instead of retrying silently forever, then back off
        // to avoid a tight loop.
        if (_controller != null && !_controller!.isClosed) {
          _controller!.addError(e);
        }
        await Future.delayed(const Duration(seconds: 1));
      }
    }
  }
}

/// NATS JetStream Stream handle wrapping operations for a specific stream.
class JsStream {
  /// Reference to the JetStream context
  final JetStream js;

  /// Name of the stream
  final String name;

  /// Constructor for JsStream
  JsStream(this.js, this.name);

  /// Get details/status of this stream
  Future<StreamInfo> info({Duration timeout = const Duration(seconds: 2)}) {
    return js.streamInfo(name, timeout: timeout);
  }

  /// Purge messages from this stream. With no arguments, purges everything.
  /// [filter] limits the purge to a matching subject, [keep] retains the
  /// newest N messages instead of purging all matches, and [seq] purges
  /// everything up to but not including that sequence number ([keep] and
  /// [seq] are mutually exclusive per the server's own API).
  Future<bool> purge(
      {String? filter,
      int? keep,
      int? seq,
      Duration timeout = const Duration(seconds: 2)}) {
    return js._purgeStream(name,
        filter: filter, keep: keep, seq: seq, timeout: timeout);
  }

  /// Bind to a consumer on this stream
  Consumer<T> consumer<T>(String consumerName,
      {T Function(String)? jsonDecoder}) {
    return js.consumer<T>(name, consumerName, jsonDecoder: jsonDecoder);
  }

  /// Create a consumer on this stream
  Future<Consumer<T>> createConsumer<T>(ConsumerConfig config,
      {Duration timeout = const Duration(seconds: 2),
      T Function(String)? jsonDecoder}) {
    return js.createConsumer<T>(name, config,
        timeout: timeout, jsonDecoder: jsonDecoder);
  }
}

/// NATS JetStream Consumer handle wrapping operations for a specific consumer.
class Consumer<T> {
  /// Reference to the JetStream context
  final JetStream js;

  /// Name of the stream bound to this consumer
  final String streamName;

  /// Name of the consumer
  final String name;

  /// Optional JSON decoder for generic type T
  final T Function(String)? jsonDecoder;

  /// Constructor for Consumer
  Consumer(this.js, this.streamName, this.name, {this.jsonDecoder});

  /// Get status of this consumer
  Future<ConsumerInfo> info({Duration timeout = const Duration(seconds: 2)}) {
    return js.consumerInfo(streamName, name, timeout: timeout);
  }

  /// Pull a batch of messages from this pull consumer
  Future<List<Message<T>>> fetch(
      {int batch = 1, Duration timeout = const Duration(seconds: 2)}) {
    return js._pull<T>(streamName, name,
        batch: batch, timeout: timeout, jsonDecoder: jsonDecoder);
  }

  /// Consume messages continuously from this consumer.
  ///
  /// Under the hood, if the consumer is configured as a push consumer (has a deliver subject),
  /// it subscribes to that deliver subject.
  /// If it is configured as a pull consumer, it continuously polls/fetches messages in the background.
  ///
  /// [batch] specifies the number of messages to pull per fetch request (only applicable for pull consumers).
  /// [timeout] specifies the expiration timeout for each pull request (only applicable for pull consumers).
  Stream<Message<T>> messages(
      {int batch = 1, Duration timeout = const Duration(seconds: 5)}) {
    late StreamController<Message<T>> controller;
    StreamSubscription<Message<T>>? pushSub;
    bool active = true;
    bool isPaused = false;

    void stop() {
      active = false;
      pushSub?.cancel();
      if (!controller.isClosed) {
        controller.close();
      }
    }

    Future<void> startPullLoop() async {
      while (active && !controller.isClosed) {
        if (isPaused) {
          await Future.delayed(const Duration(milliseconds: 100));
          continue;
        }
        try {
          final msgs = await fetch(batch: batch, timeout: timeout);
          if (msgs.isEmpty) {
            // An empty batch is the normal "nothing new yet" outcome, but
            // it's also indistinguishable from a consumer that was deleted
            // server-side: the pull request has no responder left to answer
            // it, so it silently times out with no error and no status
            // message either, exactly like a healthy idle consumer would.
            // Disambiguate by confirming the consumer still exists whenever
            // a batch comes back empty, so a deleted consumer is detected
            // within one fetch cycle instead of polling forever with
            // nothing ever surfaced.
            await info(timeout: timeout);
          }
          for (final msg in msgs) {
            if (!active || controller.isClosed) break;
            controller.add(msg);
          }
        } catch (e) {
          // Surface the failure to listeners (e.g. the consumer was deleted
          // server-side) instead of retrying silently forever, then back off
          // to avoid a tight loop.
          if (!controller.isClosed) controller.addError(e);
          await Future.delayed(const Duration(seconds: 1));
        }
      }
    }

    controller = StreamController<Message<T>>(
      onListen: () async {
        try {
          final consumerInfo = await info(timeout: timeout);
          if (!active || controller.isClosed) return;

          final deliverSubject = consumerInfo.config.deliverSubject;
          if (deliverSubject != null && deliverSubject.isNotEmpty) {
            // Push Consumer: Subscribe to the deliver subject
            final sub =
                js.client.sub<T>(deliverSubject, jsonDecoder: jsonDecoder);
            pushSub = sub.stream.listen(
              (msg) {
                if (!controller.isClosed) {
                  controller.add(msg);
                }
              },
              onError: (err) {
                if (!controller.isClosed) controller.addError(err);
              },
              onDone: stop,
            );
          } else {
            // Pull Consumer: Start pull loop in background
            startPullLoop();
          }
        } catch (e) {
          if (!controller.isClosed) {
            controller.addError(e);
            stop();
          }
        }
      },
      onCancel: stop,
      onPause: () {
        isPaused = true;
        pushSub?.pause();
      },
      onResume: () {
        isPaused = false;
        pushSub?.resume();
      },
    );

    return controller.stream;
  }
}

/// JetStream Tier resource usage
class Tier {
  /// Memory resource usage
  final int memory;

  /// Storage resource usage
  final int storage;

  /// Reserved memory limit
  final int reservedMemory;

  /// Reserved storage limit
  final int reservedStorage;

  /// Number of streams
  final int streams;

  /// Number of consumers
  final int consumers;

  /// Constructor for Tier
  Tier({
    required this.memory,
    required this.storage,
    required this.reservedMemory,
    required this.reservedStorage,
    required this.streams,
    required this.consumers,
  });

  /// Factory from JSON map
  factory Tier.fromJson(Map<String, dynamic> json) {
    return Tier(
      // Read as `num` rather than `int`: a server represents an unset
      // reserved limit as a uint64 `-1` ("no limit") sentinel, which after
      // JSON round-tripping through a double can arrive as e.g.
      // `18446744073709552000.0` — a strict `as int?` cast throws on that.
      memory: (json['memory'] as num?)?.toInt() ?? 0,
      storage: (json['storage'] as num?)?.toInt() ?? 0,
      reservedMemory: (json['reserved_memory'] as num?)?.toInt() ?? 0,
      reservedStorage: (json['reserved_storage'] as num?)?.toInt() ?? 0,
      streams: (json['streams'] as num?)?.toInt() ?? 0,
      consumers: (json['consumers'] as num?)?.toInt() ?? 0,
    );
  }
}

/// JetStream API usage statistics for an account
class APIStats {
  /// API limits level
  final int level;

  /// Total requests count
  final int total;

  /// Total errors count
  final int errors;

  /// Current inflight requests
  final int inflight;

  /// Constructor for APIStats
  APIStats({
    required this.level,
    required this.total,
    required this.errors,
    required this.inflight,
  });

  /// Factory from JSON map
  factory APIStats.fromJson(Map<String, dynamic> json) {
    return APIStats(
      level: json['level'] as int? ?? 0,
      total: json['total'] as int? ?? 0,
      errors: json['errors'] as int? ?? 0,
      inflight: json['inflight'] as int? ?? 0,
    );
  }
}

/// JetStream account information
class AccountInfo {
  /// JetStream domain
  final String domain;

  /// API statistics
  final APIStats api;

  /// Tier resource usage
  final Tier tier;

  /// Multi-tier configurations mapped by tier name
  final Map<String, Tier> tiers;

  /// Constructor for AccountInfo
  AccountInfo({
    required this.domain,
    required this.api,
    required this.tier,
    required this.tiers,
  });

  /// Factory from JSON map
  factory AccountInfo.fromJson(Map<String, dynamic> json) {
    final tiersMap = <String, Tier>{};
    if (json['tiers'] != null) {
      (json['tiers'] as Map<String, dynamic>).forEach((key, value) {
        tiersMap[key] = Tier.fromJson(value as Map<String, dynamic>);
      });
    }
    return AccountInfo(
      domain: json['domain'] as String? ?? '',
      api: APIStats.fromJson(json['api'] as Map<String, dynamic>? ?? {}),
      // The `$JS.API.INFO` response has no nested `tier` key — a server puts
      // an account's own usage/limits (`memory`, `storage`, `streams`,
      // `consumers`, `reserved_memory`, `reserved_storage`) at the top
      // level, so `Tier.fromJson(json['tier'])` was always parsing an empty
      // map and silently returning an all-zero `Tier`. Verified against a
      // live server: `Tier.fromJson(json)` reads the real fields directly,
      // and stays correct for multi-tier accounts too, since `json` there
      // still carries the account-wide aggregate alongside `tiers`.
      tier: Tier.fromJson(json),
      tiers: tiersMap,
    );
  }
}
