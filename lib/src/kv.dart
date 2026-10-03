import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'client.dart';
import 'common.dart';
import 'message.dart';
import 'jetstream.dart';
import 'inbox.dart';

/// KeyValue Configuration
class KeyValueConfig {
  /// Name of the key-value bucket
  final String bucket;

  /// Description of the key-value bucket
  final String description;

  /// Storage type: 'file' or 'memory'
  final String storage;

  /// History depth (maximum historical values kept per key)
  final int history;

  /// Maximum size of the bucket in bytes
  final int maxBytes;

  /// Time to live for values in the bucket
  final Duration ttl;

  /// Constructor for KeyValueConfig
  KeyValueConfig({
    required this.bucket,
    this.description = '',
    this.storage = 'file',
    this.history = 1,
    this.maxBytes = -1,
    this.ttl = Duration.zero,
  });

  /// Convert to JetStream StreamConfig
  StreamConfig toStreamConfig() {
    return StreamConfig(
      name: 'KV_$bucket',
      subjects: ['\$KV.$bucket.>'],
      storage: storage,
      maxMsgs: -1,
      maxBytes: maxBytes,
      allowRollup: true,
      discard: 'new',
      allowDirect: true,
      denyDelete: true,
      maxMsgsPerSubject: history,
    );
  }
}

/// KeyValue operations
enum KeyValueOp {
  /// Put operation (set/update value)
  put,

  /// Delete operation (add tombstone)
  delete,

  /// Purge operation (delete all key history)
  purge,
}

/// KeyValue Entry holding value and metadata revision details
class KeyValueEntry {
  /// Name of the bucket containing entry
  final String bucket;

  /// The entry key
  final String key;

  /// Byte payload value
  final Uint8List value;

  /// Revision number in the backing stream
  final int revision;

  /// Created timestamp
  final DateTime created;

  /// Sequence delta number
  final int delta;

  /// Operation type performed
  final KeyValueOp op;

  /// Constructor for KeyValueEntry
  KeyValueEntry({
    required this.key,
    required this.value,
    required this.revision,
    required this.created,
    this.bucket = '',
    this.delta = 0,
    this.op = KeyValueOp.put,
  });

  /// Get value payload as string
  String get string => utf8.decode(value);
}

/// EXPERIMENTAL: Key-Value Store APIs are experimental and subject to change in future releases.
///
/// NATS Key-Value Store implementation
class KeyValue {
  /// Reference to the core NATS Client
  final Client client;

  /// Name of the bucket
  final String bucket;

  /// Backing stream name
  final String streamName;

  /// Constructor for KeyValue store
  KeyValue(this.client, this.bucket) : streamName = 'KV_$bucket';

  /// Associate a value with a key
  /// Store a binary value under the given [key]. Returns the sequence number of the stored message.
  Future<int> put(String key, Uint8List value) async {
    final subject = '\$KV.$bucket.$key';
    final ack = await client.jetStream().publish(subject, value);
    return ack.sequence;
  }

  /// Associate a string value with a key
  /// Store a string value under the given [key] by encoding it as UTF‑8.
  Future<int> putString(String key, String value) {
    return put(key, Uint8List.fromList(utf8.encode(value)));
  }

  /// Retrieve the latest entry associated with a key
  Future<KeyValueEntry?> get(String key) async {
    final entry = await _getRaw(key);
    if (entry == null ||
        entry.op == KeyValueOp.delete ||
        entry.op == KeyValueOp.purge) {
      return null;
    }
    return entry;
  }

  /// Create a key with the given value only if it does not exist
  Future<int> create(String key, Uint8List value) async {
    try {
      final subject = '\$KV.$bucket.$key';
      final ack = await client.jetStream().publish(
            subject,
            value,
            opts: PubOpts(expectLastSubjectSeq: 0),
          );
      return ack.sequence;
    } catch (e) {
      final entry = await _getRaw(key);
      if (entry != null &&
          (entry.op == KeyValueOp.delete || entry.op == KeyValueOp.purge)) {
        try {
          final subject = '\$KV.$bucket.$key';
          final ack = await client.jetStream().publish(
                subject,
                value,
                opts: PubOpts(expectLastSubjectSeq: entry.revision),
              );
          return ack.sequence;
        } catch (_) {}
      }
      throw NatsException('key already exists');
    }
  }

  /// Create a key with the given string value only if it does not exist
  Future<int> createString(String key, String value) {
    return create(key, Uint8List.fromList(utf8.encode(value)));
  }

  /// Update the value for the key only if the current revision matches
  Future<int> update(String key, Uint8List value, int revision) async {
    final subject = '\$KV.$bucket.$key';
    final ack = await client.jetStream().publish(
          subject,
          value,
          opts: PubOpts(expectLastSubjectSeq: revision),
        );
    return ack.sequence;
  }

  /// Update the string value for the key only if the current revision matches
  Future<int> updateString(String key, String value, int revision) {
    return update(key, Uint8List.fromList(utf8.encode(value)), revision);
  }

  /// Retrieve a specific revision associated with a key
  Future<KeyValueEntry?> getRevision(String key, int revision) async {
    final apiSubject = '\$JS.API.STREAM.MSG.GET.$streamName';
    final payload = utf8.encode(jsonEncode({
      'seq': revision,
    }));

    try {
      final response =
          await client.request(apiSubject, Uint8List.fromList(payload));
      final map = jsonDecode(response.string);
      if (map['error'] != null) {
        if (map['error']['code'] == 404) {
          return null; // Revision not found
        }
        throw NatsException(map['error']['description'] as String);
      }
      final msgMap = map['message'] as Map<String, dynamic>;
      final msgSubject = msgMap['subject'] as String?;
      if (msgSubject != '\$KV.$bucket.$key') {
        return null; // Subject mismatch (revision corresponds to another key)
      }
      final entry = _parseEntryRaw(key, msgMap);
      if (entry == null ||
          entry.op == KeyValueOp.delete ||
          entry.op == KeyValueOp.purge) {
        return null;
      }
      return entry;
    } catch (e) {
      if (e is TimeoutException || e is NatsNoRespondersException) {
        return null;
      }
      rethrow;
    }
  }

  Future<KeyValueEntry?> _getRaw(String key) async {
    final apiSubject = '\$JS.API.STREAM.MSG.GET.$streamName';
    final payload = utf8.encode(jsonEncode({
      'last_by_subj': '\$KV.$bucket.$key',
    }));

    try {
      final response =
          await client.request(apiSubject, Uint8List.fromList(payload));
      final map = jsonDecode(response.string);
      if (map['error'] != null) {
        if (map['error']['code'] == 404) {
          return null; // Key not found
        }
        throw NatsException(map['error']['description'] as String);
      }
      return _parseEntryRaw(key, map['message'] as Map<String, dynamic>);
    } catch (e) {
      if (e is TimeoutException || e is NatsNoRespondersException) {
        return null;
      }
      rethrow;
    }
  }

  /// Delete the key (adds a deletion tombstone message to history)
  /// Delete the entry for [key] by publishing a tombstone message.
  Future<bool> delete(String key) async {
    final subject = '\$KV.$bucket.$key';
    final header = Header().add('KV-Operation', 'DEL');
    final ack =
        await client.jetStream().publish(subject, Uint8List(0), header: header);
    return ack.sequence > 0;
  }

  /// Purge the key (deletes all historical versions for this key)
  /// Purge all historical versions of [key] from the stream.
  Future<bool> purge(String key) async {
    final subject = '\$KV.$bucket.$key';
    final header =
        Header().add('KV-Operation', 'PURGE').add('Nats-Rollup', 'sub');
    final ack =
        await client.jetStream().publish(subject, Uint8List(0), header: header);
    return ack.sequence > 0;
  }

  /// Watch real‑time modifications for [key] (supports wild‑cards). If [includeHistory] is true, emits past entries as well.
  Stream<KeyValueEntry?> watch(
      {String key = '>', bool includeHistory = false}) {
    final controller = StreamController<KeyValueEntry?>();
    final deliverSubject = client.inboxPrefix + '.' + Nuid().next();
    final sub = client.sub(deliverSubject);

    final streamSub = sub.stream.listen((msg) {
      try {
        final parts = msg.subject!.split('.');
        if (parts.length < 3) return;
        final keyName = parts.sublist(2).join('.');

        Header? header = msg.header;
        final op = header?.get('KV-Operation');
        KeyValueOp kvOp = KeyValueOp.put;
        if (op == 'DEL') {
          kvOp = KeyValueOp.delete;
        } else if (op == 'PURGE') {
          kvOp = KeyValueOp.purge;
        }

        controller.add(KeyValueEntry(
          bucket: bucket,
          key: keyName,
          value: kvOp == KeyValueOp.put ? msg.byte : Uint8List(0),
          revision: msg.streamSequence ?? 0,
          created: DateTime.now(),
          op: kvOp,
        ));
      } catch (e) {
        controller.addError(e);
      }
    }, onError: (err) {
      controller.addError(err);
    }, onDone: () {
      controller.close();
    });

    final consumerConfig = ConsumerConfig(
      deliverSubject: deliverSubject,
      filterSubject: '\$KV.$bucket.$key',
      deliverPolicy: includeHistory ? 'all' : 'last',
      ackPolicy: 'none',
      // Safety net if `onCancel`'s own deleteConsumer below never runs
      // (e.g. the client vanishes uncleanly instead of cancelling the
      // stream) -- without this, an ephemeral consumer with no durable
      // name and no inactivity threshold is never reaped by the server on
      // its own and leaks indefinitely.
      inactiveThreshold: const Duration(minutes: 5),
    );

    // Guards against a race between this create() call landing and the
    // caller cancelling almost immediately: if onCancel runs first, it has
    // no consumer name yet to delete, and nothing would call it a second
    // time once create() finally resolves -- `cancelled` lets the create()
    // continuation notice that and delete the consumer itself instead of
    // leaking it for the full inactiveThreshold window.
    Consumer<dynamic>? ephemeralConsumer;
    bool cancelled = false;
    client
        .jetStream()
        .createConsumer(streamName, consumerConfig)
        .then((consumer) {
      if (cancelled) {
        client
            .jetStream()
            .deleteConsumer(streamName, consumer.name)
            .catchError((_) => false);
      } else {
        ephemeralConsumer = consumer;
      }
    }).catchError((dynamic err) {
      controller.addError(err);
      controller.close();
      throw err;
    });

    controller.onCancel = () async {
      cancelled = true;
      streamSub.cancel();
      client.unSub(sub);
      final consumer = ephemeralConsumer;
      if (consumer != null) {
        try {
          await client.jetStream().deleteConsumer(streamName, consumer.name);
        } catch (_) {
          // Best-effort: the inactiveThreshold above still reaps it
          // eventually if this fails (e.g. already disconnected).
        }
      }
    };

    return controller.stream;
  }

  KeyValueEntry? _parseEntryRaw(String key, Map<String, dynamic> jsonMsg) {
    final seq = jsonMsg['seq'] as int;
    final timeStr = jsonMsg['time'] as String;
    final created = DateTime.tryParse(timeStr) ?? DateTime.now();

    Header? header;
    final hdrs = jsonMsg['hdrs'] as String?;
    if (hdrs != null && hdrs.isNotEmpty) {
      final decodedHdrs = base64.decode(hdrs);
      header = Header.fromBytes(decodedHdrs);
    }

    KeyValueOp op = KeyValueOp.put;
    if (header != null) {
      final kvOp = header.get('KV-Operation');
      if (kvOp == 'DEL') {
        op = KeyValueOp.delete;
      } else if (kvOp == 'PURGE') {
        op = KeyValueOp.purge;
      }
    }

    final data = jsonMsg['data'] as String? ?? '';
    final value = base64.decode(data);

    return KeyValueEntry(
      bucket: bucket,
      key: key,
      value: value,
      revision: seq,
      created: created,
      op: op,
    );
  }

  /// List all active keys in this bucket (excluding deleted/purged ones).
  Future<List<String>> keys(
      {Duration timeout = const Duration(seconds: 5)}) async {
    final deliverSubject = client.inboxPrefix + '.' + Nuid().next();
    final sub = client.sub(deliverSubject);

    final consumerConfig = ConsumerConfig(
      deliverSubject: deliverSubject,
      filterSubject: '\$KV.$bucket.>',
      deliverPolicy: 'all',
      ackPolicy: 'none',
      // Safety net if cleanup()'s own deleteConsumer call below never runs.
      inactiveThreshold: const Duration(minutes: 5),
    );

    final activeKeys = <String, bool>{}; // key -> is_active
    final completer = Completer<List<String>>();
    StreamSubscription? streamSub;
    Timer? timeoutTimer;
    Consumer<dynamic>? ephemeralConsumer;
    // Guards against cleanup() running (via the pending-count-reaches-zero
    // fast path below, which can win the race against createConsumer()'s
    // own reply on a small/already-delivered result set) before
    // ephemeralConsumer is set -- without this, cleanup() would have
    // nothing to delete yet, and nothing would call it a second time once
    // createConsumer() finally resolves, leaking the consumer for the full
    // inactiveThreshold window instead of deleting it immediately.
    bool cleanedUp = false;

    void cleanup() {
      if (cleanedUp) return;
      cleanedUp = true;
      timeoutTimer?.cancel();
      streamSub?.cancel();
      client.unSub(sub);
      final consumer = ephemeralConsumer;
      if (consumer != null) {
        client
            .jetStream()
            .deleteConsumer(streamName, consumer.name)
            .catchError((_) => false);
      }
    }

    timeoutTimer = Timer(timeout, () {
      cleanup();
      if (!completer.isCompleted) {
        completer.complete(activeKeys.entries
            .where((e) => e.value)
            .map((e) => e.key)
            .toList());
      }
    });

    streamSub = sub.stream.listen((msg) {
      try {
        final parts = msg.subject!.split('.');
        if (parts.length >= 3) {
          final keyName = parts.sublist(2).join('.');
          final op = msg.header?.get('KV-Operation');
          if (op == 'DEL' || op == 'PURGE') {
            activeKeys[keyName] = false;
          } else {
            activeKeys[keyName] = true;
          }
        }
      } catch (_) {}

      final reply = msg.replyTo;
      if (reply != null) {
        final parts = reply.split('.');
        int? pending;
        if (parts.length == 9) {
          pending = int.tryParse(parts[8]);
        } else if (parts.length == 11) {
          pending = int.tryParse(parts[10]);
        }
        if (pending == 0) {
          cleanup();
          if (!completer.isCompleted) {
            completer.complete(activeKeys.entries
                .where((e) => e.value)
                .map((e) => e.key)
                .toList());
          }
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
        completer.complete(activeKeys.entries
            .where((e) => e.value)
            .map((e) => e.key)
            .toList());
      }
    });

    try {
      final consumer =
          await client.jetStream().createConsumer(streamName, consumerConfig);
      if (cleanedUp) {
        client
            .jetStream()
            .deleteConsumer(streamName, consumer.name)
            .catchError((_) => false);
      } else {
        ephemeralConsumer = consumer;
      }
    } catch (e) {
      cleanup();
      if (!completer.isCompleted) {
        completer.complete([]);
      }
    }

    return completer.future;
  }

  /// Get a stream of all revisions (history) for a specific key.
  /// The stream will yield historical entries and automatically close when the existing history is fully read.
  Stream<KeyValueEntry> history(String key,
      {Duration timeout = const Duration(seconds: 5)}) {
    final controller = StreamController<KeyValueEntry>();
    final deliverSubject = client.inboxPrefix + '.' + Nuid().next();
    final sub = client.sub(deliverSubject);

    StreamSubscription? streamSub;
    Timer? timeoutTimer;
    Consumer<dynamic>? ephemeralConsumer;
    // Guards against cleanup() running (e.g. via the pending-count-reaches-
    // zero fast path below) before createConsumer()'s reply has landed and
    // ephemeralConsumer is set -- without this, cleanup() would have
    // nothing to delete yet, and nothing would call it a second time once
    // the reply finally arrives, leaking the consumer for the full
    // inactiveThreshold window instead of deleting it immediately.
    bool cleanedUp = false;

    void cleanup() {
      if (cleanedUp) return;
      cleanedUp = true;
      timeoutTimer?.cancel();
      streamSub?.cancel();
      client.unSub(sub);
      if (!controller.isClosed) {
        controller.close();
      }
      final consumer = ephemeralConsumer;
      if (consumer != null) {
        client
            .jetStream()
            .deleteConsumer(streamName, consumer.name)
            .catchError((_) => false);
      }
    }

    timeoutTimer = Timer(timeout, () {
      cleanup();
    });

    streamSub = sub.stream.listen((msg) {
      try {
        final parts = msg.subject!.split('.');
        if (parts.length >= 3) {
          final keyName = parts.sublist(2).join('.');
          final seq = msg.streamSequence ?? 0;

          Header? header = msg.header;
          final op = header?.get('KV-Operation');
          KeyValueOp kvOp = KeyValueOp.put;
          if (op == 'DEL') {
            kvOp = KeyValueOp.delete;
          } else if (op == 'PURGE') {
            kvOp = KeyValueOp.purge;
          }

          controller.add(KeyValueEntry(
            bucket: bucket,
            key: keyName,
            value: kvOp == KeyValueOp.put ? msg.byte : Uint8List(0),
            revision: seq,
            created: DateTime.now(),
            op: kvOp,
          ));
        }
      } catch (e) {
        controller.addError(e);
      }

      final reply = msg.replyTo;
      if (reply != null) {
        final parts = reply.split('.');
        int? pending;
        if (parts.length == 9) {
          pending = int.tryParse(parts[8]);
        } else if (parts.length == 11) {
          pending = int.tryParse(parts[10]);
        }
        if (pending == 0) {
          cleanup();
        }
      }
    }, onError: (err) {
      controller.addError(err);
      cleanup();
    }, onDone: () {
      cleanup();
    });

    client
        .jetStream()
        .createConsumer(
            streamName,
            ConsumerConfig(
              deliverSubject: deliverSubject,
              filterSubject: '\$KV.$bucket.$key',
              deliverPolicy: 'all',
              ackPolicy: 'none',
              // Safety net if cleanup()'s own deleteConsumer call above
              // never runs.
              inactiveThreshold: const Duration(minutes: 5),
            ))
        .then((consumer) {
      if (cleanedUp) {
        client
            .jetStream()
            .deleteConsumer(streamName, consumer.name)
            .catchError((_) => false);
      } else {
        ephemeralConsumer = consumer;
      }
    }).catchError((dynamic err) {
      controller.addError(err);
      cleanup();
      throw err;
    });

    return controller.stream;
  }

  /// Get status details for this Key-Value bucket.
  Future<KeyValueStatus> status() async {
    final info = await client.jetStream().streamInfo(streamName);
    return KeyValueStatus(bucket, info);
  }
}

/// Represents status and statistics of a Key-Value bucket.
class KeyValueStatus {
  /// The bucket name
  final String bucket;

  /// The backing stream information
  final StreamInfo info;

  /// Constructor
  KeyValueStatus(this.bucket, this.info);

  /// Storage type: 'file' or 'memory'
  String get storage => info.config.storage;

  /// The history depth configuration (max messages per subject)
  int get history => info.config.maxMsgsPerSubject ?? 1;

  /// Total number of stored bytes (compressed/raw size in NATS)
  int get size => info.state.bytes;

  /// Total count of operations/messages currently in the backing stream
  int get values => info.state.messages;
}
