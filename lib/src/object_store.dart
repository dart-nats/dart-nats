import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:crypto/crypto.dart';

import 'client.dart';
import 'common.dart';
import 'jetstream.dart';
import 'inbox.dart';

/// Captures the single [Digest] event a [Hash.startChunkedConversion] sink
/// produces on [close], so a SHA-256 digest can be computed incrementally
/// over a stream of chunks without buffering the whole payload in memory.
class _DigestSink implements Sink<Digest> {
  Digest? value;

  @override
  void add(Digest data) => value = data;

  @override
  void close() {}
}

/// Represents a link to another object or bucket in the Object Store.
class ObjectLink {
  /// The bucket name the link points to
  final String bucket;

  /// The object name (null if linking to the entire bucket)
  final String? name;

  /// Constructor
  ObjectLink({required this.bucket, this.name});

  /// Factory from JSON map
  factory ObjectLink.fromJson(Map<String, dynamic> json) {
    return ObjectLink(
      bucket: json['bucket'] as String? ?? '',
      name: json['name'] as String?,
    );
  }

  /// Export to JSON map
  Map<String, dynamic> toJson() {
    final map = <String, dynamic>{
      'bucket': bucket,
    };
    if (name != null && name!.isNotEmpty) {
      map['name'] = name;
    }
    return map;
  }
}

/// Object Store Metadata Information
class ObjectInfo {
  /// Name of the object
  final String name;

  /// Optional description
  final String description;

  /// Name of the bucket
  final String bucket;

  /// Unique NUID identifier of the object
  final String nuid;

  /// Size of the object in bytes
  final int size;

  /// Modification timestamp
  final DateTime mtime;

  /// Number of chunks the object is split into
  final int chunks;

  /// SHA-256 digest of the full object payload
  final String digest;

  /// Whether the object is marked as deleted (tombstoned)
  final bool deleted;

  /// Link mapping if this object is a link to another object/bucket
  final ObjectLink? link;

  /// Constructor for ObjectInfo
  ObjectInfo({
    required this.name,
    this.description = '',
    required this.bucket,
    required this.nuid,
    required this.size,
    required this.mtime,
    required this.chunks,
    required this.digest,
    this.deleted = false,
    this.link,
  });

  /// Factory from JSON map
  factory ObjectInfo.fromJson(Map<String, dynamic> json) {
    final opts = json['options'] as Map<String, dynamic>?;
    ObjectLink? link;
    if (opts != null && opts['link'] != null) {
      link = ObjectLink.fromJson(opts['link'] as Map<String, dynamic>);
    }

    return ObjectInfo(
      name: json['name'] as String? ?? '',
      description: json['description'] as String? ?? '',
      bucket: json['bucket'] as String? ?? '',
      nuid: json['nuid'] as String? ?? '',
      size: json['size'] as int? ?? 0,
      mtime:
          DateTime.tryParse(json['mtime'] as String? ?? '') ?? DateTime.now(),
      chunks: json['chunks'] as int? ?? 0,
      digest: json['digest'] as String? ?? '',
      deleted: json['deleted'] as bool? ?? false,
      link: link,
    );
  }

  /// Export metadata to JSON map
  Map<String, dynamic> toJson() {
    final map = <String, dynamic>{
      'name': name,
      'description': description,
      'bucket': bucket,
      'nuid': nuid,
      'size': size,
      'mtime': mtime.toUtc().toIso8601String(),
      'chunks': chunks,
      'digest': digest,
      'deleted': deleted,
    };
    if (link != null) {
      map['options'] = {
        'link': link!.toJson(),
      };
    }
    return map;
  }
}

/// Object Store Configuration
class ObjectStoreConfig {
  /// Name of the object store bucket
  final String bucket;

  /// Description of the object store bucket
  final String description;

  /// Storage type: 'file' or 'memory'
  final String storage;

  /// Number of replicas for the backing stream
  final int replicas;

  /// Maximum size of the bucket in bytes
  final int maxBytes;

  /// Time to live for objects in the bucket
  final Duration ttl;

  /// Constructor for ObjectStoreConfig
  ObjectStoreConfig({
    required this.bucket,
    this.description = '',
    this.storage = 'file',
    this.replicas = 1,
    this.maxBytes = -1,
    this.ttl = Duration.zero,
  });

  /// Convert to StreamConfig
  StreamConfig toStreamConfig() {
    return StreamConfig(
      name: 'OBJ_$bucket',
      subjects: ['\$O.$bucket.>'],
      storage: storage,
      maxBytes: maxBytes,
      maxAge: ttl,
      numReplicas: replicas,
      allowRollup: true,
      discard: 'new',
    );
  }
}

/// EXPERIMENTAL: Object Store APIs are experimental and subject to change in future releases.
///
/// NATS Object Store implementation
class ObjectStore {
  /// The NATS Client instance
  final Client client;

  /// The Object Store bucket name
  final String bucket;

  /// The JetStream stream name backing this Object Store
  final String streamName;

  /// The default chunk size (128 KiB)
  static const int defaultChunkSize = 128 * 1024; // 128 KiB

  /// Create a new ObjectStore instance
  ObjectStore(this.client, this.bucket) : streamName = 'OBJ_$bucket';

  /// Store an object in the bucket. If [name] already has an object stored
  /// under it, the previous version's chunks are purged from the backing
  /// stream once the new object is safely written, so overwriting an object
  /// no longer leaves its old chunks orphaned server-side.
  Future<ObjectInfo> put(String name, Uint8List data,
      {String description = ''}) async {
    final previous = await getInfo(name);
    final nuid = Nuid().next();
    final totalSize = data.length;

    // Chunking the data
    final chunks = <Uint8List>[];
    var offset = 0;
    while (offset < totalSize) {
      var end = offset + defaultChunkSize;
      if (end > totalSize) {
        end = totalSize;
      }
      chunks.add(data.sublist(offset, end));
      offset = end;
    }

    // Publish all data chunks
    for (var i = 0; i < chunks.length; i++) {
      final chunkSubject = '\$O.$bucket.C.$nuid';
      await client.pub(chunkSubject, chunks[i]);
    }

    // Ensure all chunks are flushed to the server
    await client.flush();

    // Compute digest and create metadata
    final hash = sha256.convert(data);
    final digest = 'SHA-256=${base64Url.encode(hash.bytes)}';

    final info = ObjectInfo(
      name: name,
      description: description,
      bucket: bucket,
      nuid: nuid,
      size: totalSize,
      mtime: DateTime.now(),
      chunks: chunks.length,
      digest: digest,
    );

    // Save metadata
    final encodedName = base64Url.encode(utf8.encode(name));
    final metadataSubject = '\$O.$bucket.M.$encodedName';
    final payload = utf8.encode(jsonEncode(info.toJson()));

    await client
        .jetStream()
        .publish(metadataSubject, Uint8List.fromList(payload));

    if (previous != null && previous.chunks > 0 && previous.nuid != nuid) {
      await _purgeChunks(previous.nuid);
    }

    return info;
  }

  /// Store a byte payload as an object
  Future<ObjectInfo> putBytes(String name, Uint8List data,
      {String description = ''}) {
    return put(name, data, description: description);
  }

  /// Store an object from a stream of byte chunks without buffering the
  /// whole payload in memory at once -- useful for large objects. Input
  /// chunks are re-buffered/re-sliced to [defaultChunkSize] pieces before
  /// publishing, matching [put]'s on-wire framing, and the SHA-256 digest
  /// is computed incrementally as data arrives rather than over one
  /// in-memory buffer. Like [put], any previous object stored under [name]
  /// has its old chunks purged once the new object is safely written.
  Future<ObjectInfo> putStream(String name, Stream<List<int>> data,
      {String description = ''}) async {
    final previous = await getInfo(name);
    final nuid = Nuid().next();
    final chunkSubject = '\$O.$bucket.C.$nuid';

    final digestSink = _DigestSink();
    final hashInput = sha256.startChunkedConversion(digestSink);

    var totalSize = 0;
    var chunkCount = 0;
    var pending = BytesBuilder(copy: false);

    Future<void> flushChunk(Uint8List chunk) async {
      await client.pub(chunkSubject, chunk);
      chunkCount++;
    }

    await for (final piece in data) {
      final bytes = piece is Uint8List ? piece : Uint8List.fromList(piece);
      if (bytes.isEmpty) continue;
      hashInput.add(bytes);
      totalSize += bytes.length;
      pending.add(bytes);
      while (pending.length >= defaultChunkSize) {
        final buffered = pending.takeBytes();
        await flushChunk(Uint8List.sublistView(buffered, 0, defaultChunkSize));
        pending = BytesBuilder(copy: false);
        if (buffered.length > defaultChunkSize) {
          pending.add(Uint8List.sublistView(buffered, defaultChunkSize));
        }
      }
    }
    if (pending.length > 0) {
      await flushChunk(pending.takeBytes());
    }
    hashInput.close();

    // Ensure all chunks are flushed to the server
    await client.flush();

    final digest = 'SHA-256=${base64Url.encode(digestSink.value!.bytes)}';

    final info = ObjectInfo(
      name: name,
      description: description,
      bucket: bucket,
      nuid: nuid,
      size: totalSize,
      mtime: DateTime.now(),
      chunks: chunkCount,
      digest: digest,
    );

    final encodedName = base64Url.encode(utf8.encode(name));
    final metadataSubject = '\$O.$bucket.M.$encodedName';
    final payload = utf8.encode(jsonEncode(info.toJson()));

    await client
        .jetStream()
        .publish(metadataSubject, Uint8List.fromList(payload));

    if (previous != null && previous.chunks > 0 && previous.nuid != nuid) {
      await _purgeChunks(previous.nuid);
    }

    return info;
  }

  /// Store a string payload as an object
  Future<ObjectInfo> putString(String name, String value,
      {String description = ''}) {
    return put(name, Uint8List.fromList(utf8.encode(value)),
        description: description);
  }

  /// Create a link to another object in the same or different bucket.
  Future<ObjectInfo> addLink(String name, ObjectInfo target,
      {String description = ''}) async {
    final nuid = Nuid().next();

    final link = ObjectLink(
      bucket: target.bucket,
      name: target.name,
    );

    final info = ObjectInfo(
      name: name,
      description: description,
      bucket: bucket,
      nuid: nuid,
      size: 0,
      mtime: DateTime.now(),
      chunks: 0,
      digest: '',
      link: link,
    );

    final encodedName = base64Url.encode(utf8.encode(name));
    final metadataSubject = '\$O.$bucket.M.$encodedName';
    final payload = utf8.encode(jsonEncode(info.toJson()));

    await client
        .jetStream()
        .publish(metadataSubject, Uint8List.fromList(payload));
    return info;
  }

  /// Create a link to an entire bucket.
  Future<ObjectInfo> addBucketLink(String name, String targetBucket,
      {String description = ''}) async {
    final nuid = Nuid().next();

    final link = ObjectLink(
      bucket: targetBucket,
      name: null,
    );

    final info = ObjectInfo(
      name: name,
      description: description,
      bucket: bucket,
      nuid: nuid,
      size: 0,
      mtime: DateTime.now(),
      chunks: 0,
      digest: '',
      link: link,
    );

    final encodedName = base64Url.encode(utf8.encode(name));
    final metadataSubject = '\$O.$bucket.M.$encodedName';
    final payload = utf8.encode(jsonEncode(info.toJson()));

    await client
        .jetStream()
        .publish(metadataSubject, Uint8List.fromList(payload));
    return info;
  }

  /// Retrieve the ObjectInfo metadata for a given name
  Future<ObjectInfo?> getInfo(String name) async {
    final encodedName = base64Url.encode(utf8.encode(name));
    final apiSubject = '\$JS.API.STREAM.MSG.GET.$streamName';
    final payload = utf8.encode(jsonEncode({
      'last_by_subj': '\$O.$bucket.M.$encodedName',
    }));

    try {
      final response =
          await client.request(apiSubject, Uint8List.fromList(payload));
      final map = jsonDecode(response.string);
      if (map['error'] != null) {
        if (map['error']['code'] == 404) {
          return null; // Not found
        }
        throw NatsException(map['error']['description'] as String);
      }
      final msgMap = map['message'] as Map<String, dynamic>;
      final dataStr = msgMap['data'] as String? ?? '';
      final decodedMeta = jsonDecode(utf8.decode(base64.decode(dataStr)));
      return ObjectInfo.fromJson(decodedMeta as Map<String, dynamic>);
    } catch (e) {
      if (e is TimeoutException) {
        return null;
      }
      rethrow;
    }
  }

  /// Retrieve full byte data of the object and verify integrity.
  /// Resolves object links recursively up to 5 jumps.
  Future<Uint8List?> get(String name, {int depth = 0}) async {
    if (depth > 5) {
      throw NatsException('Circular link dependency detected.');
    }
    final info = await getInfo(name);
    if (info == null || info.deleted) return null;

    if (info.link != null) {
      if (info.link!.name == null) {
        throw NatsException('Cannot get data from a bucket link.');
      }
      final targetStore = ObjectStore(client, info.link!.bucket);
      return targetStore.get(info.link!.name!, depth: depth + 1);
    }

    final deliverSubject = client.inboxPrefix + '.' + Nuid().next();
    final sub = client.sub(deliverSubject);

    final consumerConfig = ConsumerConfig(
      deliverSubject: deliverSubject,
      filterSubject: '\$O.$bucket.C.${info.nuid}',
      deliverPolicy: 'all',
      ackPolicy: 'none',
    );

    final chunksData = <Uint8List>[];
    final completer = Completer<Uint8List?>();
    StreamSubscription? streamSub;
    Timer? timeoutTimer;

    void cleanup() {
      timeoutTimer?.cancel();
      streamSub?.cancel();
      client.unSub(sub);
    }

    timeoutTimer = Timer(const Duration(seconds: 15), () {
      cleanup();
      if (!completer.isCompleted) {
        completer.complete(null);
      }
    });

    streamSub = sub.stream.listen((msg) {
      chunksData.add(msg.byte);
      if (chunksData.length >= info.chunks) {
        cleanup();

        final builder = BytesBuilder();
        for (final chunk in chunksData) {
          builder.add(chunk);
        }
        final fullData = builder.takeBytes();

        // Digest verification
        final hash = sha256.convert(fullData);
        final computedDigest = 'SHA-256=${base64Url.encode(hash.bytes)}';
        if (computedDigest != info.digest) {
          if (!completer.isCompleted) {
            completer.completeError(
                NatsException('SHA-256 digest verification failed.'));
          }
        } else {
          if (!completer.isCompleted) {
            completer.complete(fullData);
          }
        }
      }
    }, onError: (err) {
      cleanup();
      if (!completer.isCompleted) {
        completer.completeError(err);
      }
    });

    try {
      await client.jetStream().createConsumer('OBJ_$bucket', consumerConfig);
    } catch (e) {
      cleanup();
      if (!completer.isCompleted) {
        completer.completeError(e);
      }
    }

    return completer.future;
  }

  /// Retrieve full byte data of the object and verify integrity
  Future<Uint8List?> getBytes(String name) {
    return get(name);
  }

  /// Retrieve object data as string
  Future<String?> getString(String name) async {
    final bytes = await get(name);
    if (bytes == null) return null;
    return utf8.decode(bytes);
  }

  /// Retrieve an object's bytes as a stream of chunks instead of buffering
  /// the whole payload in memory -- useful for large objects. Digest
  /// verification still happens once every chunk has arrived; a mismatch is
  /// surfaced as an error event on the returned stream rather than a thrown
  /// exception, since by then earlier chunks have already been handed to
  /// the caller. Resolves object links recursively up to 5 jumps, the same
  /// as [get].
  Stream<Uint8List> getStream(String name, {int depth = 0}) {
    final controller = StreamController<Uint8List>();

    Future<void> run() async {
      if (depth > 5) {
        controller
            .addError(NatsException('Circular link dependency detected.'));
        await controller.close();
        return;
      }

      final info = await getInfo(name);
      if (info == null || info.deleted) {
        await controller.close();
        return;
      }

      if (info.link != null) {
        if (info.link!.name == null) {
          controller
              .addError(NatsException('Cannot get data from a bucket link.'));
          await controller.close();
          return;
        }
        final targetStore = ObjectStore(client, info.link!.bucket);
        await controller.addStream(
            targetStore.getStream(info.link!.name!, depth: depth + 1));
        await controller.close();
        return;
      }

      if (info.chunks == 0) {
        await controller.close();
        return;
      }

      final deliverSubject = client.inboxPrefix + '.' + Nuid().next();
      final sub = client.sub(deliverSubject);

      final consumerConfig = ConsumerConfig(
        deliverSubject: deliverSubject,
        filterSubject: '\$O.$bucket.C.${info.nuid}',
        deliverPolicy: 'all',
        ackPolicy: 'none',
      );

      var received = 0;
      final digestSink = _DigestSink();
      final hashInput = sha256.startChunkedConversion(digestSink);
      final done = Completer<void>();
      StreamSubscription? sourceSub;
      Timer? timeoutTimer;

      void cleanup() {
        timeoutTimer?.cancel();
        sourceSub?.cancel();
        client.unSub(sub);
      }

      timeoutTimer = Timer(const Duration(seconds: 15), () {
        cleanup();
        controller
            .addError(NatsException('Timed out retrieving object chunks.'));
        if (!done.isCompleted) done.complete();
      });

      sourceSub = sub.stream.listen((msg) {
        hashInput.add(msg.byte);
        controller.add(msg.byte);
        received++;
        if (received >= info.chunks) {
          cleanup();
          hashInput.close();
          final digest = 'SHA-256=${base64Url.encode(digestSink.value!.bytes)}';
          if (digest != info.digest) {
            controller
                .addError(NatsException('SHA-256 digest verification failed.'));
          }
          if (!done.isCompleted) done.complete();
        }
      }, onError: (err) {
        cleanup();
        controller.addError(err);
        if (!done.isCompleted) done.complete();
      });

      try {
        await client.jetStream().createConsumer('OBJ_$bucket', consumerConfig);
      } catch (e) {
        cleanup();
        controller.addError(e);
        if (!done.isCompleted) done.complete();
      }

      await done.future;
      await controller.close();
    }

    run();
    return controller.stream;
  }

  /// Delete/mark object deleted and purge its chunk history
  Future<bool> delete(String name) async {
    final info = await getInfo(name);
    if (info == null) return false;

    final encodedName = base64Url.encode(utf8.encode(name));
    final metadataSubject = '\$O.$bucket.M.$encodedName';

    final deletedInfo = ObjectInfo(
      name: info.name,
      description: info.description,
      bucket: info.bucket,
      nuid: info.nuid,
      size: 0,
      mtime: DateTime.now(),
      chunks: 0,
      digest: '',
      deleted: true,
    );

    // Update metadata to deleted=true
    final payload = utf8.encode(jsonEncode(deletedInfo.toJson()));
    await client
        .jetStream()
        .publish(metadataSubject, Uint8List.fromList(payload));

    // Purge the chunks subject to reclaim NATS space
    if (info.chunks > 0) {
      await _purgeChunks(info.nuid);
    }

    return true;
  }

  /// Purge a stored object's chunks (identified by its `nuid`) from the
  /// backing stream. Used both by [delete] and by [put]/[putStream] when
  /// overwriting an object, so a previous version's chunks don't linger
  /// server-side once nothing references them anymore.
  Future<void> _purgeChunks(String nuid,
      {Duration timeout = const Duration(seconds: 2)}) async {
    final purgeSubject = '\$JS.API.STREAM.PURGE.OBJ_$bucket';
    final purgePayload = utf8.encode(jsonEncode({
      'filter': '\$O.$bucket.C.$nuid',
    }));
    await client.request(purgeSubject, Uint8List.fromList(purgePayload),
        timeout: timeout);
  }

  /// List all active objects in this Object Store bucket
  Future<List<ObjectInfo>> list() async {
    final deliverSubject = client.inboxPrefix + '.' + Nuid().next();
    final sub = client.sub(deliverSubject);

    final consumerConfig = ConsumerConfig(
      deliverSubject: deliverSubject,
      filterSubject: '\$O.$bucket.M.>',
      deliverPolicy: 'all',
      ackPolicy: 'none',
    );

    final activeObjects = <String, ObjectInfo>{};
    final completer = Completer<List<ObjectInfo>>();
    StreamSubscription? streamSub;
    Timer? timeoutTimer;

    void cleanup() {
      timeoutTimer?.cancel();
      streamSub?.cancel();
      client.unSub(sub);
    }

    timeoutTimer = Timer(const Duration(seconds: 5), () {
      cleanup();
      if (!completer.isCompleted) {
        completer.complete(activeObjects.values.toList());
      }
    });

    streamSub = sub.stream.listen((msg) {
      try {
        final map = jsonDecode(msg.string);
        final info = ObjectInfo.fromJson(map as Map<String, dynamic>);
        if (info.deleted) {
          activeObjects.remove(info.name);
        } else {
          activeObjects[info.name] = info;
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
            completer.complete(activeObjects.values.toList());
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
        completer.complete(activeObjects.values.toList());
      }
    });

    try {
      await client.jetStream().createConsumer('OBJ_$bucket', consumerConfig);
    } catch (e) {
      cleanup();
      if (!completer.isCompleted) {
        completer.complete([]);
      }
    }

    return completer.future;
  }
}
