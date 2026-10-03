import 'dart:async';
import 'dart:typed_data';
import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

void main() {
  group('KeyValue Store Integration', () {
    late Client client;
    late JetStream js;
    late String bucket;

    setUp(() async {
      client = Client();
      await client.connect(Uri.parse('nats://localhost:4222'), retry: false);
      js = client.jetStream();
      bucket = 'kv_test_${DateTime.now().millisecondsSinceEpoch}';
    });

    tearDown(() async {
      try {
        await js.deleteKeyValue(bucket);
      } catch (_) {}
      await client.close();
    });

    test('KeyValue Store config and creation', () async {
      final config = KeyValueConfig(
        bucket: bucket,
        description: 'Test bucket description',
        storage: 'memory',
        history: 5,
        maxBytes: 10 * 1024 * 1024,
        ttl: const Duration(hours: 1),
      );

      final kv = await js.createKeyValue(config);
      expect(kv.bucket, equals(bucket));
      expect(kv.streamName, equals('KV_$bucket'));

      // Retrieve existing kv
      final existingKv = await js.keyValue(bucket);
      expect(existingKv.bucket, equals(bucket));
    });

    test('KeyValue Store basic put/get operations (bytes & strings)', () async {
      final kv = await js.createKeyValue(
        KeyValueConfig(bucket: bucket, storage: 'memory'),
      );

      // 1. Put and get binary bytes
      final binaryData = Uint8List.fromList([1, 2, 3, 4, 5]);
      final rev1 = await kv.put('bin-key', binaryData);
      expect(rev1, isPositive);

      final entry1 = await kv.get('bin-key');
      expect(entry1, isNotNull);
      expect(entry1!.key, equals('bin-key'));
      expect(entry1.value, equals(binaryData));
      expect(entry1.revision, equals(rev1));
      expect(entry1.op, equals(KeyValueOp.put));

      // 2. Put and get string
      final rev2 = await kv.putString('str-key', 'hello-kv');
      expect(rev2, isPositive);

      final entry2 = await kv.get('str-key');
      expect(entry2, isNotNull);
      expect(entry2!.key, equals('str-key'));
      expect(entry2.string, equals('hello-kv'));
      expect(entry2.revision, equals(rev2));
      expect(entry2.op, equals(KeyValueOp.put));
    });

    test('KeyValue Store atomic create & update (optimistic concurrency)',
        () async {
      final kv = await js.createKeyValue(
        KeyValueConfig(bucket: bucket, storage: 'memory'),
      );

      // 1. Create - only if not exists
      final binaryData = Uint8List.fromList([10, 20]);
      final rev1 = await kv.create('key-atomic', binaryData);
      expect(rev1, isPositive);

      // Creating again should throw error
      await expectLater(
        () => kv.create('key-atomic', Uint8List.fromList([30])),
        throwsA(isA<NatsException>()),
      );

      // Create String
      final rev2 = await kv.createString('key-atomic-str', 'atomic-string');
      expect(rev2, isPositive);

      await expectLater(
        () => kv.createString('key-atomic-str', 'another'),
        throwsA(isA<NatsException>()),
      );

      // 2. Update - only if revision matches
      final rev3 = await kv.update(
        'key-atomic',
        Uint8List.fromList([100, 200]),
        rev1,
      );
      expect(rev3, isPositive);

      // Updating with stale revision should throw
      await expectLater(
        () => kv.update('key-atomic', Uint8List.fromList([99]), rev1),
        throwsA(isA<NatsException>()),
      );

      // Update String
      final rev4 = await kv.updateString(
          'key-atomic-str', 'updated-atomic-string', rev2);
      expect(rev4, isPositive);

      await expectLater(
        () => kv.updateString('key-atomic-str', 'stale', rev2),
        throwsA(isA<NatsException>()),
      );
    });

    test('KeyValue Store getRevision', () async {
      final kv = await js.createKeyValue(
        KeyValueConfig(bucket: bucket, storage: 'memory', history: 3),
      );

      final rev1 = await kv.putString('history-key', 'v1');
      final rev2 = await kv.putString('history-key', 'v2');
      final rev3 = await kv.putString('history-key', 'v3');

      final entry1 = await kv.getRevision('history-key', rev1);
      expect(entry1, isNotNull);
      expect(entry1!.string, equals('v1'));

      final entry2 = await kv.getRevision('history-key', rev2);
      expect(entry2, isNotNull);
      expect(entry2!.string, equals('v2'));

      final entry3 = await kv.getRevision('history-key', rev3);
      expect(entry3, isNotNull);
      expect(entry3!.string, equals('v3'));

      // Non-existent revision or mismatched key sequence
      final entryNone = await kv.getRevision('history-key', 999);
      expect(entryNone, isNull);
    });

    test('KeyValue Store delete, purge, and keys list', () async {
      final kv = await js.createKeyValue(
        KeyValueConfig(bucket: bucket, storage: 'memory', history: 5),
      );

      await kv.putString('key-1', 'val-1');
      await kv.putString('key-2', 'val-2');
      await kv.putString('key-3', 'val-3');

      // 1. Check active keys
      var activeKeys = await kv.keys();
      expect(activeKeys, containsAll(['key-1', 'key-2', 'key-3']));

      // 2. Delete key-1 (leaves tombstone, value becomes null)
      final delResult = await kv.delete('key-1');
      expect(delResult, isTrue);

      final entryAfterDel = await kv.get('key-1');
      expect(entryAfterDel, isNull);

      activeKeys = await kv.keys();
      expect(activeKeys, isNot(contains('key-1')));
      expect(activeKeys, containsAll(['key-2', 'key-3']));

      // 3. Purge key-2 (clears history, leaves rollup delete)
      final purgeResult = await kv.purge('key-2');
      expect(purgeResult, isTrue);

      final entryAfterPurge = await kv.get('key-2');
      expect(entryAfterPurge, isNull);

      activeKeys = await kv.keys();
      expect(activeKeys, isNot(contains('key-2')));
      expect(activeKeys, contains('key-3'));
    });

    test('KeyValue Store history and status', () async {
      final kv = await js.createKeyValue(
        KeyValueConfig(bucket: bucket, storage: 'memory', history: 10),
      );

      await kv.putString('hist-key', 'first');
      await kv.putString('hist-key', 'second');
      await kv.putString('hist-key', 'third');
      await kv.delete('hist-key');

      // Check history
      final historyList = await kv.history('hist-key').toList();
      expect(historyList.length, equals(4));
      expect(historyList[0].string, equals('first'));
      expect(historyList[1].string, equals('second'));
      expect(historyList[2].string, equals('third'));
      expect(historyList[3].op, equals(KeyValueOp.delete));

      // Check status
      final status = await kv.status();
      expect(status.bucket, equals(bucket));
      expect(status.storage, equals('memory'));
      expect(status.values, equals(4)); // 3 puts + 1 delete tombstone
      expect(status.size, isPositive);
    });

    test('KeyValue Store watch (real-time stream)', () async {
      final kv = await js.createKeyValue(
        KeyValueConfig(bucket: bucket, storage: 'memory'),
      );

      final watchResults = <KeyValueEntry>[];
      final completer = Completer<void>();

      final subscription =
          kv.watch(key: 'watch.*', includeHistory: true).listen((entry) {
        if (entry != null) {
          watchResults.add(entry);
          if (watchResults.length >= 3) {
            completer.complete();
          }
        }
      });

      await kv.putString('watch.a', 'apple');
      await kv.putString('watch.b', 'banana');
      await kv.delete('watch.a');

      await completer.future.timeout(const Duration(seconds: 5));
      await subscription.cancel();

      expect(watchResults.length, equals(3));
      expect(watchResults[0].key, equals('watch.a'));
      expect(watchResults[0].string, equals('apple'));
      expect(watchResults[1].key, equals('watch.b'));
      expect(watchResults[1].string, equals('banana'));
      expect(watchResults[2].key, equals('watch.a'));
      expect(watchResults[2].op, equals(KeyValueOp.delete));
    });
  });
}
