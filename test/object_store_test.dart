import 'dart:convert';
import 'dart:typed_data';
import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

void main() {
  group('Object Store Integration', () {
    late Client client;
    late JetStream js;
    late String bucket;

    setUp(() async {
      client = Client();
      await client.connect(Uri.parse('nats://localhost:4222'), retry: false);
      js = client.jetStream();
      bucket = 'obj_test_${DateTime.now().millisecondsSinceEpoch}';
    });

    tearDown(() async {
      try {
        await js.deleteObjectStore(bucket);
      } catch (_) {}
      await client.close();
    });

    test('Object Store config and creation', () async {
      final config = ObjectStoreConfig(
        bucket: bucket,
        description: 'Test object store description',
        storage: 'memory',
        replicas: 1,
        maxBytes: 5 * 1024 * 1024,
        ttl: const Duration(days: 1),
      );

      final os = await js.createObjectStore(config);
      expect(os.bucket, equals(bucket));
      expect(os.streamName, equals('OBJ_$bucket'));

      // Bind to existing
      final existingOs = await js.objectStore(bucket);
      expect(existingOs.bucket, equals(bucket));
    });

    test('Object Store basic put and get (bytes, strings, and chunking)',
        () async {
      final os = await js.createObjectStore(
        ObjectStoreConfig(bucket: bucket, storage: 'memory'),
      );

      // 1. Put and get string
      final infoString = await os.putString(
          'test-str.txt', 'hello object store',
          description: 'text content');
      expect(infoString.name, equals('test-str.txt'));
      expect(infoString.description, equals('text content'));
      expect(infoString.size, equals(18));
      expect(infoString.chunks, equals(1));
      expect(infoString.deleted, isFalse);
      expect(infoString.link, isNull);

      final strData = await os.getString('test-str.txt');
      expect(strData, equals('hello object store'));

      // 2. Put and get large byte array to force chunking (chunk size is 128 KiB)
      final size = 150 * 1024; // 150 KiB
      final originalData = Uint8List(size);
      for (var i = 0; i < size; i++) {
        originalData[i] = i % 256;
      }

      final infoBytes = await os.putBytes('large-file.bin', originalData);
      expect(infoBytes.name, equals('large-file.bin'));
      expect(infoBytes.size, equals(size));
      expect(infoBytes.chunks, equals(2)); // 2 chunks of 128 KiB

      final retrievedBytes = await os.getBytes('large-file.bin');
      expect(retrievedBytes, isNotNull);
      expect(retrievedBytes!.length, equals(size));
      expect(retrievedBytes, equals(originalData));
    });

    test('Object Store list and delete', () async {
      final os = await js.createObjectStore(
        ObjectStoreConfig(bucket: bucket, storage: 'memory'),
      );

      await os.putString('a.txt', 'aaa');
      await os.putString('b.txt', 'bbb');

      // 1. List active objects
      var list = await os.list();
      expect(list.length, equals(2));
      final names = list.map((info) => info.name).toList();
      expect(names, containsAll(['a.txt', 'b.txt']));

      // 2. Delete object
      final delResult = await os.delete('a.txt');
      expect(delResult, isTrue);

      final infoAfterDel = await os.getInfo('a.txt');
      expect(infoAfterDel, isNotNull);
      expect(infoAfterDel!.deleted, isTrue);

      // getBytes on deleted should return null
      final bytesAfterDel = await os.getBytes('a.txt');
      expect(bytesAfterDel, isNull);

      // List should no longer contain a.txt
      list = await os.list();
      expect(list.length, equals(1));
      expect(list[0].name, equals('b.txt'));
    });

    test(
        'Object Store links (object link resolution and bucket link restriction)',
        () async {
      final targetBucket =
          'target_bucket_${DateTime.now().millisecondsSinceEpoch}';

      final srcStore = await js.createObjectStore(
        ObjectStoreConfig(bucket: bucket, storage: 'memory'),
      );
      final destStore = await js.createObjectStore(
        ObjectStoreConfig(bucket: targetBucket, storage: 'memory'),
      );

      try {
        // 1. Create a target object in srcStore
        final targetInfo =
            await srcStore.putString('real-obj', 'link target payload');

        // 2. Add link in destStore pointing to target object
        final linkInfo = await destStore.addLink('link-to-obj', targetInfo,
            description: 'object reference');
        expect(linkInfo.link, isNotNull);
        expect(linkInfo.link!.bucket, equals(bucket));
        expect(linkInfo.link!.name, equals('real-obj'));

        // 3. getBytes on link should resolve target object data
        final resolvedString = await destStore.getString('link-to-obj');
        expect(resolvedString, equals('link target payload'));

        // 4. Add bucket link in destStore
        final bucketLinkInfo =
            await destStore.addBucketLink('link-to-bucket', bucket);
        expect(bucketLinkInfo.link, isNotNull);
        expect(bucketLinkInfo.link!.bucket, equals(bucket));
        expect(bucketLinkInfo.link!.name, isNull);

        // 5. Trying to resolve data on a bucket link should throw
        expect(
          () => destStore.get('link-to-bucket'),
          throwsA(isA<NatsException>().having(
            (e) => e.message,
            'message',
            contains('Cannot get data from a bucket link'),
          )),
        );
      } finally {
        await js.deleteObjectStore(targetBucket);
      }
    });

    test('Object Store circular link dependency detection', () async {
      final store = await js.createObjectStore(
        ObjectStoreConfig(bucket: bucket, storage: 'memory'),
      );

      // Create a chain of links: link1 -> link2 -> link3 -> link1
      // Note: we can mock/insert metadata directly using put
      // But wait! We can just create them sequentially:
      // Create a dummy metadata ObjectInfo for link3, point link1 to link2, link2 to link3, link3 to link1.
      final targetInfo3 = ObjectInfo(
        name: 'link3',
        bucket: bucket,
        nuid: 'nuid3',
        size: 0,
        mtime: DateTime.now(),
        chunks: 0,
        digest: '',
        link: ObjectLink(bucket: bucket, name: 'link1'),
      );
      await store.addLink('link2', targetInfo3);

      final info2 = await store.getInfo('link2');
      expect(info2, isNotNull);

      await store.addLink('link1', info2!);

      // Now create the link3 pointing back to link1
      final info1 = await store.getInfo('link1');
      expect(info1, isNotNull);

      // Overwrite/recreate link3 to point to link1
      await store.addLink('link3', info1!);

      // Trying to get 'link1' should throw circular reference
      expect(
        () => store.get('link1'),
        throwsA(isA<NatsException>().having(
          (e) => e.message,
          'message',
          contains('Circular link dependency detected'),
        )),
      );
    });

    test('Object Store overwrite purges the previous version\'s chunks',
        () async {
      final os = await js.createObjectStore(
        ObjectStoreConfig(bucket: bucket, storage: 'memory'),
      );

      final size = 150 * 1024; // 150 KiB -> 2 chunks
      final originalData = Uint8List(size);
      for (var i = 0; i < size; i++) {
        originalData[i] = i % 256;
      }

      final oldInfo = await os.putBytes('overwrite-me.bin', originalData);
      expect(oldInfo.chunks, equals(2));
      final oldChunkSubject = '\$O.$bucket.C.${oldInfo.nuid}';
      expect(
          await _chunkSubjectHasMessages(
              client, 'OBJ_$bucket', oldChunkSubject),
          isTrue);

      final newData = Uint8List.fromList(utf8.encode('replacement payload'));
      final newInfo = await os.putBytes('overwrite-me.bin', newData);
      expect(newInfo.nuid, isNot(equals(oldInfo.nuid)));

      // The old version's chunks should be gone from the backing stream.
      expect(
          await _chunkSubjectHasMessages(
              client, 'OBJ_$bucket', oldChunkSubject),
          isFalse);

      // The new version is still readable and correct.
      final readBack = await os.getBytes('overwrite-me.bin');
      expect(readBack, equals(newData));
    });

    test('Object Store streaming put/get (putStream/getStream)', () async {
      final os = await js.createObjectStore(
        ObjectStoreConfig(bucket: bucket, storage: 'memory'),
      );

      final size = 300 * 1024; // 300 KiB -> 3 chunks of up to 128 KiB
      final originalData = Uint8List(size);
      for (var i = 0; i < size; i++) {
        originalData[i] = (i * 7) % 256;
      }

      // Feed putStream with small, uneven pieces to exercise re-chunking.
      Stream<List<int>> unevenChunks() async* {
        const pieceSize = 4096;
        var offset = 0;
        while (offset < originalData.length) {
          final end = (offset + pieceSize > originalData.length)
              ? originalData.length
              : offset + pieceSize;
          yield originalData.sublist(offset, end);
          offset = end;
        }
      }

      final info = await os.putStream('streamed.bin', unevenChunks());
      expect(info.size, equals(size));
      expect(info.chunks, equals(3));

      final received = <int>[];
      await for (final chunk in os.getStream('streamed.bin')) {
        received.addAll(chunk);
      }
      expect(Uint8List.fromList(received), equals(originalData));

      // The buffered get() path should read back the same data too.
      final bytes = await os.getBytes('streamed.bin');
      expect(bytes, equals(originalData));

      // Overwriting a streamed-put object via putStream also purges the
      // previous version's chunks.
      final oldChunkSubject = '\$O.$bucket.C.${info.nuid}';
      expect(
          await _chunkSubjectHasMessages(
              client, 'OBJ_$bucket', oldChunkSubject),
          isTrue);

      Stream<List<int>> replacement() async* {
        yield utf8.encode('short replacement');
      }

      final newInfo = await os.putStream('streamed.bin', replacement());
      expect(newInfo.nuid, isNot(equals(info.nuid)));
      expect(
          await _chunkSubjectHasMessages(
              client, 'OBJ_$bucket', oldChunkSubject),
          isFalse);
      expect(await os.getString('streamed.bin'), equals('short replacement'));
    });

    test('Object Store getStream surfaces digest verification failure',
        () async {
      final store = await js.createObjectStore(
        ObjectStoreConfig(bucket: bucket, storage: 'memory'),
      );

      final info =
          await store.putString('corrupt-stream.txt', 'integrity test');

      final purgeSubject = '\$JS.API.STREAM.PURGE.OBJ_$bucket';
      final purgePayload = utf8.encode(jsonEncode({
        'filter': '\$O.$bucket.C.${info.nuid}',
      }));
      await client.request(purgeSubject, Uint8List.fromList(purgePayload));

      final chunkSubject = '\$O.$bucket.C.${info.nuid}';
      await client.pubString(chunkSubject, 'corrupted chunk');
      await client.flush();

      expect(
        () => store.getStream('corrupt-stream.txt').drain<void>(),
        throwsA(isA<NatsException>().having(
          (e) => e.message,
          'message',
          contains('SHA-256 digest verification failed'),
        )),
      );
    });

    test('Object Store integrity digest validation failure', () async {
      final store = await js.createObjectStore(
        ObjectStoreConfig(bucket: bucket, storage: 'memory'),
      );

      // 1. Put object
      final info = await store.putString('corrupt-me.txt', 'integrity test');

      // 2. Purge the original chunk so NATS doesn't deliver the valid one first
      final purgeSubject = '\$JS.API.STREAM.PURGE.OBJ_$bucket';
      final purgePayload = utf8.encode(jsonEncode({
        'filter': '\$O.$bucket.C.${info.nuid}',
      }));
      await client.request(purgeSubject, Uint8List.fromList(purgePayload));

      // 3. Publish the corrupted chunk on the same subject
      final chunkSubject = '\$O.$bucket.C.${info.nuid}';
      await client.pubString(chunkSubject, 'corrupted chunk');
      await client.flush();

      // 4. Trying to get the object should throw digest verification failure
      expect(
        () => store.getString('corrupt-me.txt'),
        throwsA(isA<NatsException>().having(
          (e) => e.message,
          'message',
          contains('SHA-256 digest verification failed'),
        )),
      );
    });
  });
}

/// Whether a direct-get last-by-subject request against [subject] on
/// [streamName] finds any message -- used to confirm a purged chunk subject
/// genuinely has nothing left, the same technique [ObjectStore.getInfo]
/// itself uses to look up metadata.
Future<bool> _chunkSubjectHasMessages(
    Client client, String streamName, String subject) async {
  final apiSubject = '\$JS.API.STREAM.MSG.GET.$streamName';
  final payload = utf8.encode(jsonEncode({'last_by_subj': subject}));
  final response =
      await client.request(apiSubject, Uint8List.fromList(payload));
  final map = jsonDecode(response.string);
  return map['error'] == null;
}
