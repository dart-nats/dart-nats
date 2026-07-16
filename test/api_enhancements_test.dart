import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';
import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

void main() {
  group('Core Client & Connection Enhancements', () {
    test('Server Pooling Failover', () async {
      final client = Client();

      // Configure an invalid primary URI and a valid fallback URI
      final primary = Uri.parse('nats://localhost:1234'); // Invalid
      final fallback = Uri.parse('nats://localhost:4222'); // Valid

      await client.connect(primary, servers: [fallback], retry: false);
      expect(client.connected, isTrue);

      await client.close();
    });

    test('Connection callbacks on state transitions', () async {
      final client = Client();
      bool connectCalled = false;
      bool disconnectCalled = false;
      bool closeCalled = false;

      client.onConnect = () {
        connectCalled = true;
      };
      client.onDisconnect = () {
        disconnectCalled = true;
      };
      client.onClose = () {
        closeCalled = true;
      };

      await client.connect(Uri.parse('nats://localhost:4222'), retry: false);
      expect(connectCalled, isTrue);

      await client.tcpClose(); // Force TCP closed -> triggers disconnect
      // Allow event loop to propagate status
      await Future.delayed(const Duration(milliseconds: 100));
      expect(disconnectCalled, isTrue);

      await client.close();
      expect(closeCalled, isTrue);
    });

    test('Graceful Draining and Connection Flushing', () async {
      final client = Client();
      await client.connect(Uri.parse('nats://localhost:4222'), retry: false);

      final subject = 'test.drain.subj';
      final sub = client.sub(subject);
      final received = <Message>[];
      final subscription = sub.stream.listen((msg) {
        received.add(msg);
      });

      // Publish some messages
      await client.pubString(subject, 'message-1');
      await client.pubString(subject, 'message-2');
      await client.flush();

      // Drain the subscription
      await sub.drain();

      expect(received.length, equals(2));
      expect(received[0].string, equals('message-1'));
      expect(received[1].string, equals('message-2'));

      // Validate that sub has been cleaned up from Client
      expect(client.unSub(sub), isFalse);

      await client.drain();
      expect(client.connected, isFalse);
      await subscription.cancel();
    });

    test('Request-Reply Headers Support', () async {
      final client = Client();
      await client.connect(Uri.parse('nats://localhost:4222'), retry: false);

      final subject = 'test.req.headers.subj';
      final sub = client.sub(subject);

      // Respond helper that mirrors headers
      sub.stream.listen((msg) {
        final replyHeader = Header();
        if (msg.header != null) {
          msg.header!.headers?.forEach((k, v) {
            replyHeader.add(k, v);
          });
        }
        // Send custom reply directly to replyTo with headers (and do NOT call respond() to avoid duplicate message race)
        if (msg.replyTo != null) {
          client.pub(
              msg.replyTo, Uint8List.fromList(utf8.encode('reply-payload')),
              header: replyHeader);
        }
      });

      final reqHeader = Header().add('X-Custom-Req-Header', 'req-val');
      final resp = await client.request(
        subject,
        Uint8List.fromList(utf8.encode('req-data')),
        header: reqHeader,
      );

      expect(resp.string, equals('reply-payload'));
      expect(resp.header?.get('X-Custom-Req-Header'), equals('req-val'));

      client.unSub(sub);
      await client.close();
    });
  });

  group('Credentials File Parsing & Auth Callbacks', () {
    test('Credentials Block Parsing', () {
      final credsContent = '''
-----BEGIN NATS USER JWT-----
eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJ1c2VyMSJ9.signature
------END NATS USER JWT------
some comments here
-----BEGIN USER NKEY SEED-----
SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY
------END USER NKEY SEED------
''';
      final creds = Credentials.parse(credsContent);
      expect(
          creds.jwt,
          equals(
              'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJ1c2VyMSJ9.signature'));
      expect(creds.seed,
          equals('SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY'));
    });

    test('Auth Handlers and loadCredentials', () async {
      final client = Client();
      final credsContent = '''
-----BEGIN NATS USER JWT-----
test-jwt
------END NATS USER JWT------
-----BEGIN USER NKEY SEED-----
SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY
------END USER NKEY SEED------
''';
      client.loadCredentials(credsContent);
      expect(client.seed,
          equals('SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY'));
      expect(client.userJwtHandler!(), equals('test-jwt'));
    });
  });

  group('JetStream Publish Options & Sync ACK', () {
    late Client client;
    late JetStream js;

    setUp(() async {
      client = Client();
      await client.connect(Uri.parse('nats://localhost:4222'), retry: false);
      js = client.jetStream();
    });

    tearDown(() async {
      await client.close();
    });

    test('Exactly-Once Dedup & Expected Last Sequence', () async {
      final streamName = 'test-pub-opts-stream';
      await js.createStream(StreamConfig(
        name: streamName,
        subjects: ['pub-opts-subject.*'],
        storage: 'memory',
      ));

      // 1. Publish first message with MsgId
      final ack1 = await js.publishString(
        'pub-opts-subject.test',
        'val-1',
        opts: PubOpts(msgId: 'msg-id-123'),
      );
      expect(ack1.sequence, equals(1));
      expect(ack1.duplicate, isFalse);

      // 2. Publish duplicate message with same MsgId -> should succeed but duplicate is true
      final ack2 = await js.publishString(
        'pub-opts-subject.test',
        'val-1',
        opts: PubOpts(msgId: 'msg-id-123'),
      );
      expect(ack2.duplicate, isTrue);

      // 3. Publish asserting expected last sequence match
      final ack3 = await js.publishString(
        'pub-opts-subject.test',
        'val-2',
        opts: PubOpts(expectLastSeq: 1),
      );
      expect(ack3.sequence, equals(2));

      // 4. Publish asserting mismatched sequence -> should fail with error
      expect(
        () => js.publishString(
          'pub-opts-subject.test',
          'val-3',
          opts: PubOpts(expectLastSeq: 100),
        ),
        throwsA(isA<NatsException>()),
      );

      await js.deleteStream(streamName);
    });

    test('JetStream Stream and Consumer Management APIs', () async {
      final streamName = 'mgmt-stream';
      final streamConfig = StreamConfig(
        name: streamName,
        subjects: ['mgmt.*'],
        storage: 'memory',
      );

      // 1. Create Stream
      final stream = await js.createStream(streamConfig);

      // 2. Get Stream Info
      final info = await stream.info();
      expect(info.config.name, equals(streamName));
      expect(info.state.messages, equals(0));

      // 3. Update Stream config
      final updatedConfig = StreamConfig(
        name: streamName,
        subjects: ['mgmt.*', 'mgmt-extra.*'],
        storage: 'memory',
      );
      final updatedStream = await js.updateStream(updatedConfig);

      final updatedInfo = await updatedStream.info();
      expect(updatedInfo.config.subjects.length, equals(2));

      // 4. Create Consumer
      final consumerName = 'mgmt-consumer';
      final consumer = await updatedStream
          .createConsumer(ConsumerConfig(durable: consumerName));

      // 5. Get Consumer Info
      final consInfo = await consumer.info();
      expect(consInfo.name, equals(consumerName));

      // 6. List consumers
      final consumers = await js.listConsumers(streamName);
      expect(consumers.length, equals(1));
      expect(consumers[0].name, equals(consumerName));

      // 7. List streams
      final streamsList = await js.listStreams();
      expect(streamsList.any((s) => s.config.name == streamName), isTrue);

      // 8. Purge stream
      await js.publishString('mgmt.test', 'purge-me');
      final infoBeforePurge = await updatedStream.info();
      expect(infoBeforePurge.state.messages, equals(1));

      final purgeOk = await stream.purge();
      expect(purgeOk, isTrue);

      final infoAfterPurge = await updatedStream.info();
      expect(infoAfterPurge.state.messages, equals(0));

      await js.deleteConsumer(streamName, consumerName);
      await js.deleteStream(streamName);
    });

    test('Sync Acknowledgements and Metadata extraction', () async {
      final streamName = 'sync-ack-stream';
      final consumerName = 'sync-ack-consumer';

      final stream = await js.createStream(StreamConfig(
        name: streamName,
        subjects: ['sync-ack.*'],
        storage: 'memory',
      ));

      await js.publishString('sync-ack.test', 'sync-ack-data');

      final consumer =
          await stream.createConsumer(ConsumerConfig(durable: consumerName));

      final messages = await consumer.fetch(batch: 1);
      expect(messages.length, equals(1));

      final msg = messages[0];
      // Verify replyTo subject format and parsed stream sequence
      expect(msg.streamSequence, equals(1));

      // Test ackSync
      await msg.ackSync();

      await js.deleteConsumer(streamName, consumerName);
      await js.deleteStream(streamName);
    });
  });

  group('High-Level Abstractions (KeyValue & Object Store)', () {
    late Client client;
    late JetStream js;

    setUp(() async {
      client = Client();
      await client.connect(Uri.parse('nats://localhost:4222'), retry: false);
      js = client.jetStream();
    });

    tearDown(() async {
      await client.close();
    });

    test('KeyValue Store put/get/delete/purge/watch lifecycle', () async {
      final bucket = 'my_test_kv_${DateTime.now().millisecondsSinceEpoch}';
      final kv = await js.createKeyValue(
          KeyValueConfig(bucket: bucket, storage: 'memory', history: 10));

      // 1. Put
      final rev1 = await kv.putString('setting1', 'value1');
      expect(rev1, equals(1));

      // 2. Get
      final entry = await kv.get('setting1');
      expect(entry, isNotNull);
      expect(entry!.string, equals('value1'));
      expect(entry.revision, equals(1));
      expect(entry.bucket, equals(bucket));
      expect(entry.op, equals(KeyValueOp.put));

      // 3. Create (Atomic conditional put)
      final rev2 = await kv.createString('setting2', 'value2');
      expect(rev2, isPositive);

      // Attempting to create existing key should throw
      expect(() => kv.createString('setting2', 'value2_new'),
          throwsA(isA<NatsException>()));

      // 4. Update (Atomic conditional update)
      final rev3 = await kv.updateString('setting2', 'value2_updated', rev2);
      expect(rev3, isPositive);

      // Updating with incorrect revision should throw
      expect(() => kv.updateString('setting2', 'value2_stale', rev2),
          throwsA(isA<NatsException>()));

      // 5. GetRevision
      final revEntry = await kv.getRevision('setting2', rev2);
      expect(revEntry, isNotNull);
      expect(revEntry!.string, equals('value2'));

      // 6. Watch (with deletion/purge structured entries)
      final watchCompleter = Completer<KeyValueEntry>();
      final watchDeletes = <KeyValueEntry>[];
      final watchSub =
          kv.watch(key: 'setting1', includeHistory: true).listen((update) {
        if (update != null) {
          print(
              'KV Watch received update: key=${update.key}, value=${update.string}, revision=${update.revision}, op=${update.op}');
          if (update.op == KeyValueOp.put) {
            if (!watchCompleter.isCompleted) {
              watchCompleter.complete(update);
            }
          } else {
            watchDeletes.add(update);
          }
        }
      });

      final watchEntry =
          await watchCompleter.future.timeout(const Duration(seconds: 4));
      expect(watchEntry, isNotNull);
      expect(watchEntry.string, equals('value1'));

      // Delete & Purge
      final delOk = await kv.delete('setting1');
      expect(delOk, isTrue);

      final entryAfterDel = await kv.get('setting1');
      expect(entryAfterDel, isNull);

      await kv.putString('setting1', 'value3');
      final purgeOk = await kv.purge('setting1');
      expect(purgeOk, isTrue);

      // Wait a moment for watch events to propagate
      await Future<void>.delayed(const Duration(milliseconds: 200));
      watchSub.cancel();

      expect(watchDeletes.any((e) => e.op == KeyValueOp.delete), isTrue);
      expect(watchDeletes.any((e) => e.op == KeyValueOp.purge), isTrue);

      // Clean up bucket using modern deleteKeyValue
      await js.deleteKeyValue(bucket);
    });

    test('Object Store chunking, hashing, put/get/delete/list lifecycle',
        () async {
      final bucket = 'my_test_obj_${DateTime.now().millisecondsSinceEpoch}';
      final os = await js.createObjectStore(
          ObjectStoreConfig(bucket: bucket, storage: 'memory'));

      // Generate random binary data larger than 128 KiB chunk size
      final size = 150 * 1024;
      final originalData = Uint8List(size);
      for (var i = 0; i < size; i++) {
        originalData[i] = i % 256;
      }

      // 1. PutBytes
      final info = await os.putBytes('large-file.bin', originalData,
          description: 'binary data');
      expect(info.name, equals('large-file.bin'));
      expect(info.size, equals(size));
      expect(info.chunks, equals(2)); // 150 KiB / 128 KiB chunking = 2 chunks
      expect(info.deleted, isFalse);

      // 2. GetBytes & verify integrity
      final retrievedData = await os.getBytes('large-file.bin');
      expect(retrievedData, isNotNull);
      expect(retrievedData!.length, equals(size));
      expect(retrievedData, equals(originalData));

      // 3. List
      final objects = await os.list();
      expect(objects.length, equals(1));
      expect(objects[0].name, equals('large-file.bin'));

      // 4. Delete & Clean chunks
      final delOk = await os.delete('large-file.bin');
      expect(delOk, isTrue);

      final infoAfterDel = await os.getInfo('large-file.bin');
      expect(infoAfterDel!.deleted, isTrue);

      final dataAfterDel = await os.getBytes('large-file.bin');
      expect(dataAfterDel, isNull);

      // Clean up bucket using modern deleteObjectStore
      await js.deleteObjectStore(bucket);
    });
  });

  group('New enhancements (heartbeats, reconnect buffer, microservices)', () {
    test('Heartbeat pings are sent and connection stays healthy', () async {
      final client = Client();
      await client.connect(
        Uri.parse('nats://localhost:4222'),
        retry: false,
        pingInterval: const Duration(milliseconds: 200),
        maxPingsOut: 2,
      );

      expect(client.connected, isTrue);
      // Wait for a few pings to fire
      await Future.delayed(const Duration(milliseconds: 600));
      expect(client.connected, isTrue);

      await client.close();
    });

    test('Reconnection works with finite retryCount after dropping', () async {
      final client = Client();
      final statusHistory = <Status>[];
      client.statusStream.listen(statusHistory.add);

      await client.connect(
        Uri.parse('nats://localhost:4222'),
        retry: true,
        retryCount: 5,
        retryInterval: 1,
      );

      expect(client.connected, isTrue);

      // Force disconnected state
      await client.tcpClose();

      // Wait until reconnection automatically happens
      await client.waitUntilConnected();
      expect(client.connected, isTrue);

      // Verify that we transitioned through disconnected and reconnecting states
      expect(statusHistory, contains(Status.disconnected));
      expect(statusHistory, contains(Status.reconnecting));

      await client.close();
    });

    test('Reconnect buffer limit throws exception when exceeded', () async {
      final client = Client();
      await client.connect(
        Uri.parse('nats://localhost:4222'),
        retry: false,
        maxReconnectBuffer: 2,
      );

      // Force disconnected state
      await client.tcpClose();
      await Future.delayed(const Duration(milliseconds: 50));
      expect(client.connected, isFalse);

      // Publish within limit
      final pub1 = await client.pubString('buf.test', 'msg1');
      final pub2 = await client.pubString('buf.test', 'msg2');
      expect(pub1, isTrue);
      expect(pub2, isTrue);

      // Exceed limit -> should throw
      expect(
        () => client.pubString('buf.test', 'msg3'),
        throwsA(isA<NatsException>()),
      );

      await client.close();
    });

    test('NATS Microservices Framework (ADR-32) endpoints & monitoring', () async {
      final client = Client();
      await client.connect(Uri.parse('nats://localhost:4222'), retry: false);

      final service = await client.addService(ServiceConfig(
        name: 'test-service',
        version: '1.2.3',
        description: 'Test Microservice',
        metadata: {'env': 'test'},
        endpoints: [
          Endpoint(
            name: 'hello',
            subject: 'service.hello',
            handler: (msg) {
              msg.respondString('hello-response');
            },
          ),
        ],
      ));

      // 1. Call service endpoint
      final resp = await client.requestString('service.hello', 'input');
      expect(resp.string, equals('hello-response'));

      // 2. Query PING
      final pingResp = await client.requestString('\$SRV.PING.test-service', '');
      final pingMap = jsonDecode(pingResp.string);
      expect(pingMap['name'], equals('test-service'));
      expect(pingMap['version'], equals('1.2.3'));

      // 3. Query INFO
      final infoResp = await client.requestString('\$SRV.INFO.test-service', '');
      final infoMap = jsonDecode(infoResp.string);
      expect(infoMap['name'], equals('test-service'));
      expect(infoMap['description'], equals('Test Microservice'));
      expect(infoMap['endpoints'][0]['name'], equals('hello'));

      // 4. Query STATS
      final statsResp = await client.requestString('\$SRV.STATS.test-service', '');
      final statsMap = jsonDecode(statsResp.string);
      expect(statsMap['name'], equals('test-service'));
      final epStats = statsMap['stats']['endpoints'][0];
      expect(epStats['name'], equals('hello'));
      expect(epStats['num_requests'], equals(1));

      await service.stop();
      await client.close();
    });

    test('Service discovery: fan-in PING collects every running instance',
        () async {
      final client = Client();
      await client.connect(Uri.parse('nats://localhost:4222'), retry: false);

      final instance1 = Client();
      await instance1.connect(Uri.parse('nats://localhost:4222'),
          retry: false);
      final instance2 = Client();
      await instance2.connect(Uri.parse('nats://localhost:4222'),
          retry: false);

      final service1 = await instance1.addService(ServiceConfig(
        name: 'discovery-fanout-service',
        version: '1.0.0',
      ));
      final service2 = await instance2.addService(ServiceConfig(
        name: 'discovery-fanout-service',
        version: '1.0.0',
      ));

      // Bare $SRV.PING should be answered by both running instances.
      final replies = await client.discoverServices(
        timeout: const Duration(milliseconds: 800),
      );
      final ours =
          replies.where((r) => r.name == 'discovery-fanout-service').toList();

      expect(ours.length, equals(2));
      expect(ours.map((r) => r.id).toSet().length, equals(2));
      expect(ours.every((r) => r.version == '1.0.0'), isTrue);

      // A name that nothing is registered under should collect nothing,
      // and still resolve once the collection window elapses.
      final none = await client.discoverServices(
        name: 'no-such-service',
        timeout: const Duration(milliseconds: 300),
      );
      expect(none, isEmpty);

      await service1.stop();
      await service2.stop();
      await instance1.close();
      await instance2.close();
      await client.close();
    });

    test('Service discovery: targeted INFO and STATS for a named service',
        () async {
      final client = Client();
      await client.connect(Uri.parse('nats://localhost:4222'), retry: false);

      final service = await client.addService(ServiceConfig(
        name: 'discovery-detail-service',
        version: '2.1.0',
        description: 'Discovery detail test service',
        metadata: {'region': 'us-west-1'},
        endpoints: [
          Endpoint(
            name: 'echo',
            subject: 'discovery.echo',
            handler: (msg) {
              msg.respondString('echo-response');
            },
          ),
        ],
      ));

      // Drive one request through the endpoint so STATS has something to report.
      await client.requestString('discovery.echo', 'ping');

      final infoReplies =
          await client.getServicesInfo(name: 'discovery-detail-service');
      expect(infoReplies.length, equals(1));
      expect(infoReplies.single.description,
          equals('Discovery detail test service'));
      expect(infoReplies.single.endpoints.single.name, equals('echo'));
      expect(
          infoReplies.single.endpoints.single.subject, equals('discovery.echo'));

      final statsReplies =
          await client.getServicesStats(name: 'discovery-detail-service');
      expect(statsReplies.length, equals(1));
      expect(statsReplies.single.metadata['region'], equals('us-west-1'));
      final epStats = statsReplies.single.endpoints.single;
      expect(epStats.name, equals('echo'));
      expect(epStats.numRequests, equals(1));
      expect(epStats.numErrors, equals(0));

      // Check id-without-name argument validation
      expect(
        () => client.discoverServices(id: 'some-id'),
        throwsArgumentError,
      );

      await service.stop();
      await client.close();
    });
  });
}
