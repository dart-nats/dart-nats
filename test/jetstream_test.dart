import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';
import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

void main() {
  group('JetStream Integration', () {
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

    test('Stream Management Lifecycle', () async {
      final streamName = 'stream-mgmt-test';
      final subject = 'mgmt-subj.foo';

      // 1. createStream
      final streamConfig = StreamConfig(
        name: streamName,
        subjects: [subject],
        storage: 'memory',
      );
      final stream = await js.createStream(streamConfig);
      expect(stream.name, equals(streamName));

      // 2. streamInfo & getStream (deprecated)
      final info = await js.streamInfo(streamName);
      expect(info.config.name, equals(streamName));
      expect(info.config.subjects, contains(subject));

      final deprecatedInfo = await js.getStream(streamName);
      expect(deprecatedInfo.config.name, equals(streamName));

      // 3. updateStream
      final updatedConfig = StreamConfig(
        name: streamName,
        subjects: [subject, 'mgmt-subj.bar'],
        storage: 'memory',
      );
      final updatedStream = await js.updateStream(updatedConfig);
      final updatedInfo = await updatedStream.info();
      expect(
          updatedInfo.config.subjects, containsAll([subject, 'mgmt-subj.bar']));

      // 4. createOrUpdateStream
      final streamOrUpdated = await js.createOrUpdateStream(StreamConfig(
        name: streamName,
        subjects: [subject, 'mgmt-subj.baz'],
        storage: 'memory',
      ));
      final finalInfo = await streamOrUpdated.info();
      expect(finalInfo.config.subjects, contains('mgmt-subj.baz'));

      // 5. listStreams & streamNameBySubject
      final streamsList = await js.listStreams();
      expect(streamsList.any((s) => s.config.name == streamName), isTrue);

      final mappedName = await js.streamNameBySubject('mgmt-subj.baz');
      expect(mappedName, equals(streamName));

      // 6. JsStream.purge() & purgeStream (deprecated)
      await js.publishString(subject, 'test-payload');
      final infoBeforePurge = await stream.info();
      expect(infoBeforePurge.state.messages, equals(1));

      final purgeOk1 = await stream.purge();
      expect(purgeOk1, isTrue);

      await js.publishString(subject, 'test-payload-2');
      final purgeOk2 = await js.purgeStream(streamName);
      expect(purgeOk2, isTrue);

      final infoAfterPurge = await stream.info();
      expect(infoAfterPurge.state.messages, equals(0));

      // 7. addStream (deprecated)
      final tempStreamName = 'stream-temp-add';
      final addOk = await js.addStream(StreamConfig(
          name: tempStreamName, subjects: ['temp-subj'], storage: 'memory'));
      expect(addOk, isTrue);
      await js.deleteStream(tempStreamName);

      // 8. deleteStream
      final deleteResult = await js.deleteStream(streamName);
      expect(deleteResult, isTrue);
    });

    test('Publishing and Message Get APIs', () async {
      final streamName = 'pub-get-test';
      final subject = 'pub.get.subj';

      await js.createStream(StreamConfig(
        name: streamName,
        subjects: [subject],
        storage: 'memory',
      ));

      // 1. publish (bytes) and publishString
      final ack1 = await js.publish(subject, Uint8List.fromList([1, 2, 3]));
      expect(ack1.sequence, equals(1));

      final ack2 = await js.publishString(subject, 'hello-js',
          opts: PubOpts(msgId: 'unique-id'));
      expect(ack2.sequence, equals(2));

      // 2. getMsg (by sequence)
      final msg1 = await js.getMsg(streamName, 1);
      expect(msg1.byte, equals(Uint8List.fromList([1, 2, 3])));
      expect(msg1.streamSequence, equals(1));

      // 3. getLastMsg (by subject)
      final msg2 = await js.getLastMsg(streamName, subject);
      expect(msg2.string, equals('hello-js'));
      expect(msg2.streamSequence, equals(2));

      await js.deleteStream(streamName);
    });

    test('Consumer Management Lifecycle', () async {
      final streamName = 'cons-mgmt-test';
      final subject = 'cons.mgmt.subj';

      await js.createStream(StreamConfig(
        name: streamName,
        subjects: [subject],
        storage: 'memory',
      ));

      final consumerName = 'test-consumer';
      final config = ConsumerConfig(
        durable: consumerName,
        ackPolicy: 'explicit',
      );

      // 1. createConsumer & addConsumer (deprecated)
      final consumer = await js.createConsumer(streamName, config);
      expect(consumer.name, equals(consumerName));

      final consumerInfo = await js.consumerInfo(streamName, consumerName);
      expect(consumerInfo.name, equals(consumerName));

      final deprecatedConsInfo = await js.getConsumer(streamName, consumerName);
      expect(deprecatedConsInfo.name, equals(consumerName));

      // 2. createOrUpdateConsumer
      final consumerOrUpdate =
          await js.createOrUpdateConsumer(streamName, config);
      expect(consumerOrUpdate.name, equals(consumerName));

      // 3. listConsumers
      final consumersList = await js.listConsumers(streamName);
      expect(consumersList.any((c) => c.name == consumerName), isTrue);

      // 4. deleteConsumer
      final deleteResult = await js.deleteConsumer(streamName, consumerName);
      expect(deleteResult, isTrue);

      // 5. addConsumer (deprecated)
      final addConsOk = await js.addConsumer(
          streamName, ConsumerConfig(durable: 'add-cons-test'));
      expect(addConsOk, isTrue);
      await js.deleteConsumer(streamName, 'add-cons-test');

      await js.deleteStream(streamName);
    });

    test('Pull/Fetch & Message Acknowledgement states', () async {
      final streamName = 'pull-ack-test';
      final subject = 'pull.ack.subj';

      final stream = await js.createStream(StreamConfig(
        name: streamName,
        subjects: [subject],
        storage: 'memory',
      ));

      await js.publishString(subject, 'msg-1');
      await js.publishString(subject, 'msg-2');
      await js.publishString(subject, 'msg-3');
      await js.publishString(subject, 'msg-4');

      final consumer = await stream.createConsumer(ConsumerConfig(
        durable: 'pull-cons',
        ackPolicy: 'explicit',
        deliverPolicy: 'all',
      ));

      // 1. pull (deprecated)
      final pulledMessages = await js.pull(streamName, 'pull-cons', batch: 2);
      expect(pulledMessages.length, equals(2));

      // 2. fetch on Consumer
      final fetchedMessages = await consumer.fetch(batch: 2);
      expect(fetchedMessages.length, equals(2));

      // 3. Message acknowledgement operations
      // ack
      final ackResult = pulledMessages[0].ack();
      expect(ackResult, isTrue);

      // nak
      final nakResult = pulledMessages[1].nak();
      expect(nakResult, isTrue);

      // term
      final termResult = fetchedMessages[0].term();
      expect(termResult, isTrue);

      // inProgress
      final inProgressResult = fetchedMessages[1].inProgress();
      expect(inProgressResult, isTrue);

      // ackSync
      await pulledMessages[0].ackSync();

      await js.deleteConsumer(streamName, 'pull-cons');
      await js.deleteStream(streamName);
    });

    test('Deprecated Push Subscription subscribe helper', () async {
      final streamName = 'push-sub-test';
      final subject = 'push.sub.subj';

      await js.createStream(StreamConfig(
        name: streamName,
        subjects: [subject],
        storage: 'memory',
      ));

      // subscribe (deprecated)
      final sub = await js.subscribe(subject, stream: streamName);

      await js.publishString(subject, 'pushed');

      final msg = await sub.stream.first.timeout(const Duration(seconds: 3));
      expect(msg.string, equals('pushed'));

      client.unSub(sub);
      await js.deleteStream(streamName);
    });

    test('Ordered Consumer and status controls', () async {
      final streamName = 'ordered-cons-test';
      final subject = 'ordered.subj';

      await js.createStream(StreamConfig(
        name: streamName,
        subjects: [subject],
        storage: 'memory',
      ));

      await js.publishString(subject, 'ordered-1');
      await js.publishString(subject, 'ordered-2');

      final oc = js.orderedConsumer(
          streamName, OrderedConsumerConfig(filterSubject: subject));
      final messagesReceived = <Message>[];
      final completer = Completer<void>();

      final subscription = oc.messages().listen((msg) {
        messagesReceived.add(msg);
        if (messagesReceived.length >= 2) {
          completer.complete();
        }
      });

      await completer.future.timeout(const Duration(seconds: 5));
      expect(messagesReceived.length, equals(2));
      expect(messagesReceived[0].string, equals('ordered-1'));
      expect(messagesReceived[1].string, equals('ordered-2'));

      subscription.cancel();
      oc.stop();

      await js.deleteStream(streamName);
    });

    test('Continuous Consumer messages() - Pull Consumer', () async {
      final streamName = 'cont-pull-test';
      final subject = 'cont.pull.subj';

      final stream = await js.createStream(StreamConfig(
        name: streamName,
        subjects: [subject],
        storage: 'memory',
      ));

      final consumer = await stream.createConsumer(ConsumerConfig(
        durable: 'cont-pull-cons',
        ackPolicy: 'explicit',
        deliverPolicy: 'all',
      ));

      final messagesReceived = <Message>[];
      final subscription = consumer.messages().listen((msg) {
        messagesReceived.add(msg);
        msg.ack();
      });

      // Publish messages
      await js.publishString(subject, 'msg-1');
      await js.publishString(subject, 'msg-2');

      // Wait a short time to receive them
      await Future.delayed(const Duration(milliseconds: 500));

      expect(messagesReceived.length, equals(2));
      expect(messagesReceived[0].string, equals('msg-1'));
      expect(messagesReceived[1].string, equals('msg-2'));

      // Test pause/resume
      subscription.pause();
      await js.publishString(subject, 'msg-3');
      await Future.delayed(const Duration(milliseconds: 300));
      expect(messagesReceived.length, equals(2)); // Still 2, as we paused

      subscription.resume();
      await Future.delayed(const Duration(milliseconds: 500));
      expect(messagesReceived.length, equals(3)); // Received msg-3 after resume
      expect(messagesReceived[2].string, equals('msg-3'));

      await subscription.cancel();
      await js.deleteStream(streamName);
    });

    test('Continuous Consumer messages() - Push Consumer', () async {
      final streamName = 'cont-push-test';
      final subject = 'cont.push.subj';
      final deliverSubject = client.inboxPrefix + '.cont-push-deliver';

      final stream = await js.createStream(StreamConfig(
        name: streamName,
        subjects: [subject],
        storage: 'memory',
      ));

      final consumer = await stream.createConsumer(ConsumerConfig(
        durable: 'cont-push-cons',
        deliverSubject: deliverSubject,
        ackPolicy: 'none',
        deliverPolicy: 'all',
      ));

      final messagesReceived = <Message>[];
      final subscription = consumer.messages().listen((msg) {
        messagesReceived.add(msg);
      });

      // Wait for info check and subscription to start
      await Future.delayed(const Duration(milliseconds: 300));

      // Publish messages
      await js.publishString(subject, 'msg-1');
      await js.publishString(subject, 'msg-2');

      // Wait a short time to receive them
      await Future.delayed(const Duration(milliseconds: 500));

      expect(messagesReceived.length, equals(2));
      expect(messagesReceived[0].string, equals('msg-1'));
      expect(messagesReceived[1].string, equals('msg-2'));

      // Test pause/resume
      subscription.pause();
      await js.publishString(subject, 'msg-3');
      await Future.delayed(const Duration(milliseconds: 300));
      expect(messagesReceived.length, equals(2)); // Still 2, as we paused

      subscription.resume();
      await Future.delayed(const Duration(milliseconds: 500));
      expect(messagesReceived.length, equals(3)); // Received msg-3 after resume
      expect(messagesReceived[2].string, equals('msg-3'));

      await subscription.cancel();
      await js.deleteStream(streamName);
    });

    test('Generic publishing and generic Consumer message parsing', () async {
      final streamName = 'gen-test-stream';
      final subject = 'gen.test.subj';

      // Register decoder
      client.registerJsonDecoder<TestUser>(
          (str) => TestUser.fromJson(jsonDecode(str) as Map<String, dynamic>));

      await js.createStream(StreamConfig(
        name: streamName,
        subjects: [subject],
        storage: 'memory',
      ));

      // 1. Generic Publishing
      final user1 = TestUser(42, 'Alice');
      final user2 = TestUser(100, 'Bob');

      await js.publishPayload<TestUser>(subject, user1);
      await js.publishPayload<TestUser>(subject, user2);

      // 2. Generic Consumer (Fetch)
      final consumer = await js.createConsumer<TestUser>(
        streamName,
        ConsumerConfig(
          durable: 'gen-cons',
          ackPolicy: 'explicit',
          deliverPolicy: 'all',
        ),
      );

      final fetchedMsgs = await consumer.fetch(batch: 2);
      expect(fetchedMsgs.length, equals(2));
      expect(fetchedMsgs[0].data, isA<TestUser>());
      expect(fetchedMsgs[0].data.name, equals('Alice'));
      expect(fetchedMsgs[1].data.id, equals(100));
      expect(fetchedMsgs[1].data.name, equals('Bob'));

      // 3. Generic Consumer (messages() Stream)
      final messagesReceived = <Message<TestUser>>[];
      final completer = Completer<void>();
      final sub = consumer.messages().listen((msg) {
        messagesReceived.add(msg);
        msg.ack();
        if (messagesReceived.length >= 2) {
          completer.complete();
        }
      });

      // Publish new messages so that the continuous pull consumer has new messages to fetch
      await js.publishPayload<TestUser>(subject, TestUser(3, 'Charlie'));
      await js.publishPayload<TestUser>(subject, TestUser(4, 'David'));

      await completer.future.timeout(const Duration(seconds: 3));
      expect(messagesReceived.length, equals(2));
      expect(messagesReceived[0].data.name, equals('Charlie'));
      expect(messagesReceived[1].data.name, equals('David'));

      await sub.cancel();
      await js.deleteConsumer(streamName, 'gen-cons');
      await js.deleteStream(streamName);
    });

    test('Consumer Pause/Resume', () async {
      final streamName = 'pause-resume-test';
      final subject = 'pause.resume.subj';

      final stream = await js.createStream(StreamConfig(
        name: streamName,
        subjects: [subject],
        storage: 'memory',
      ));

      final consumerName = 'pause-resume-cons';
      final consumer = await stream.createConsumer(ConsumerConfig(
        durable: consumerName,
        ackPolicy: 'explicit',
        deliverPolicy: 'all',
      ));

      await js.publishString(subject, 'before-pause');

      // 1. pauseConsumer
      final pauseUntil =
          DateTime.now().toUtc().add(const Duration(seconds: 30));
      final pauseResponse =
          await js.pauseConsumer(streamName, consumerName, pauseUntil);
      expect(pauseResponse.paused, isTrue);

      final pausedInfo = await js.consumerInfo(streamName, consumerName);
      expect(pausedInfo.paused, isTrue);

      // While paused, a pull fetch gets nothing back rather than the pending message.
      final fetchWhilePaused = await consumer.fetch(
          batch: 1, timeout: const Duration(milliseconds: 500));
      expect(fetchWhilePaused, isEmpty);

      // 2. resumeConsumer
      final resumeResponse = await js.resumeConsumer(streamName, consumerName);
      expect(resumeResponse.paused, isFalse);

      final resumedInfo = await js.consumerInfo(streamName, consumerName);
      expect(resumedInfo.paused, isFalse);

      // Now the pending message can actually be pulled.
      final fetchAfterResume = await consumer.fetch(batch: 1);
      expect(fetchAfterResume.length, equals(1));
      expect(fetchAfterResume[0].string, equals('before-pause'));

      await js.deleteConsumer(streamName, consumerName);
      await js.deleteStream(streamName);
    });

    test('Account Info details', () async {
      final streamName = 'account-info-test';
      await js.createStream(StreamConfig(
        name: streamName,
        subjects: ['account-info-test.>'],
        storage: 'memory',
      ));

      final accInfo = await js.accountInfo();
      // Regression guard: `Tier.fromJson` used to always parse an empty map
      // (the response has no nested `tier` key for a single-tier account),
      // so every one of these silently came back as 0 regardless of real
      // account state. Asserting `isNotNull` alone doesn't catch that, since
      // 0 is not null — assert the stream we just created is actually
      // reflected in the count.
      expect(accInfo.tier.streams, greaterThanOrEqualTo(1));
      expect(accInfo.api.total, greaterThan(0));

      await js.deleteStream(streamName);
    });
  });
}

class TestUser {
  final int id;
  final String name;

  TestUser(this.id, this.name);

  factory TestUser.fromJson(Map<String, dynamic> json) {
    return TestUser(json['id'] as int, json['name'] as String);
  }

  Map<String, dynamic> toJson() => {'id': id, 'name': name};
}
