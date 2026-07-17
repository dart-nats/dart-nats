import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

/// `nak()`/`term()`/`inProgress()` (like `ack()` before it gained
/// `ackSync()`) publish their JetStream control message via `pub()` without
/// awaiting or returning its Future -- a disconnected client silently
/// buffers the publish and still reports success, so a caller has no way to
/// know the ack/nak/term never actually reached the server. These tests
/// cover the new `nakSync()`/`termSync()`/`inProgressSync()` methods, which
/// use the same request/reply pattern `ackSync()` already established: they
/// wait for the server's own confirmation and throw if it doesn't arrive.
void main() {
  group('Message *Sync acknowledgement methods', () {
    late Client client;
    late JetStream js;
    final streamName = 'acksync-${DateTime.now().microsecondsSinceEpoch}';

    setUp(() async {
      client = Client();
      await client.connect(Uri.parse('nats://localhost:4222'), retry: false);
      js = client.jetStream();
      await js.createStream(StreamConfig(
        name: streamName,
        subjects: ['$streamName.>'],
        storage: 'memory',
      ));
    });

    tearDown(() async {
      try {
        await js.deleteStream(streamName);
      } catch (_) {}
      if (client.connected) await client.close();
    });

    test('nakSync() triggers immediate redelivery', () async {
      const consumerName = 'nak-consumer';
      await js.publishString('$streamName.a', 'payload');
      await js.createConsumer(
          streamName, ConsumerConfig(durable: consumerName));
      final consumer = js.consumer(streamName, consumerName);

      final first = await consumer.fetch(batch: 1);
      expect(first, hasLength(1));
      await first[0].nakSync();

      final redelivered =
          await consumer.fetch(batch: 1, timeout: const Duration(seconds: 3));
      expect(redelivered, hasLength(1));
      expect(redelivered[0].string, equals('payload'));
    });

    test('termSync() prevents redelivery', () async {
      const consumerName = 'term-consumer';
      await js.publishString('$streamName.b', 'payload');
      await js.createConsumer(
          streamName, ConsumerConfig(durable: consumerName));
      final consumer = js.consumer(streamName, consumerName);

      final first = await consumer.fetch(batch: 1);
      expect(first, hasLength(1));
      await first[0].termSync();

      final notRedelivered =
          await consumer.fetch(batch: 1, timeout: const Duration(seconds: 2));
      expect(notRedelivered, isEmpty);

      final info = await consumer.info();
      expect(info.numAckPending, equals(0));
    });

    test('inProgressSync() does not throw and can be followed by ackSync()',
        () async {
      const consumerName = 'wpi-consumer';
      await js.publishString('$streamName.c', 'payload');
      await js.createConsumer(
          streamName, ConsumerConfig(durable: consumerName));
      final consumer = js.consumer(streamName, consumerName);

      final first = await consumer.fetch(batch: 1);
      expect(first, hasLength(1));
      await first[0].inProgressSync();
      await first[0].ackSync();

      final info = await consumer.info();
      expect(info.numAckPending, equals(0));
    });

    test(
        'nakSync()/termSync()/inProgressSync() throw when the client is '
        'disconnected instead of silently reporting success', () async {
      const consumerName = 'disconnected-consumer';
      await js.publishString('$streamName.d', 'payload');
      await js.createConsumer(
          streamName, ConsumerConfig(durable: consumerName));
      final consumer = js.consumer(streamName, consumerName);

      final first = await consumer.fetch(batch: 1);
      expect(first, hasLength(1));
      final msg = first[0];

      await client.close();

      expect(() => msg.nakSync(), throwsA(isA<NatsException>()));
      expect(() => msg.termSync(), throwsA(isA<NatsException>()));
      expect(() => msg.inProgressSync(), throwsA(isA<NatsException>()));

      // Contrast: the old fire-and-forget methods report success anyway --
      // pub() silently buffers the publish for a future reconnect that may
      // never happen, rather than failing loudly. Documents exactly the
      // bug these Sync variants exist to fix, not desired behavior.
      expect(msg.nak(), isTrue);
      expect(msg.term(), isTrue);
      expect(msg.inProgress(), isTrue);
    });
  });
}
