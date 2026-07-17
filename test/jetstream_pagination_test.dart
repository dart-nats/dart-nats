import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

/// nats-server caps `$JS.API.STREAM.LIST` and `$JS.API.CONSUMER.LIST.*`
/// responses to a fixed page (256 items) regardless of how many
/// streams/consumers actually exist, reporting the true count in the same
/// response's `total` field. A client that never sends `offset` silently
/// drops everything past the first page -- no error, no truncation flag.
/// These tests create more than one page's worth of streams/consumers and
/// confirm `listStreams()`/`listConsumers()` return the full set.
void main() {
  group('JetStream list pagination', () {
    late Client client;
    late JetStream js;
    const streamCount = 260;
    final prefix = 'pg-${DateTime.now().microsecondsSinceEpoch}';

    setUp(() async {
      client = Client();
      await client.connect(Uri.parse('nats://localhost:4222'), retry: false);
      js = client.jetStream();
    });

    tearDown(() async {
      await client.close();
    });

    test('listStreams returns every stream past the first server page',
        () async {
      for (var i = 0; i < streamCount; i++) {
        await js.createStream(StreamConfig(
          name: '$prefix-$i',
          subjects: ['$prefix.$i'],
          storage: 'memory',
        ));
      }

      try {
        final streams =
            await js.listStreams(timeout: const Duration(seconds: 10));
        final ours = streams.where((s) => s.config.name.startsWith(prefix));
        expect(ours.length, equals(streamCount));
      } finally {
        for (var i = 0; i < streamCount; i++) {
          await js.deleteStream('$prefix-$i');
        }
      }
    }, timeout: const Timeout(Duration(minutes: 2)));

    test('listConsumers returns every consumer past the first server page',
        () async {
      final streamName = '$prefix-consumers';
      await js.createStream(StreamConfig(
        name: streamName,
        subjects: ['$streamName.>'],
        storage: 'memory',
      ));

      try {
        for (var i = 0; i < streamCount; i++) {
          await js.createConsumer(
              streamName, ConsumerConfig(durable: 'c-$i', ackPolicy: 'none'));
        }

        final consumers = await js.listConsumers(streamName,
            timeout: const Duration(seconds: 10));
        expect(consumers.length, equals(streamCount));
      } finally {
        await js.deleteStream(streamName);
      }
    }, timeout: const Timeout(Duration(minutes: 2)));
  });
}
