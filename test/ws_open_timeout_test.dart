import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

import 'defect/fake_nats_server.dart';

/// `connect(timeout:)` bounded the TCP dial of `nats://` and `tls://` and,
/// since 1.5.0, the handshake -- but not the WebSocket open. A peer that
/// accepts the connection and never completes the upgrade hung the attempt
/// for good, so not even `retry` moved on.
void main() {
  group('a websocket open that never completes', () {
    late FakeNatsServer server;

    setUp(() async {
      // Accepts TCP and says nothing: the upgrade request goes unanswered.
      server = FakeNatsServer(sendInfo: false);
      await server.start();
    });

    tearDown(() async {
      await server.stop();
    });

    test('fails connect() within the timeout', () async {
      final client = Client();
      final watch = Stopwatch()..start();
      await expectNoEscape(() async {
        await expectLater(
          client.connect(Uri.parse('ws://127.0.0.1:${server.port}'),
              retry: false, timeout: 1),
          throwsA(predicate((e) => '$e'.contains('no websocket open'))),
        );
      });
      watch.stop();
      expect(watch.elapsed, lessThan(const Duration(seconds: 3)));
      expect(client.isClosedAndCleaned, isTrue);
      await client.close();
    });

    test('is retried like any other failed attempt', () async {
      final client = Client();
      final reported = <Object>[];
      client.onError = (dynamic e) => reported.add(e as Object);
      await expectNoEscape(() async {
        await expectLater(
          client.connect(Uri.parse('ws://127.0.0.1:${server.port}'),
              retry: true, retryCount: 2, retryInterval: 1, timeout: 1),
          throwsA(isA<NatsException>()),
        );
      });
      expect(server.tcpAccepted, 2);
      expect(reported, hasLength(2));
      await client.close();
    });
  });
}
