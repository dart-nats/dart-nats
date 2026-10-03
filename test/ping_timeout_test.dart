import 'dart:async';

import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

import 'defect/fake_nats_server.dart';

/// A connection that dies without a close -- a backgrounded app, a dropped
/// radio -- gives no error on write. Only an unanswered PING shows it.
void main() {
  late FakeNatsServer server;

  setUp(() async {
    server = FakeNatsServer();
    await server.start();
  });

  tearDown(() async {
    await server.stop();
  });

  Uri uri() => Uri.parse('nats://127.0.0.1:${server.port}');

  /// Connects, then lets the server go silent, and returns how long the
  /// client took to report the connection gone.
  Future<Duration> timeToDisconnect(Client client,
      {Duration? pingTimeout}) async {
    await client.connect(uri(),
        retry: false,
        pingInterval: const Duration(seconds: 1),
        maxPingsOut: 1,
        pingTimeout: pingTimeout);
    server.answerPing = false;
    final watch = Stopwatch()..start();
    await client.statusStream
        .firstWhere((s) => s == Status.disconnected)
        .timeout(const Duration(seconds: 5));
    return watch.elapsed;
  }

  group('ping(timeout:)', () {
    test('completes when the server answers', () async {
      final client = Client();
      await client.connect(uri(), retry: false);
      await client.ping(timeout: const Duration(seconds: 1));
      await client.close();
    });

    test('fails with TimeoutException when no PONG arrives', () async {
      final client = Client();
      await client.connect(uri(), retry: false);
      server.answerPing = false;
      await expectNoEscape(() async {
        await expectLater(
          client.ping(timeout: const Duration(milliseconds: 300)),
          throwsA(isA<TimeoutException>()),
        );
        // Closing fails the abandoned PING; nothing may escape from it.
        await client.close();
      });
    });
  });

  group('heartbeat', () {
    test('without pingTimeout, death is noticed on the next tick', () async {
      final client = Client();
      final elapsed = await timeToDisconnect(client);
      expect(elapsed, greaterThan(const Duration(milliseconds: 1800)));
      await client.close();
    });

    test('with pingTimeout, death is noticed at the deadline', () async {
      final client = Client();
      final elapsed = await timeToDisconnect(client,
          pingTimeout: const Duration(milliseconds: 300));
      expect(elapsed, greaterThan(const Duration(milliseconds: 1200)));
      expect(elapsed, lessThan(const Duration(milliseconds: 1700)));
      await client.close();
    });

    test('pingTimeout leaves an answering connection alone', () async {
      final client = Client();
      await client.connect(uri(),
          retry: false,
          pingInterval: const Duration(milliseconds: 300),
          maxPingsOut: 1,
          pingTimeout: const Duration(milliseconds: 100));
      await Future<void>.delayed(const Duration(milliseconds: 1500));
      expect(client.status, Status.connected);
      await client.close();
    });
  });
}
