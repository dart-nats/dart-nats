import 'dart:async';

import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

import 'defect/fake_nats_server.dart';

/// A request the connection lost is one of two things: never sent, which is
/// always safe to send again, or sent and unanswered, which is not. The
/// exception has to say which.
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

  test('a request on a client that is not connected fails as unsent', () async {
    final client = Client();
    await expectLater(
        client.requestString('orders.place', 'x'),
        throwsA(isA<NatsConnectionLost>()
            .having((e) => e.sent, 'sent', isFalse)
            .having((e) => e.subject, 'subject', 'orders.place')));
    expect(client.pendingRequestCount, 0);
  });

  test('a request in flight when the connection drops fails as sent', () async {
    final client = Client();
    await client.connect(uri(), retry: false);
    final watch = Stopwatch()..start();
    final outcome = expectLater(
        client.requestString('orders.place', 'x',
            timeout: const Duration(seconds: 10)),
        throwsA(isA<NatsConnectionLost>()
            .having((e) => e.sent, 'sent', isTrue)
            .having((e) => e.subject, 'subject', 'orders.place')));
    await Future<void>.delayed(const Duration(milliseconds: 100));
    await server.stop();
    await outcome;
    expect(watch.elapsed, lessThan(const Duration(seconds: 2)));
    await client.close();
  });

  test('closing the client fails a request in flight as sent', () async {
    final client = Client();
    await client.connect(uri(), retry: false);
    final outcome = expectLater(
        client.requestString('orders.place', 'x',
            timeout: const Duration(seconds: 10)),
        throwsA(
            isA<NatsConnectionLost>().having((e) => e.sent, 'sent', isTrue)));
    await Future<void>.delayed(const Duration(milliseconds: 100));
    await client.close();
    await outcome;
  });

  test('pendingRequestCount follows the requests in flight', () async {
    final client = Client();
    await client.connect(uri(), retry: false);
    expect(client.pendingRequestCount, 0);

    final first = client.requestString('nobody.listens', 'a',
        timeout: const Duration(milliseconds: 300));
    final second = client.requestString('nobody.listens', 'b',
        timeout: const Duration(seconds: 10));
    unawaited(first.then<void>((_) {}, onError: (Object _) {}));
    unawaited(second.then<void>((_) {}, onError: (Object _) {}));
    await Future<void>.delayed(const Duration(milliseconds: 100));
    expect(client.pendingRequestCount, 2);

    await expectLater(first, throwsA(isA<TimeoutException>()));
    expect(client.pendingRequestCount, 1);

    await client.close();
    await expectLater(second, throwsA(isA<NatsConnectionLost>()));
    expect(client.pendingRequestCount, 0);
  });
}
