import 'dart:async';

import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

import 'fake_nats_server.dart';

/// connect() used to apply its arguments (server pool, retry settings, a new
/// connect completer) before checking whether the client was already in use.
/// A call it then rejected still rewired the live connection, and the
/// orphaned completer later failed with nobody listening.
void main() {
  test('a rejected connect() leaves the live connection untouched', () async {
    final server = FakeNatsServer();
    await server.start();
    final unused = FakeNatsServer();
    await unused.start();
    final unusedPort = unused.port;
    await unused.stop();

    final client = Client();
    Object? escaped;
    Object? failure;

    await runZonedGuarded(() async {
      try {
        await client.connect(
          Uri.parse('nats://127.0.0.1:${server.port}'),
          retry: true,
          retryCount: -1,
          retryInterval: 1,
          timeout: 2,
        );

        await expectLater(
          client.connect(Uri.parse('nats://127.0.0.1:$unusedPort'),
              retry: false),
          throwsA(predicate((e) => '$e'.contains('client in use'))),
        );

        // Still retrying, and still against the original server.
        final dropped =
            client.statusStream.firstWhere((s) => s == Status.disconnected);
        server.resetCounters();
        await server.restart();
        await dropped.timeout(const Duration(seconds: 5));
        await client.waitUntilConnected().timeout(const Duration(seconds: 10));
        expect(server.tcpAccepted, greaterThanOrEqualTo(1));

        await client.forceClose();
        await Future<void>.delayed(const Duration(milliseconds: 200));
      } catch (e) {
        // Errors stay in this zone; carry them out to fail the test.
        failure = e;
      }
    }, (error, stack) {
      escaped ??= error;
    });

    await client.forceClose();
    await server.stop();
    if (failure != null) fail('$failure');
    expect(escaped, isNull, reason: 'escaped to the zone: $escaped');
  });
}
