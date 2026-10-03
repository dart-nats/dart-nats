import 'dart:async';
import 'dart:io';

import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

import 'fake_nats_server.dart';

/// The secure socket's error handler used to rethrow a TlsException. An
/// exception thrown from a stream error handler has no caller to reach, so it
/// always escaped as an unhandled zone error.
void main() {
  test('a TLS error on an established connection does not escape', () async {
    final server = FakeNatsServer(tls: true);
    await server.start();
    final proxy = TcpProxy(server.port);
    await proxy.start();

    final client = Client()..acceptBadCert = true;
    final reported = <Object>[];
    client.onError = (dynamic e) => reported.add(e as Object);
    Object? escaped;
    Object? failure;

    await runZonedGuarded(() async {
      try {
        await client.connect(
          Uri.parse('tls://127.0.0.1:${proxy.port}'),
          retry: true,
          retryCount: -1,
          retryInterval: 1,
          timeout: 2,
        );
        expect(client.status, Status.connected);

        // Not a valid TLS record: the client's SecureSocket fails to decode it.
        proxy.inject(List<int>.filled(64, 0x17));
        await client
            .waitUntil(Status.closed)
            .timeout(const Duration(seconds: 5));
        await Future<void>.delayed(const Duration(milliseconds: 200));
      } catch (e) {
        // Errors stay in this zone; carry them out to fail the test.
        failure = e;
      }
    }, (error, stack) {
      escaped ??= error;
    });

    await client.forceClose();
    await proxy.stop();
    await server.stop();

    if (failure != null) fail('$failure');
    expect(escaped, isNull, reason: 'escaped to the zone: $escaped');
    expect(reported.whereType<TlsException>(), isNotEmpty,
        reason: 'onError got: $reported');
    expect(client.status, Status.closed);
  });
}
