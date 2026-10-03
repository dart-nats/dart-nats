import 'dart:io';

import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

import 'fake_nats_server.dart';

/// The secure socket's error handler used to rethrow a TlsException. An
/// exception thrown from a stream error handler has no caller to reach, so it
/// always escaped as an unhandled zone error. Since #56 such an error is an
/// ordinary disconnect, retried like any other.
void main() {
  test('a TLS error on an established connection is reported and retried',
      () async {
    final server = FakeNatsServer(tls: true);
    await server.start();
    final proxy = TcpProxy(server.port);
    await proxy.start();

    final client = Client()..acceptBadCert = true;
    final reported = <Object>[];
    client.onError = (dynamic e) => reported.add(e as Object);

    try {
      await expectNoEscape(() async {
        await client.connect(
          Uri.parse('tls://127.0.0.1:${proxy.port}'),
          retry: true,
          retryCount: -1,
          retryInterval: 1,
          timeout: 2,
        );
        expect(client.status, Status.connected);

        final dropped =
            client.statusStream.firstWhere((s) => s == Status.disconnected);
        // Not a valid TLS record: the client's SecureSocket fails to decode it.
        proxy.inject(List<int>.filled(64, 0x17));
        await dropped.timeout(const Duration(seconds: 5));
        await client.waitUntilConnected().timeout(const Duration(seconds: 10));
      });
    } finally {
      await client.forceClose();
      await proxy.stop();
      await server.stop();
    }

    expect(reported.whereType<TlsException>(), isNotEmpty,
        reason: 'onError got: $reported');
  });
}
