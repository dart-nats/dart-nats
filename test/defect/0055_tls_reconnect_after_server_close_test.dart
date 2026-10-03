import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

import 'fake_nats_server.dart';

/// Issue #55: after the server closed an established TLS connection, the
/// client kept a reference to the dead secure socket. The next attempt's TCP
/// listener only forwards data while there is no secure socket, so the new
/// INFO was dropped, the TLS upgrade never started and the client never got
/// back to connected.
void main() {
  Future<void> reconnectsAfterRestart(String scheme) async {
    final server = FakeNatsServer(tls: true);
    await server.start();
    final client = Client()..acceptBadCert = true;
    try {
      await client.connect(
        Uri.parse('$scheme://127.0.0.1:${server.port}'),
        retry: true,
        retryCount: -1,
        retryInterval: 1,
        timeout: 2,
      );
      expect(client.status, Status.connected);

      final dropped =
          client.statusStream.firstWhere((s) => s == Status.disconnected);
      server.resetCounters();
      await server.restart();
      await dropped.timeout(const Duration(seconds: 5));
      await client.waitUntilConnected().timeout(const Duration(seconds: 10));

      expect(client.status, Status.connected);
      expect(server.tlsHandshakes, 1);
    } finally {
      await client.forceClose();
      await server.stop();
    }
  }

  group('0055 TLS reconnect after the server closes the connection', () {
    test('tls:// client reconnects', () => reconnectsAfterRestart('tls'));

    test('nats:// client of a tls_required server reconnects',
        () => reconnectsAfterRestart('nats'));
  });
}
