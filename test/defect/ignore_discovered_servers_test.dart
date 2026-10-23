import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

import 'fake_nats_server.dart';

/// `connect_urls` in INFO adds the server's peers to the pool, so a reconnect
/// can go to one of them. With `ignoreDiscoveredServers` the client only
/// reconnects to the servers it was given.
void main() {
  Future<void> reconnectAfterPrimaryStops({
    required bool ignoreDiscoveredServers,
  }) async {
    final peer = FakeNatsServer();
    await peer.start();
    final primary = FakeNatsServer(connectUrls: ['127.0.0.1:${peer.port}']);
    await primary.start();
    final client = Client();
    try {
      await client.connect(
        Uri.parse('nats://127.0.0.1:${primary.port}'),
        retry: true,
        retryCount: -1,
        retryInterval: 1,
        timeout: 2,
        ignoreDiscoveredServers: ignoreDiscoveredServers,
      );
      expect(client.status, Status.connected);

      final dropped =
          client.statusStream.firstWhere((s) => s == Status.disconnected);
      final primaryPort = primary.port;
      await primary.stop();
      await dropped.timeout(const Duration(seconds: 5));

      if (ignoreDiscoveredServers) {
        // Several retry intervals with only the stopped primary to try.
        await Future<void>.delayed(const Duration(seconds: 3));
        expect(peer.tcpAccepted, 0);
        await primary.start(primaryPort);
        await client.waitUntilConnected().timeout(const Duration(seconds: 10));
        expect(primary.tcpAccepted, greaterThan(0));
      } else {
        await client.waitUntilConnected().timeout(const Duration(seconds: 10));
        expect(peer.tcpAccepted, greaterThan(0));
      }
    } finally {
      await client.forceClose();
      await primary.stop();
      await peer.stop();
    }
  }

  group('ignoreDiscoveredServers', () {
    test('by default a reconnect can go to a discovered peer',
        () => reconnectAfterPrimaryStops(ignoreDiscoveredServers: false));

    test('when set, a reconnect only goes to the given server',
        () => reconnectAfterPrimaryStops(ignoreDiscoveredServers: true));
  });
}
