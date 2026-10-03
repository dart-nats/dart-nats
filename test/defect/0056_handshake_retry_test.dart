import 'dart:async';

import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

import 'fake_nats_server.dart';

/// Issue #56: a connect attempt counted as successful as soon as TCP was up.
/// Anything failing after that (the TLS handshake, -ERR, a server that never
/// sends INFO) was not retried on the first connect, and was retried in a
/// tight loop that ignored retryInterval and retryCount after a reconnect.
/// An attempt now succeeds only once the handshake reaches connected.
void main() {
  /// Asserts the attempts were spaced by about [interval], not back to back.
  void expectSpaced(List<DateTime> times, Duration interval) {
    for (var i = 1; i < times.length; i++) {
      expect(times[i].difference(times[i - 1]),
          greaterThanOrEqualTo(interval * 0.8),
          reason: 'attempts $i-1 and $i');
    }
  }

  group('0056 handshake failures are retried like failed connects', () {
    test('first connect with a bad certificate retries until it is accepted',
        () async {
      final server = FakeNatsServer(tls: true);
      await server.start();
      final client = Client(); // acceptBadCert = false
      final reported = <Object>[];
      client.onError = (dynamic e) => reported.add(e as Object);
      try {
        await expectNoEscape(() async {
          final connected = client.connect(
            Uri.parse('tls://127.0.0.1:${server.port}'),
            retry: true,
            retryCount: -1,
            retryInterval: 1,
            timeout: 2,
          );
          await Future<void>.delayed(const Duration(milliseconds: 2500));
          expect(server.tcpAccepted, inInclusiveRange(3, 4));
          expectSpaced(server.acceptTimes, const Duration(seconds: 1));
          expect(reported.map((e) => '$e'),
              everyElement(contains('TLS handshake failed')));

          client.acceptBadCert = true; // e.g. the certificate got fixed
          await connected.timeout(const Duration(seconds: 5));
          expect(client.status, Status.connected);
        });
      } finally {
        await client.forceClose();
        await server.stop();
      }
    });

    test('without retry, connect() fails with the TLS error', () async {
      final server = FakeNatsServer(tls: true);
      await server.start();
      final client = Client();
      try {
        await expectNoEscape(() async {
          await expectLater(
            client.connect(Uri.parse('tls://127.0.0.1:${server.port}'),
                retry: false, timeout: 2),
            throwsA(predicate((e) => '$e'.contains('TLS handshake failed'))),
          );
          expect(server.tcpAccepted, 1);
        });
      } finally {
        await client.forceClose();
        await server.stop();
      }
    });

    test('reconnect against a failing certificate honours retry settings',
        () async {
      final server = FakeNatsServer(tls: true);
      await server.start();
      final client = Client()..acceptBadCert = true;
      var disconnects = 0;
      client.onDisconnect = () => disconnects++;
      try {
        await expectNoEscape(() async {
          await client.connect(
            Uri.parse('tls://127.0.0.1:${server.port}'),
            retry: true,
            retryCount: 3,
            retryInterval: 1,
            timeout: 2,
          );
          client.acceptBadCert = false;
          server.resetCounters();
          await server.restart();

          await client
              .waitUntil(Status.closed)
              .timeout(const Duration(seconds: 10));
          // Before the fix: thousands of connects within seconds.
          expect(server.tcpAccepted, 3);
          expectSpaced(server.acceptTimes, const Duration(seconds: 1));
          expect(disconnects, 1,
              reason: 'one real disconnect, not one per try');
        });
      } finally {
        await client.forceClose();
        await server.stop();
      }
    });

    test('a server that never sends INFO times out and is retried', () async {
      final server = FakeNatsServer(sendInfo: false);
      await server.start();
      final client = Client();
      try {
        await expectNoEscape(() async {
          await expectLater(
            client.connect(Uri.parse('nats://127.0.0.1:${server.port}'),
                retry: true, retryCount: 2, retryInterval: 1, timeout: 1),
            throwsA(predicate((e) => '$e'.contains('no handshake'))),
          );
          expect(server.tcpAccepted, 2);
        });
      } finally {
        await client.forceClose();
        await server.stop();
      }
    });

    test('-ERR during the handshake is retried with retryInterval', () async {
      final server =
          FakeNatsServer(errOnAccept: 'maximum connections exceeded');
      await server.start();
      final client = Client();
      try {
        await expectNoEscape(() async {
          await expectLater(
            client.connect(Uri.parse('nats://127.0.0.1:${server.port}'),
                retry: true, retryCount: 3, retryInterval: 1, timeout: 2),
            throwsA(predicate(
                (e) => '$e'.contains('maximum connections exceeded'))),
          );
          expect(server.tcpAccepted, 3);
          expectSpaced(server.acceptTimes, const Duration(seconds: 1));
        });
      } finally {
        await client.forceClose();
        await server.stop();
      }
    });
  });
}
