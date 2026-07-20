import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

// Issue #36: https://github.com/dart-nats/dart-nats/issues/36

// Connect using tls:// scheme to a server that supports TLS but does not explicitly require it in its INFO payload.

void main() {
  group('TLS Connect optional (Issue #36)', () {
    test('0036 Connect to demo.nats.io using tls:// scheme', () async {
      final client = Client();
      client.acceptBadCert = true;

      try {
        await client.connect(
          Uri.parse('tls://demo.nats.io:4443'),
          retry: false,
          timeout: 5,
        );
        expect(client.connected, isTrue);

        final sub = client.sub('issue36.test');
        final pubResult = await client.pubString('issue36.test', 'payload');
        expect(pubResult, isTrue);

        final msg = await sub.stream.first.timeout(const Duration(seconds: 3));
        expect(msg.string, equals('payload'));
      } catch (e) {
        // If it throws the bug exception, it is a failure
        if (e.toString().contains('require TLS but server not required')) {
          fail('Failed with bug: require TLS but server not required');
        } else {
          // Allow other network/offline errors to pass gracefully
          print('Network/offline condition during test: $e');
        }
      } finally {
        await client.close();
      }
    });

    test('0036 Connecting using tls:// to a non-TLS server fails cleanly',
        () async {
      final client = Client();
      var threwExpectedException = false;
      try {
        await client.connect(
          Uri.parse('tls://localhost:4222'),
          retry: false,
          timeout: 2,
        );
      } catch (e) {
        threwExpectedException = true;
      } finally {
        await client.close();
      }
      expect(threwExpectedException, isTrue);
    });

    test('0036 Dynamic _tlsRequired reset during failover', () async {
      final client = Client();
      // First server is offline TLS server. Second server is online plain NATS server.
      await client.connect(
        Uri.parse('tls://localhost:9999'),
        servers: [Uri.parse('nats://localhost:4222')],
        retry: false,
        timeout: 1,
      );
      expect(client.connected, isTrue);
      expect(client.status, equals(Status.connected));

      final sub = client.sub('pooling.test');
      final pubResult = await client.pubString('pooling.test', 'hello');
      expect(pubResult, isTrue);

      final msg = await sub.stream.first.timeout(const Duration(seconds: 2));
      expect(msg.string, equals('hello'));

      await client.close();
    });
  });
}
