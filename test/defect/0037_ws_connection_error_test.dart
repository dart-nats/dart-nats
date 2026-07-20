import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

// Issue #37: https://github.com/dart-nats/dart-nats/issues/37

// WebSocket connection failure should be caught gracefully instead of causing unhandled asynchronous exceptions.

void main() {
  group('WS Connection Error (Issue #37)', () {
    test(
        '0037 WS connection failure is handled gracefully without unhandled exceptions',
        () async {
      final client = Client();
      var threwExpectedException = false;

      try {
        print('--- Test Start Connect ---');
        await client.connect(
          Uri.parse('ws://localhost:9999'),
          retry: false,
          timeout: 2,
        );
        print('--- Test End Connect Success ---');
      } catch (e) {
        print('--- Test Caught Exception: $e ---');
        threwExpectedException = true;
      } finally {
        print('--- Test Finally Close ---');
        await client.close();
      }

      expect(threwExpectedException, isTrue);
    });

    test('0037 WS connection retry exhausted throws NatsException gracefully',
        () async {
      final client = Client();
      final statusHistory = <Status>[];
      client.statusStream.listen((s) {
        statusHistory.add(s);
      });

      var threwExpectedException = false;
      try {
        await client.connect(
          Uri.parse('ws://localhost:9999'),
          retry: true,
          retryCount: 2,
          retryInterval: 1,
          timeout: 1,
        );
      } catch (e) {
        threwExpectedException = true;
        expect(e, isA<NatsException>());
      } finally {
        await client.close();
      }

      expect(threwExpectedException, isTrue);
      expect(statusHistory, contains(Status.connecting));
      expect(statusHistory, contains(Status.reconnecting));
      expect(statusHistory, contains(Status.closed));
    });

    test('0037 WS server pooling failover handles offline servers gracefully',
        () async {
      final client = Client();
      var threwExpectedException = false;
      try {
        await client.connect(
          Uri.parse('ws://localhost:9999'),
          servers: [Uri.parse('ws://localhost:9998')],
          retry: false,
          timeout: 1,
        );
      } catch (e) {
        threwExpectedException = true;
        expect(e, isA<NatsException>());
      } finally {
        await client.close();
      }

      expect(threwExpectedException, isTrue);
    });
  });
}
