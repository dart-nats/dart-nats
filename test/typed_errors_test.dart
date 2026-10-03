import 'dart:async';

import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

import 'defect/fake_nats_server.dart';

void main() {
  group('server errors by type', () {
    test('a publish permissions violation names the subject', () {
      final e = NatsException.fromServerError(
          "'Permissions Violation for Publish to \"orders.new\"'");
      expect(e, isA<NatsPermissionsViolation>());
      e as NatsPermissionsViolation;
      expect(e.operation, NatsOperation.publish);
      expect(e.subject, 'orders.new');
      expect(e.queue, isNull);
      expect(e.message, contains('Permissions Violation'));
    });

    test('a subscription permissions violation names subject and queue', () {
      final e = NatsException.fromServerError(
          "'Permissions Violation for Subscription to \"orders.>\" "
          'using queue "workers"\'');
      expect(e, isA<NatsPermissionsViolation>());
      e as NatsPermissionsViolation;
      expect(e.operation, NatsOperation.subscribe);
      expect(e.subject, 'orders.>');
      expect(e.queue, 'workers');
    });

    test('authentication errors share one supertype', () {
      expect(NatsException.fromServerError("'Authorization Violation'"),
          isA<NatsAuthorizationViolation>());
      expect(NatsException.fromServerError("'User Authentication Expired'"),
          isA<NatsAuthenticationExpired>());
      expect(NatsException.fromServerError("'User Authentication Revoked'"),
          isA<NatsAuthenticationExpired>());
      for (final text in [
        "'Authorization Violation'",
        "'User Authentication Expired'",
        "'Authentication Timeout'",
      ]) {
        expect(NatsException.fromServerError(text),
            isA<NatsAuthenticationException>());
      }
    });

    test('the other recognised errors', () {
      expect(NatsException.fromServerError("'Stale Connection'"),
          isA<NatsStaleConnection>());
      expect(NatsException.fromServerError("'maximum connections exceeded'"),
          isA<NatsMaxConnectionsExceeded>());
      expect(NatsException.fromServerError("'Maximum Payload Violation'"),
          isA<NatsMaxPayloadViolation>());
    });

    test('an unrecognised error is a plain NatsException with its text', () {
      final e = NatsException.fromServerError("'Unknown Protocol Operation'");
      expect(e.runtimeType, NatsException);
      expect(e.toString(), "NatsException: 'Unknown Protocol Operation'");
    });
  });

  group('a denied publish', () {
    late FakeNatsServer server;
    late Client client;

    setUp(() async {
      server = FakeNatsServer(onLine: (line, reply) {
        if (line.toLowerCase().startsWith('pub denied ')) {
          reply("-ERR 'Permissions Violation for Publish to \"denied\"'\r\n");
        }
      });
      await server.start();
      client = Client();
      await client.connect(Uri.parse('nats://127.0.0.1:${server.port}'),
          retry: false);
    });

    tearDown(() async {
      await client.close();
      await server.stop();
    });

    test('fails its request at once, and only its request', () async {
      final reported = <Object>[];
      client.onError = (dynamic e) => reported.add(e as Object);

      final other = client.requestString('allowed', 'x',
          timeout: const Duration(seconds: 2));
      final otherOutcome = expectLater(other, throwsA(isA<TimeoutException>()));

      final watch = Stopwatch()..start();
      await expectLater(
        client.requestString('denied', 'x',
            timeout: const Duration(seconds: 10)),
        throwsA(isA<NatsPermissionsViolation>()
            .having((e) => e.subject, 'subject', 'denied')
            .having((e) => e.operation, 'operation', NatsOperation.publish)),
      );
      watch.stop();

      expect(watch.elapsed, lessThan(const Duration(seconds: 1)));
      expect(reported.single, isA<NatsPermissionsViolation>());
      expect(client.status, Status.connected);
      await otherOutcome;
    });
  });
}
