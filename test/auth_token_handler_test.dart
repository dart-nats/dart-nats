import 'dart:async';

import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

// The nats-token server of docker-compose.yml: `--auth mytoken`.
final _tokenServer = Uri.parse('nats://localhost:4224');

void main() {
  group('authTokenHandler', () {
    test('supplies the token on connect and again on every reconnect',
        () async {
      var client = Client();
      var calls = 0;
      client.authTokenHandler = () {
        calls++;
        return 'mytoken';
      };
      await client.connect(_tokenServer, retryInterval: 1);
      expect(calls, equals(1));

      var reconnected = client.statusStream
          .firstWhere((s) => s == Status.connected)
          .timeout(Duration(seconds: 10));
      await client.tcpClose();
      await reconnected;

      expect(calls, equals(2));
      expect(await client.pubString('subject1', 'message1', buffer: false),
          isTrue);
      await client.close();
    });

    test('may be asynchronous, and overrides ConnectOption.authToken',
        () async {
      var client = Client();
      client.authTokenHandler = () async {
        await Future<void>.delayed(Duration(milliseconds: 50));
        return 'mytoken';
      };
      await client.connect(_tokenServer,
          retry: false, connectOption: ConnectOption(authToken: 'stale'));
      expect(client.status, equals(Status.connected));
      await client.close();
    });

    test('a handler that throws is a failed attempt, and is retried', () async {
      var client = Client();
      var calls = 0;
      var reported = <Object>[];
      client.onError = (dynamic e) => reported.add(e as Object);
      client.authTokenHandler = () {
        calls++;
        if (calls == 1) throw StateError('token service unavailable');
        return 'mytoken';
      };
      await client.connect(_tokenServer,
          retry: true, retryCount: 3, retryInterval: 1);

      expect(calls, equals(2));
      expect(client.status, equals(Status.connected));
      expect(reported.whereType<StateError>(), hasLength(1));
      await client.close();
    });

    test('a token the server refuses still closes the client', () async {
      var client = Client();
      client.authTokenHandler = () => 'wrong';
      await expectLater(
        client.connect(_tokenServer, retry: false),
        throwsA(isA<NatsException>()),
      );
      expect(client.status, isNot(equals(Status.connected)));
      await client.close();
    });
  });
}
