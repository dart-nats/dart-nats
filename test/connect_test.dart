import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

void main() {
  group('all', () {
    test('ws', () async {
      var client = Client();
      unawaited(
          client.connect(Uri.parse('ws://localhost:8080'), retryInterval: 1));
      var sub = client.sub('subject1');
      client.pub('subject1', Uint8List.fromList('message1'.codeUnits));
      var msg = await sub.stream.first;
      await client.close();
      expect(String.fromCharCodes(msg.byte), equals('message1'));
    });
    test('nats:', () async {
      var client = Client();
      unawaited(
          client.connect(Uri.parse('nats://localhost:4222'), retryInterval: 1));
      var sub = client.sub('subject1');
      client.pub('subject1', Uint8List.fromList('message1'.codeUnits));
      var msg = await sub.stream.first;
      await client.close();
      expect(String.fromCharCodes(msg.byte), equals('message1'));
    });
    test('await', () async {
      var client = Client();
      await client.connect(Uri.parse('nats://localhost'));
      var sub = client.sub('subject1');
      var result = await client.pub(
          'subject1', Uint8List.fromList('message1'.codeUnits),
          buffer: false);
      expect(result, true);

      var msg = await sub.stream.first;
      await client.close();
      expect(String.fromCharCodes(msg.byte), equals('message1'));
    });
    test('retry false', () async {
      var client = Client();
      await client.connect(Uri.parse('nats://localhost'), retry: false);
      var sub = client.sub('subject1');
      var result = await client.pub(
          'subject1', Uint8List.fromList('message1'.codeUnits),
          buffer: false);
      expect(result, true);

      var msg = await sub.stream.first;
      await client.close();
      expect(String.fromCharCodes(msg.byte), equals('message1'));
    });
    test('reconnect', () async {
      var client = Client();
      await client.connect(Uri.parse('nats://localhost'));
      var sub = client.sub('subject1');
      var result = await client.pub(
          'subject1', Uint8List.fromList('message1'.codeUnits),
          buffer: false);
      expect(result, true);

      var msg = await sub.stream.first;
      await client.close();
      expect(String.fromCharCodes(msg.byte), equals('message1'));

      await client.connect(Uri.parse('nats://localhost'));
      result = await client.pub(
          'subject1', Uint8List.fromList('message2'.codeUnits),
          buffer: false);
      expect(result, true);
      msg = await sub.stream.first;
      expect(String.fromCharCodes(msg.byte), equals('message2'));
    });
    test('retry background', () async {
      var client = Client();
      unawaited(client.connect(
        Uri.parse('nats://localhost'),
        retry: true,
        retryCount: -1,
        timeout: 2,
        retryInterval: 2,
      ));

      await client.waitUntilConnected();
      var sub = client.sub('subject1');
      var result = await client.pub(
          'subject1', Uint8List.fromList('message1'.codeUnits),
          buffer: false);
      expect(result, true);

      var msg = await sub.stream.first;
      expect(String.fromCharCodes(msg.byte), equals('message1'));

      await client.tcpClose();
      await client.waitUntilConnected();

      result = await client.pub(
          'subject1', Uint8List.fromList('message2'.codeUnits),
          buffer: false);
      expect(result, true);
      msg = await sub.stream.first;
      expect(String.fromCharCodes(msg.byte), equals('message2'));
      await client.close();
    });

    test('background reconnect retries after failed Socket.connect', () async {
      var client = Client();
      var connectCalls = 0;
      var onConnectCount = 0;
      var onReconnectCount = 0;
      final attemptTimes = <DateTime>[];

      client.onConnect = () => onConnectCount++;
      client.onReconnect = () => onReconnectCount++;

      await client.connect(
        Uri.parse('nats://localhost:4222'),
        retry: true,
        retryCount: -1,
        timeout: 2,
        retryInterval: 1,
      );
      expect(client.status, equals(Status.connected));
      expect(onConnectCount, equals(1));
      expect(onReconnectCount, equals(0));

      const failCount = 3;
      client.debugSocketConnect = (host, port, {timeout}) async {
        connectCalls++;
        attemptTimes.add(DateTime.now());
        if (connectCalls <= failCount) {
          throw Exception('Simulated offline connect failure');
        }
        return Socket.connect(host, port, timeout: timeout);
      };

      await client.tcpClose();
      await client.waitUntilConnected().timeout(const Duration(seconds: 15));

      expect(client.status, equals(Status.connected));
      expect(connectCalls, greaterThan(failCount));
      expect(onReconnectCount, equals(1));
      expect(onConnectCount, equals(1));

      // retryInterval is 1s; failures should be spaced by ~1s (not busy-spin).
      expect(attemptTimes.length, greaterThanOrEqualTo(2));
      final gap = attemptTimes[1].difference(attemptTimes[0]);
      expect(gap.inMilliseconds, greaterThanOrEqualTo(800));

      var sub = client.sub('reconnect_fail_then_ok');
      await client.pub(
          'reconnect_fail_then_ok', Uint8List.fromList('ok'.codeUnits));
      var msg = await sub.stream.first.timeout(const Duration(seconds: 5));
      expect(String.fromCharCodes(msg.byte), equals('ok'));

      client.debugSocketConnect = null;
      await client.close();
    });

    test('successful reconnect fires onReconnect not onConnect', () async {
      var client = Client();
      var onConnectCount = 0;
      var onReconnectCount = 0;
      client.onConnect = () => onConnectCount++;
      client.onReconnect = () => onReconnectCount++;

      await client.connect(
        Uri.parse('nats://localhost:4222'),
        retry: true,
        retryCount: -1,
        retryInterval: 1,
      );
      expect(onConnectCount, equals(1));
      expect(onReconnectCount, equals(0));

      await client.tcpClose();
      await client.waitUntilConnected();

      expect(client.status, equals(Status.connected));
      expect(onReconnectCount, equals(1));
      expect(onConnectCount, equals(1));

      await client.close();
    });
    test('status stream', () async {
      var client = Client();
      var statusHistory = <Status>[];
      client.statusStream.listen((s) {
        // print(s);
        statusHistory.add(s);
      });
      await client.connect(Uri.parse('ws://localhost:8080'));
      await client.close();

      // no runtime error should be fine
      // expect only first and last status
      expect(statusHistory.first, equals(Status.connecting));
      expect(statusHistory.last, equals(Status.closed));
    });
    test('status stream detail', () async {
      var client = Client();
      var statusHistory = <Status>[];
      client.statusStream.listen((s) {
        // print(s);
        statusHistory.add(s);
      });
      await client.connect(
        Uri.parse('ws://localhost:8080'),
        retry: true,
        retryCount: 3,
        retryInterval: 1,
      );
      await client.close();

      // no runtime error should be fine
      // expect only first and last status
      expect(statusHistory.length, equals(4));
      expect(statusHistory[0], equals(Status.connecting));
      expect(statusHistory[1], equals(Status.infoHandshake));
      expect(statusHistory[2], equals(Status.connected));
      expect(statusHistory[3], equals(Status.closed));
    });
    test('status stream retry fail', () async {
      var client = Client();
      var statusHistory = <Status>[];
      try {
        client.statusStream.listen((s) {
          print(s);
          statusHistory.add(s);
        });
        await client.connect(
          Uri.parse('nats://localhost:1234'),
          retry: true,
          retryCount: 3,
          retryInterval: 1,
        );
        print('after connect');
      } on NatsException {
        //
      } on WebSocketChannelException {
        //
      } catch (e) {
        //
      }
      await client.close();

      // no runtime error should be fine
      // expect only first and last status
      expect(statusHistory.length, equals(4));
      expect(statusHistory[0], equals(Status.connecting));
      expect(statusHistory[1], equals(Status.reconnecting));
      expect(statusHistory[2], equals(Status.reconnecting));
      expect(statusHistory[3], equals(Status.closed));
    });

    test('observe client.status getter during connection lifecycle', () async {
      var client = Client();
      expect(client.status, equals(Status.disconnected));

      final connectFuture = client.connect(Uri.parse('nats://localhost:4222'));
      expect(client.status, equals(Status.connecting));

      await connectFuture;
      expect(client.status, equals(Status.connected));

      await client.close();
      expect(client.status, equals(Status.closed));
    });

    test('ensure no connection socket leak after close', () async {
      // Test for TCP connection
      var clientTcp = Client();
      expect(clientTcp.isClosedAndCleaned, isTrue);

      await clientTcp.connect(Uri.parse('nats://localhost:4222'));
      expect(clientTcp.isClosedAndCleaned, isFalse);

      await clientTcp.close();
      expect(clientTcp.isClosedAndCleaned, isTrue);

      // Test for WebSocket connection
      var clientWs = Client();
      expect(clientWs.isClosedAndCleaned, isTrue);

      await clientWs.connect(Uri.parse('ws://localhost:8080'));
      expect(clientWs.isClosedAndCleaned, isFalse);

      await clientWs.close();
      expect(clientWs.isClosedAndCleaned, isTrue);
    });

    test('reconnect to valid server after connection failure to invalid server',
        () async {
      var client = Client();

      // 1. Attempt connection to a non-existent WebSocket NATS server
      try {
        await client.connect(
          Uri.parse('ws://localhost:54321'), // invalid port
          retry: true,
          retryCount: 2,
          retryInterval: 1,
          timeout: 1,
        );
      } catch (e) {
        // Expect to fail
      }

      // 2. Call close() to reset/clean up the client's internal status from used/failed state
      await client.close();

      // 3. Reconnect to the valid local NATS server (port 8080)
      await client.connect(
        Uri.parse('ws://localhost:8080'),
        retry: false,
      );

      expect(client.status, equals(Status.connected));

      // 4. Verify that publish/subscribe works after reconnecting
      var sub = client.sub('reconnect_test_subject');
      await client.pub(
          'reconnect_test_subject', Uint8List.fromList('test_data'.codeUnits));
      var msg = await sub.stream.first;
      expect(String.fromCharCodes(msg.byte), equals('test_data'));

      await client.close();
    });

    test('connection close logic and state transition', () async {
      var client = Client();
      await client.connect(Uri.parse('nats://localhost:4222'), retry: false);
      expect(client.status, equals(Status.connected));
      expect(client.connected, isTrue);

      // Verify pending ping is completed with error upon close
      var pingFuture = client.ping();

      // Expect that pingFuture will throw NatsException when closed.
      // This is registered before close() is called to avoid unhandled async errors in the test Zone.
      expect(pingFuture, throwsA(isA<NatsException>()));

      await client.close();
      expect(client.status, equals(Status.closed));
      expect(client.connected, isFalse);
      expect(client.isClosedAndCleaned, isTrue);

      // Subsequent operations like request should throw NatsException
      expect(
        client.request('subj', Uint8List(0)),
        throwsA(isA<NatsException>()),
      );

      // Subsequent unbuffered pub should return false
      var pubResult = await client.pub('subj', Uint8List(0), buffer: false);
      expect(pubResult, isFalse);
    });

    test('connection forceClose cancels retries and closes', () async {
      var client = Client();
      // Connect to a valid server with retry enabled (will block/loop in statusStream await)
      var connectFuture = client.connect(
        Uri.parse('nats://localhost:4222'),
        retry: true,
        retryCount: -1,
        retryInterval: 1,
      );

      await client.waitUntilConnected();
      expect(client.status, equals(Status.connected));

      await client.forceClose();
      expect(client.status, equals(Status.closed));
      expect(client.isClosedAndCleaned, isTrue);

      // The connectFuture should complete successfully (emits null) when closed
      await expectLater(connectFuture, completes);

      // Wait a bit to ensure it doesn't trigger reconnect and remains closed
      await Future<void>.delayed(Duration(seconds: 2));
      expect(client.status, equals(Status.closed));
    });

    test('connection drain gracefully receives pending messages and closes',
        () async {
      var client = Client();
      await client.connect(Uri.parse('nats://localhost:4222'), retry: false);

      var sub = client.sub('drain_test');

      // Publish messages
      await client.pubString('drain_test', 'msg1');
      await client.pubString('drain_test', 'msg2');
      await client.pubString('drain_test', 'msg3');

      // Collect received messages and track stream completion
      var received = <String>[];
      var streamClosed = Completer<void>();

      sub.stream.listen(
        (msg) {
          received.add(msg.string);
        },
        onDone: () {
          streamClosed.complete();
        },
      );

      // Call drain
      await client.drain();

      // The client should be closed
      expect(client.status, equals(Status.closed));
      expect(client.isClosedAndCleaned, isTrue);

      // The subscription stream should complete (onDone is called)
      await streamClosed.future.timeout(Duration(seconds: 5));

      // Verify all published messages were received before stream closure
      expect(received, equals(['msg1', 'msg2', 'msg3']));
    });
  });
}
