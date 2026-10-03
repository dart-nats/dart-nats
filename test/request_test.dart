import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

void main() {
  group('all', () {
    test('simple', () async {
      var client = Client();
      await client.connect(Uri.parse('ws://localhost:8080'), retryInterval: 1);
      var sub = client.sub('subject1');
      client.pub('subject1', Uint8List.fromList('message1'.codeUnits));
      var msg = await sub.stream.first;
      await client.close();
      expect(String.fromCharCodes(msg.byte), equals('message1'));
    });
    test('respond', () async {
      var server = Client();
      await server.connect(Uri.parse('ws://localhost:8080'));
      var service = server.sub('service');
      service.stream.listen((m) {
        m.respondString('respond');
      });

      var requester = Client();
      await requester.connect(Uri.parse('ws://localhost:8080'));
      var inbox = newInbox();
      var inboxSub = requester.sub(inbox);

      requester.pubString('service', 'request', replyTo: inbox);

      var receive = await inboxSub.stream.first;

      await requester.close();
      await server.close();
      expect(receive.string, equals('respond'));
    });
    test('request', () async {
      var server = Client();
      await server.connect(Uri.parse('ws://localhost:8080'));
      var service = server.sub('service');
      unawaited(service.stream.first.then((m) {
        m.respond(Uint8List.fromList('respond'.codeUnits));
      }));

      var client = Client();
      await client.connect(Uri.parse('ws://localhost:8080'));
      var receive = await client.request(
          'service', Uint8List.fromList('request'.codeUnits));

      await client.close();
      await server.close();
      expect(receive.string, equals('respond'));
    });
    test('custom inbox', () async {
      var server = Client();
      await server.connect(Uri.parse('ws://localhost:8080'));
      var service = server.sub('service');
      unawaited(service.stream.first.then((m) {
        m.respond(Uint8List.fromList('respond'.codeUnits));
      }));

      var client = Client();
      client.inboxPrefix = '_INBOX.test_test';
      await client.connect(Uri.parse('ws://localhost:8080'));
      var receive = await client.request(
          'service', Uint8List.fromList('request'.codeUnits));

      await client.close();
      await server.close();
      expect(receive.string, equals('respond'));
    });
    test('request with timeout', () async {
      var server = Client();
      await server.connect(Uri.parse('ws://localhost:8080'));
      var service = server.sub('service');
      unawaited(service.stream.first.then((m) {
        sleep(Duration(seconds: 1));
        m.respond(Uint8List.fromList('respond'.codeUnits));
      }));

      var client = Client();
      await client.connect(Uri.parse('ws://localhost:8080'));
      var receive = await client.request(
          'service', Uint8List.fromList('request'.codeUnits),
          timeout: Duration(seconds: 3));

      await client.close();
      await server.close();
      expect(receive.string, equals('respond'));
    });
    test('request with timeout exception', () async {
      var server = Client();
      await server.connect(Uri.parse('ws://localhost:8080'));
      var service = server.sub('service');
      unawaited(service.stream.first.then((m) {
        sleep(Duration(seconds: 5));
        m.respond(Uint8List.fromList('respond'.codeUnits));
      }));

      var client = Client();
      var gotit = false;
      await client.connect(Uri.parse('ws://localhost:8080'));
      try {
        await client.request('service', Uint8List.fromList('request'.codeUnits),
            timeout: Duration(seconds: 2));
      } on TimeoutException {
        gotit = true;
      }
      await client.close();
      await service.close();
      await server.close();
      expect(gotit, equals(true));
    });
    test('future request to 2 service', () async {
      var server = Client();
      await server.connect(Uri.parse('ws://localhost:8080'));
      var service1 = server.sub('service1');
      service1.stream.listen((m) {
        m.respond(Uint8List.fromList('respond1'.codeUnits));
      });
      var service2 = server.sub('service2');
      service2.stream.listen((m) {
        m.respond(Uint8List.fromList('respond2'.codeUnits));
      });

      var client = Client();
      await client.connect(Uri.parse('ws://localhost:8080'));
      Future<Message> receive1;
      Future<Message> receive2;
      unawaited(receive2 =
          client.request('service2', Uint8List.fromList('request'.codeUnits)));
      unawaited(receive1 =
          client.request('service1', Uint8List.fromList('request'.codeUnits)));
      var r1 = await receive1;
      var r2 = await receive2;
      await client.close();
      await server.close();
      expect(r1.string, equals('respond1'));
      expect(r2.string, equals('respond2'));
    });
    test('an unanswered request does not delay another', () async {
      var server = Client();
      await server.connect(Uri.parse('ws://localhost:8080'));
      var service = server.sub('answered');
      service.stream.listen((m) {
        m.respondString('respond');
      });

      var client = Client();
      await client.connect(Uri.parse('ws://localhost:8080'));
      // Nobody subscribes to this subject, so the request runs to its timeout.
      var unanswered = client.requestString('nobody.listens', 'request',
          timeout: Duration(seconds: 3));
      unawaited(unanswered.then<void>((_) {}, onError: (_) {}));

      var watch = Stopwatch()..start();
      var receive = await client.requestString('answered', 'request',
          timeout: Duration(seconds: 3));
      watch.stop();

      expect(receive.string, equals('respond'));
      expect(watch.elapsed, lessThan(Duration(seconds: 1)));
      await expectLater(unanswered, throwsA(isA<TimeoutException>()));
      await client.close();
      await server.close();
    });
    test('overlapping requests each get their own reply', () async {
      var server = Client();
      await server.connect(Uri.parse('ws://localhost:8080'));
      var service = server.sub('echo');
      service.stream.listen((m) {
        m.respondString('echo ' + m.string);
      });

      var client = Client();
      await client.connect(Uri.parse('ws://localhost:8080'));
      var replies = await Future.wait([
        for (var i = 0; i < 50; i++) client.requestString('echo', '$i'),
      ]);

      await client.close();
      await server.close();
      for (var i = 0; i < 50; i++) {
        expect(replies[i].string, equals('echo $i'));
      }
    });
    test('closing the client fails a request in flight', () async {
      var client = Client();
      await client.connect(Uri.parse('ws://localhost:8080'));
      var watch = Stopwatch()..start();
      var pending = client.requestString('nobody.listens', 'request',
          timeout: Duration(seconds: 10));
      var outcome = expectLater(pending, throwsA(isA<NatsException>()));
      await Future<void>.delayed(Duration(milliseconds: 200));
      await client.close();
      await outcome;
      watch.stop();
      expect(watch.elapsed, lessThan(Duration(seconds: 2)));
    });
    test('a request can follow a close and reconnect', () async {
      var server = Client();
      await server.connect(Uri.parse('ws://localhost:8080'));
      var service = server.sub('answered');
      service.stream.listen((m) {
        m.respondString('respond');
      });

      var client = Client();
      await client.connect(Uri.parse('ws://localhost:8080'));
      expect((await client.requestString('answered', 'a')).string,
          equals('respond'));
      await client.close();
      await client.connect(Uri.parse('ws://localhost:8080'));
      expect((await client.requestString('answered', 'b')).string,
          equals('respond'));

      await client.close();
      await server.close();
    });
    test('noResponders fails a request nobody serves at once', () async {
      var client = Client();
      await client.connect(Uri.parse('ws://localhost:8080'),
          connectOption: ConnectOption(noResponders: true));
      var watch = Stopwatch()..start();
      await expectLater(
        client.requestString('nobody.listens', 'request',
            timeout: Duration(seconds: 10)),
        throwsA(isA<NatsNoRespondersException>()
            .having((e) => e.subject, 'subject', 'nobody.listens')),
      );
      watch.stop();
      expect(watch.elapsed, lessThan(Duration(seconds: 1)));
      await client.close();
    });
    test('noResponders leaves an answered request alone', () async {
      var server = Client();
      await server.connect(Uri.parse('ws://localhost:8080'));
      var service = server.sub('answered');
      service.stream.listen((m) {
        m.respondString('respond');
      });

      var client = Client();
      await client.connect(Uri.parse('ws://localhost:8080'),
          connectOption: ConnectOption(noResponders: true));
      var receive = await client.requestString('answered', 'request');

      await client.close();
      await server.close();
      expect(receive.string, equals('respond'));
    });
    test('without noResponders a request nobody serves times out', () async {
      var client = Client();
      await client.connect(Uri.parse('ws://localhost:8080'));
      await expectLater(
        client.requestString('nobody.listens', 'request',
            timeout: Duration(seconds: 1)),
        throwsA(isA<TimeoutException>()),
      );
      await client.close();
    });
    test('noResponders without headers is refused before connecting', () async {
      var client = Client();
      await expectLater(
        client.connect(Uri.parse('ws://localhost:8080'),
            connectOption: ConnectOption(noResponders: true, headers: false)),
        throwsA(isA<NatsException>()),
      );
      expect(client.status, Status.disconnected);
    });
    test('a status header gives its code and description', () {
      var noMessages = Header.fromBytes(
          Uint8List.fromList('NATS/1.0 404 No Messages\r\n\r\n'.codeUnits));
      expect(noMessages.status, equals(404));
      expect(noMessages.description, equals('No Messages'));

      var noResponders = Header.fromBytes(
          Uint8List.fromList('NATS/1.0 503\r\n\r\n'.codeUnits));
      expect(noResponders.status, equals(503));
      expect(noResponders.description, isNull);

      expect(Header().status, isNull);
      expect(Header().description, isNull);
    });
  });
}
