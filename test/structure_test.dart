import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

import 'model/student.dart';

void main() {
  group('all', () {
    test('string to string', () async {
      var client = Client();

      await client.connect(Uri.parse('nats://localhost:4222'),
          retryInterval: 1);
      var sub = client.sub<String>('subject1', jsonDecoder: string2string);
      client.pub('subject1', Uint8List.fromList('message1'.codeUnits));
      var msg = await sub.stream.first;
      await client.close();
      expect(msg.data, equals('message1'));
    });

    test('sub', () async {
      var client = Client();
      await client.connect(Uri.parse('nats://localhost:4222'),
          retryInterval: 1);
      var sub = client.sub<Student>('subject1', jsonDecoder: json2Student);
      var student = Student('id', 'name', 1);
      client.pubString('subject1', jsonEncode(student.toJson()));
      var msg = await sub.stream.first;
      await client.close();
      expect(msg.data.id, student.id);
      expect(msg.data.name, student.name);
      expect(msg.data.score, student.score);
    });
    test('sub register jsonDecoder', () async {
      var client = Client();
      await client.connect(Uri.parse('nats://localhost:4222'),
          retryInterval: 1);
      client.registerJsonDecoder<Student>(json2Student);
      var sub = client.sub<Student>('subject1');
      var student = Student('id', 'name', 1);
      client.pubString('subject1', jsonEncode(student.toJson()));
      var msg = await sub.stream.first;
      await client.close();
      expect(msg.data.id, student.id);
      expect(msg.data.name, student.name);
      expect(msg.data.score, student.score);
    });
    test('sub no type', () async {
      var client = Client();
      await client.connect(Uri.parse('nats://localhost:4222'),
          retryInterval: 1);
      var sub = client.sub('subject1', jsonDecoder: json2Student);
      var student = Student('id', 'name', 1);
      client.pubString('subject1', jsonEncode(student.toJson()));
      var msg = await sub.stream.first;
      await client.close();
      expect(msg.data.id, student.id);
      expect(msg.data.name, student.name);
      expect(msg.data.score, student.score);
    });
    test('sub no type no jsonDecoder', () async {
      var client = Client();
      await client.connect(Uri.parse('nats://localhost:4222'),
          retryInterval: 1);
      var sub = client.sub('subject1');
      var student = Student('id', 'name', 1);
      client.pubString('subject1', jsonEncode(student.toJson()));
      var msg = await sub.stream.first;
      await client.close();
      if (!(msg.data is Uint8List)) {
        throw Exception('missing data type');
      }
    });
    test('request', () async {
      var server = Client();
      await server.connect(Uri.parse('nats://localhost:4222'));
      server.registerJsonDecoder<Student>(json2Student);
      var service = server.sub<Student>('structure_service');
      unawaited(service.stream.first.then((m) {
        m.respondString(jsonEncode(m.data.toJson()));
      }));

      var client = Client();
      var s1 = Student('id', 'name', 1);
      await client.connect(Uri.parse('ws://localhost:8080'));
      var receive = await client.requestString(
          'structure_service', jsonEncode(s1.toJson()));
      var s2 = Student.fromJson(jsonDecode(receive.string));
      await client.close();
      await server.close();
      expect(s1.score, equals(s2.score));
    });
    test('request register jsonDecoder', () async {
      var server = Client();
      server.registerJsonDecoder<Student>(json2Student);
      await server.connect(Uri.parse('nats://localhost:4222'));
      var service = server.sub<Student>('structure_service');
      unawaited(service.stream.first.then((m) {
        m.respondString(jsonEncode(m.data.toJson()));
      }));

      var client = Client();
      client.registerJsonDecoder<Student>(json2Student);
      var s1 = Student('id', 'name', 1);
      await client.connect(Uri.parse('ws://localhost:8080'));
      var receive = await client.requestString<Student>(
          'structure_service', jsonEncode(s1.toJson()));
      var s2 = receive.data;
      await client.close();
      await server.close();
      expect(s1.score, equals(s2.score));
    });
  });
}

String string2string(String input) {
  return input;
}

Student json2Student(String json) {
  var map = jsonDecode(json);
  return Student.fromJson(map);
}

String student2json(Student student) {
  return jsonEncode(student.toJson());
}
