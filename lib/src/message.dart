// message model sending from NATS server
import 'dart:convert';
import 'dart:typed_data';

import 'client.dart';
import 'common.dart';

/// Message Header
class Header {
  /// header version
  String version;

  /// headers key value
  Map<String, String>? headers;

  /// constructor
  Header({this.headers, this.version = 'NATS/1.0'}) {
    this.headers ??= {};
  }

  /// add key, value
  Header add(String key, String value) {
    headers![key] = value;
    return this;
  }

  /// get value from key
  /// return null if notfound
  String? get(String key) {
    return headers![key];
  }

  /// construct from bytes
  static Header fromBytes(Uint8List b) {
    var str = utf8.decode(b);
    Map<String, String> m = {};
    var strList = str.split('\r\n');
    var version = strList[0];
    strList.removeAt(0);
    for (var h in strList) {
      /// values of headers can contain ':' so find the first index for the
      /// correct split index
      var splitIndex = h.indexOf(':');

      /// if the index is <= to 0 it means there was either no ':' or its the
      /// first character. In either case its not a valid header to split.
      if (splitIndex <= 0) {
        continue;
      }
      var key = h.substring(0, splitIndex);
      var value = h.substring(splitIndex + 1);
      m[key] = value;
    }

    return Header(headers: m, version: version);
  }

  /// convert to bytes
  Uint8List toBytes() {
    var str = '${this.version}\r\n';

    headers?.forEach((k, v) {
      str = str + '$k:$v\r\n';
    });
    str += '\r\n';

    return Uint8List.fromList(utf8.encode(str));
  }

  /// Get status code if this is a NATS status message
  int? get status {
    final statusStr = get('Status');
    if (statusStr != null) {
      return int.tryParse(statusStr);
    }
    final parts = version.split(' ');
    if (parts.length >= 2) {
      return int.tryParse(parts[1]);
    }
    return null;
  }
}

/// Message class
class Message<T> {
  ///subscriber id auto generate by client
  final int sid;

  /// subject  and replyto
  final String? subject, replyTo;
  final Client _client;

  /// message header
  final Header? header;

  ///payload of data in byte
  final Uint8List byte;

  ///convert from json string to T for structure data
  T Function(String)? jsonDecoder;

  ///payload of data in byte
  T get data {
    // if (jsonDecoder == null) throw Exception('no converter. can not convert. use msg.byte instead');
    if (jsonDecoder == null) {
      return byte as T;
    }
    return jsonDecoder!(string);
  }

  ///constructor
  Message(this.subject, this.sid, this.byte, this._client,
      {this.replyTo, this.jsonDecoder, this.header});

  ///payload in string
  String get string => utf8.decode(byte);

  ///Respond to message
  bool respond(Uint8List data) {
    if (replyTo == null || replyTo == '') return false;
    _client.pub(replyTo, data);
    return true;
  }

  ///Respond to string message
  bool respondString(String str) {
    return respond(Uint8List.fromList(utf8.encode(str)));
  }

  /// Acknowledge the message (JetStream)
  ///
  /// Fire-and-forget: publishes the ack but does not wait for (or report)
  /// whether it actually reached the server, so a disconnected client still
  /// returns `true`. Prefer [ackSync] when the caller needs to know the ack
  /// really landed.
  bool ack() {
    if (replyTo == null || replyTo == '') return false;
    _client.pub(replyTo, Uint8List.fromList(utf8.encode('+ACK')));
    return true;
  }

  /// Negatively acknowledge the message (JetStream)
  ///
  /// Fire-and-forget: see [ack]'s doc comment. Prefer [nakSync].
  bool nak() {
    if (replyTo == null || replyTo == '') return false;
    _client.pub(replyTo, Uint8List.fromList(utf8.encode('-NAK')));
    return true;
  }

  /// Terminate the message, preventing redelivery (JetStream)
  ///
  /// Fire-and-forget: see [ack]'s doc comment. Prefer [termSync].
  bool term() {
    if (replyTo == null || replyTo == '') return false;
    _client.pub(replyTo, Uint8List.fromList(utf8.encode('+TERM')));
    return true;
  }

  /// Indicate message processing is still in progress (JetStream)
  ///
  /// Fire-and-forget: see [ack]'s doc comment. Prefer [inProgressSync].
  bool inProgress() {
    if (replyTo == null || replyTo == '') return false;
    _client.pub(replyTo, Uint8List.fromList(utf8.encode('+WPI')));
    return true;
  }

  /// Get the JetStream sequence number directly from the reply subject metadata
  int? get streamSequence {
    if (replyTo == null) return null;
    final parts = replyTo!.split('.');
    if (parts.length < 8) return null;
    if (parts[0] != '\$JS' || parts[1] != 'ACK') return null;

    // $JS.ACK.<stream>.<consumer>.<delivered_count>.<stream_seq>...
    if (parts.length == 8) {
      return int.tryParse(parts[5]);
    }
    if (parts.length == 9) {
      return int.tryParse(parts[5]);
    }
    if (parts.length == 10) {
      return int.tryParse(parts[7]);
    }
    if (parts.length == 11) {
      return int.tryParse(parts[7]);
    }
    return null;
  }

  /// Get the JetStream consumer sequence number directly from the reply subject metadata
  int? get consumerSequence {
    if (replyTo == null) return null;
    final parts = replyTo!.split('.');
    if (parts.length < 8) return null;
    if (parts[0] != '\$JS' || parts[1] != 'ACK') return null;

    if (parts.length == 8 || parts.length == 9) {
      return int.tryParse(parts[6]);
    }
    if (parts.length == 10 || parts.length == 11) {
      return int.tryParse(parts[8]);
    }
    return null;
  }

  /// Acknowledge the message and wait for confirmation from the JetStream server (synchronous ack)
  Future<void> ackSync({Duration timeout = const Duration(seconds: 2)}) async {
    if (replyTo == null || replyTo == '') {
      throw NatsException('Cannot acknowledge message: no reply subject');
    }
    await _client.request(replyTo!, Uint8List.fromList(utf8.encode('+ACK')),
        timeout: timeout);
  }

  /// Negatively acknowledge the message and wait for confirmation from the
  /// JetStream server (synchronous nak) -- throws if it doesn't land, e.g.
  /// because the client is disconnected.
  Future<void> nakSync({Duration timeout = const Duration(seconds: 2)}) async {
    if (replyTo == null || replyTo == '') {
      throw NatsException('Cannot nak message: no reply subject');
    }
    await _client.request(replyTo!, Uint8List.fromList(utf8.encode('-NAK')),
        timeout: timeout);
  }

  /// Terminate the message, preventing redelivery, and wait for
  /// confirmation from the JetStream server (synchronous term) -- throws if
  /// it doesn't land, e.g. because the client is disconnected.
  Future<void> termSync({Duration timeout = const Duration(seconds: 2)}) async {
    if (replyTo == null || replyTo == '') {
      throw NatsException('Cannot terminate message: no reply subject');
    }
    await _client.request(replyTo!, Uint8List.fromList(utf8.encode('+TERM')),
        timeout: timeout);
  }

  /// Indicate message processing is still in progress and wait for
  /// confirmation from the JetStream server (synchronous in-progress) --
  /// throws if it doesn't land, e.g. because the client is disconnected.
  Future<void> inProgressSync(
      {Duration timeout = const Duration(seconds: 2)}) async {
    if (replyTo == null || replyTo == '') {
      throw NatsException('Cannot mark message in progress: no reply subject');
    }
    await _client.request(replyTo!, Uint8List.fromList(utf8.encode('+WPI')),
        timeout: timeout);
  }
}
