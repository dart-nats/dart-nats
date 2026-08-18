import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

/// A socket whose writes fail the way an aborted connection makes them fail: `add` does not throw,
/// the failure arrives on `done`.
class _AbortOnWriteSocket extends Stream<Uint8List> implements Socket {
  final _incoming = StreamController<Uint8List>();
  final _done = Completer<dynamic>();

  @override
  StreamSubscription<Uint8List> listen(
    void Function(Uint8List event)? onData, {
    Function? onError,
    void Function()? onDone,
    bool? cancelOnError,
  }) {
    // The server speaks first. Without an INFO the client never sends anything, so the write that is
    // the point of this test would never be attempted.
    Timer(const Duration(milliseconds: 20), () {
      if (_incoming.isClosed) return;
      _incoming.add(Uint8List.fromList(utf8.encode(
          'INFO {"server_id":"test","version":"2.10.0","proto":1,'
          '"host":"127.0.0.1","port":4222,"max_payload":1048576}\r\n')));
    });
    return _incoming.stream.listen(onData,
        onError: onError, onDone: onDone, cancelOnError: cancelOnError);
  }

  @override
  void add(List<int> data) {
    if (!_done.isCompleted) {
      _done.completeError(
        const SocketException('Software caused connection abort'),
      );
    }
  }

  @override
  Future<dynamic> get done => _done.future;

  @override
  Future<void> close() async {}

  @override
  void destroy() {}

  @override
  dynamic noSuchMethod(Invocation invocation) =>
      super.noSuchMethod(invocation);
}

void main() {
  test('a write onto an aborted socket does not escape as an unhandled error',
      () async {
    Object? escaped;

    await runZonedGuarded(() async {
      final client = Client();
      client.debugSocketConnect =
          (host, port, {timeout}) async => _AbortOnWriteSocket();
      // Not awaited: the handshake cannot finish on a socket that refuses every write, so connect()
      // never returns. Its own failure is caught here so that only the WRITE failure can reach the
      // zone, which is what this test is about.
      unawaited(client
          .connect(Uri.parse('nats://127.0.0.1:4222'), retry: false, timeout: 1)
          .catchError((dynamic _) {}));
      await Future<void>.delayed(const Duration(milliseconds: 500));
    }, (error, stack) {
      escaped ??= error;
    });

    await Future<void>.delayed(const Duration(milliseconds: 200));
    expect(escaped, isNull,
        reason: 'the failed write reached the zone with no handler: $escaped');
  });
}
