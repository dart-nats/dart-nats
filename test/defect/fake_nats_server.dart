import 'dart:async';
import 'dart:convert';
import 'dart:io';

/// Just enough of a NATS server to drive the client's connect and reconnect
/// paths without a real nats-server: it sends INFO, optionally upgrades to
/// TLS the way nats-server does (INFO first, then the handshake), and answers
/// PING with PONG.
class FakeNatsServer {
  FakeNatsServer({
    this.tls = false,
    this.sendInfo = true,
    this.errOnAccept,
    this.answerPing = true,
    this.onLine,
  });

  /// Advertise `tls_required` in INFO and upgrade every connection.
  final bool tls;

  /// When false, accept the TCP connection and then say nothing.
  final bool sendInfo;

  /// When set, answer every connection with `-ERR '<errOnAccept>'` after
  /// INFO and close it, like nats-server rejecting a client.
  final String? errOnAccept;

  /// When false, only the handshake PING is answered; every later one is
  /// ignored, like a connection that has silently died. Settable mid-test.
  bool answerPing;

  /// Called with each protocol line a client sends, and a function that
  /// writes a raw reply to that client -- enough to script an `-ERR`.
  final void Function(String line, void Function(String raw) reply)? onLine;

  // Self-signed, test-only (CN=localhost, SAN 127.0.0.1), valid for 100 years.
  // Committed on purpose, unlike the generated certs in test/config.
  static final _context = SecurityContext()
    ..useCertificateChain('test/certs/server-cert.pem')
    ..usePrivateKey('test/certs/server-key.pem');

  ServerSocket? _server;
  final _sockets = <Socket>[];

  /// TCP connections accepted since start (or the last [resetCounters]).
  int tcpAccepted = 0;

  /// TLS handshakes that completed since start (or the last [resetCounters]).
  int tlsHandshakes = 0;

  /// When each TCP connection was accepted.
  final acceptTimes = <DateTime>[];

  int get port => _server!.port;

  Future<void> start([int port = 0]) async {
    _server = await ServerSocket.bind(InternetAddress.loopbackIPv4, port,
        shared: true);
    _server!.listen(_accept);
  }

  void resetCounters() {
    tcpAccepted = 0;
    tlsHandshakes = 0;
    acceptTimes.clear();
  }

  /// Drop every connection and stop listening, like a nats-server shutdown.
  Future<void> stop() async {
    for (final s in _sockets) {
      s.destroy();
    }
    _sockets.clear();
    await _server?.close();
    _server = null;
  }

  /// Stop, then listen again on the same port.
  Future<void> restart() async {
    final p = port;
    await stop();
    await start(p);
  }

  Future<void> _accept(Socket socket) async {
    tcpAccepted++;
    acceptTimes.add(DateTime.now());
    if (!sendInfo) {
      _sockets.add(socket);
      socket.listen((_) {}, onError: (dynamic _) {}, onDone: () {});
      return;
    }
    socket.add(utf8.encode('INFO {"server_id":"fake","version":"2.10.0",'
        '"proto":1,"max_payload":1048576,"tls_required":$tls}\r\n'));
    if (errOnAccept != null) {
      socket.add(utf8.encode("-ERR '$errOnAccept'\r\n"));
      await socket.flush();
      socket.destroy();
      return;
    }
    Socket conn = socket;
    if (tls) {
      try {
        conn = await SecureSocket.secureServer(socket, _context)
            .timeout(const Duration(seconds: 2));
      } catch (_) {
        socket.destroy();
        return;
      }
      tlsHandshakes++;
    }
    _sockets.add(conn);
    var pings = 0;
    conn.listen((data) {
      for (final line in utf8.decode(data).split('\r\n')) {
        if (line.toUpperCase().startsWith('PING')) {
          pings++;
          if (answerPing || pings == 1) {
            conn.add(utf8.encode('PONG\r\n'));
          }
        }
        onLine?.call(line, (raw) => conn.add(utf8.encode(raw)));
      }
    }, onError: (dynamic _) {}, onDone: () {});
  }
}

/// A plain TCP pipe in front of a server, so a test can write raw bytes
/// straight at the client, past any TLS layer.
class TcpProxy {
  TcpProxy(this.targetPort);

  final int targetPort;
  late ServerSocket _server;
  final _clientSides = <Socket>[];
  final _upstreams = <Socket>[];

  int get port => _server.port;

  Future<void> start() async {
    _server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    _server.listen((client) async {
      final upstream =
          await Socket.connect(InternetAddress.loopbackIPv4, targetPort);
      _clientSides.add(client);
      _upstreams.add(upstream);
      client.listen(upstream.add,
          onError: (dynamic _) {}, onDone: upstream.destroy);
      upstream.listen(client.add,
          onError: (dynamic _) {}, onDone: client.destroy);
    });
  }

  /// Write [bytes] to every client connection as if the server sent them.
  void inject(List<int> bytes) {
    for (final c in _clientSides) {
      c.add(bytes);
    }
  }

  Future<void> stop() async {
    for (final s in [..._clientSides, ..._upstreams]) {
      s.destroy();
    }
    await _server.close();
  }
}

/// Runs [body] in a guarded zone and fails the test if [body] fails or if
/// any error escapes to the zone unhandled.
Future<void> expectNoEscape(Future<void> Function() body) async {
  Object? escaped;
  Object? failure;
  StackTrace? failureStack;
  await runZonedGuarded(() async {
    try {
      await body();
    } catch (e, st) {
      // Errors stay in this zone; carry them out to fail the test.
      failure = e;
      failureStack = st;
    }
  }, (error, stack) {
    escaped ??= error;
  });
  if (failure != null) {
    return Future<void>.error(failure!, failureStack);
  }
  if (escaped != null) {
    throw StateError('escaped to the zone: $escaped');
  }
}
