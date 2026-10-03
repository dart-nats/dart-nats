import 'dart:async';
import 'dart:convert';
import 'dart:io';

/// Just enough of a NATS server to drive the client's connect and reconnect
/// paths without a real nats-server: it sends INFO, optionally upgrades to
/// TLS the way nats-server does (INFO first, then the handshake), and answers
/// PING with PONG.
class FakeNatsServer {
  FakeNatsServer({this.tls = false});

  /// Advertise `tls_required` in INFO and upgrade every connection.
  final bool tls;

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

  int get port => _server!.port;

  Future<void> start([int port = 0]) async {
    _server = await ServerSocket.bind(InternetAddress.loopbackIPv4, port,
        shared: true);
    _server!.listen(_accept);
  }

  void resetCounters() {
    tcpAccepted = 0;
    tlsHandshakes = 0;
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
    socket.add(utf8.encode('INFO {"server_id":"fake","version":"2.10.0",'
        '"proto":1,"max_payload":1048576,"tls_required":$tls}\r\n'));
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
    conn.listen((data) {
      for (final line in utf8.decode(data).split('\r\n')) {
        if (line.toUpperCase().startsWith('PING')) {
          conn.add(utf8.encode('PONG\r\n'));
        }
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
