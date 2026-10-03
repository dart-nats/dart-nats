import 'dart:async';
import 'dart:convert';
import 'platform/platform.dart';
import 'dart:typed_data';

import 'package:meta/meta.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

import 'common.dart';
import 'inbox.dart';
import 'message.dart';
import 'nkeys.dart';
import 'subscription.dart';
import 'jetstream.dart';

enum _ReceiveState {
  idle,
  msg,
}

enum _ClientStatus {
  init,
  used,
  closed,
}

/// NATS connection status
enum Status {
  /// Disconnected state
  disconnected,

  /// Performing TLS handshake
  tlsHandshake,

  /// Connected, awaiting initial info handshake
  infoHandshake,

  /// Fully connected and ready to process messages
  connected,

  /// Connection has been closed
  closed,

  /// Reconnecting to the server
  reconnecting,

  /// Connecting by connect() method
  connecting,
}

class _Pub {
  final String? subject;
  final List<int> data;
  final String? replyTo;

  _Pub(this.subject, this.data, this.replyTo);
}

/// A request waiting for its reply: the request's own subject, kept so a
/// failure can name it, and the completer its reply inbox resolves.
class _PendingRequest {
  final String subject;
  final completer = Completer<Message<dynamic>>();

  _PendingRequest(this.subject);
}

/// A request that gave up waiting: what it asked, and when it gave up.
class _AbandonedRequest {
  final String subject;
  final DateTime at;

  _AbandonedRequest(this.subject, this.at);
}

/// NATS client implementation
class Client {
  final _ackStream = StreamController<bool>.broadcast();
  _ClientStatus _clientStatus = _ClientStatus.init;
  WebSocketChannel? _wsChannel;
  Socket? _tcpSocket;
  SecureSocket? _secureSocket;
  bool _tlsRequired = false;
  bool _retry = false;

  Info _info = Info();
  final _pingCompleters = <Completer<void>>[];
  late Completer<void> _connectCompleter;

  List<Uri> _serverPool = [];
  int _currentServerIndex = 0;

  /// Callback when the client connects to the server
  void Function()? onConnect;

  /// Callback when the client disconnects from the server
  void Function()? onDisconnect;

  /// Callback when an error occurs
  void Function(dynamic error)? onError;

  /// Callback when the client reconnects to the server
  void Function()? onReconnect;

  /// Callback when the client is closed
  void Function()? onClose;

  /// Callback when the reply to a request arrives after that request timed
  /// out: the request's subject, and how long after the timeout the reply
  /// came. It tells "the reply was late" from "the reply never came", which
  /// a [TimeoutException] alone cannot. The late reply itself is dropped.
  ///
  /// Only the most recent timed-out requests are remembered, and none at
  /// all while this is null.
  void Function(String subject, Duration lateBy)? onLateReply;

  /// User authentication callbacks
  String Function()? userJwtHandler;

  /// Supplies the auth token for each connection attempt -- the first
  /// connect and every reconnect -- replacing [ConnectOption.authToken].
  ///
  /// For tokens that expire: a reconnect that sent the token captured at
  /// [connect] would present a stale one. May be asynchronous, since a fresh
  /// token usually comes from a network call. If it throws, the attempt
  /// fails like any other: reported through [onError] and retried per the
  /// `retry` settings.
  FutureOr<String> Function()? authTokenHandler;

  /// Callback to sign a challenge nonce during NKEY authentication
  Uint8List Function(Uint8List nonce)? signatureHandler;

  /// Error handler for websocket errors
  Function(dynamic) wsErrorHandler = (e) {};

  var _status = Status.disconnected;

  /// Check if client is fully connected
  bool get connected => _status == Status.connected;

  final _statusController = StreamController<Status>.broadcast();
  var _channelStream = StreamController<dynamic>();

  /// Get current connection status of client
  Status get status => _status;

  /// Returns true if all underlying connection sockets (TCP, Secure, WS) are fully closed and null.
  /// This is useful for verifying there are no socket leaks.
  bool get isClosedAndCleaned =>
      _tcpSocket == null && _secureSocket == null && _wsChannel == null;

  /// Accept self-signed or invalid certificate. Not recommended for production.
  bool acceptBadCert = false;

  /// Stream of status updates
  Stream<Status> get statusStream => _statusController.stream;

  var _connectOption = ConnectOption();

  /// Security context used for secure socket setup
  SecurityContext? securityContext;

  Nkeys? _nkeys;

  /// Get NKEY seed
  String? get seed => _nkeys?.seed;

  /// Set NKEY seed
  set seed(String? newseed) {
    if (newseed == null) {
      _nkeys = null;
      return;
    }
    _nkeys = Nkeys.fromSeed(newseed);
  }

  final _jsonDecoder = <Type, dynamic Function(String)>{};

  /// Register a JSON decoder for a generic type `<T>`
  void registerJsonDecoder<T>(T Function(String) f) {
    if (T == dynamic) {
      throw NatsException('can not register dynamic type');
    }
    _jsonDecoder[T] = f;
  }

  /// Get latest server info
  Info? get info => _info;

  final _subs = <int, Subscription>{};
  final _backendSubs = <int, bool>{};
  final _pubBuffer = <_Pub>[];

  /// Heartbeat ping interval duration
  Duration pingInterval = const Duration(seconds: 120);

  /// Maximum outstanding heartbeat pings allowed before reconnecting
  int maxPingsOut = 2;

  /// How long a heartbeat PING may go unanswered before it counts against
  /// [maxPingsOut] right away. Null (the default) leaves the check to the
  /// next heartbeat tick, so a dead connection is noticed up to
  /// `(maxPingsOut + 1) * pingInterval` after it died; with a timeout that
  /// becomes `maxPingsOut * pingInterval + pingTimeout`.
  Duration? pingTimeout;
  Timer? _pingTimer;
  Timer? _pingDeadline;
  int _pingsOut = 0;

  /// Maximum number of messages to buffer during reconnection
  int maxReconnectBuffer = 1000;
  int _reconnectTimeout = 5;
  int _reconnectInterval = 10;
  int _reconnectCount = 3;
  bool _wasConnected = false;
  bool _reconnecting = false;

  /// True while completing a background reconnect cycle (for [onReconnect]).
  bool _reconnectCycle = false;

  /// Override for [Socket.connect] used by tests to simulate connect failures.
  /// Return a platform [Socket] (or throw) — typed as [Object] so VM/web stubs unify.
  @visibleForTesting
  Future<Object> Function(String host, int port, {Duration? timeout})?
      debugSocketConnect;

  static const Duration _socketCleanupTimeout = Duration(seconds: 2);

  int _ssid = 0;
  int _connectionId = 0;
  bool _connectOptionSent = false;

  /// The connect attempt in flight. It completes once the attempt reaches
  /// [Status.connected], or with the error that ended it: an open socket is
  /// not a connection yet, since TLS, CONNECT and the first PONG can all still
  /// fail. Only [_connectUri] creates it, and it is always awaited there.
  Completer<void>? _handshake;

  bool get _handshakePending => _handshake?.isCompleted == false;

  void _failHandshake(Object error) {
    final handshake = _handshake;
    if (handshake == null || handshake.isCompleted) return;
    handshake.completeError(error);
    // A PING sent during the failed attempt must not be matched with the
    // PONG of the next one.
    while (_pingCompleters.isNotEmpty) {
      final completer = _pingCompleters.removeAt(0);
      if (!completer.isCompleted) {
        completer.completeError(error);
      }
    }
  }

  Uint8List _buffer = Uint8List(0);
  int _bufferLength = 0;
  int _readOffset = 0;
  _ReceiveState _receiveState = _ReceiveState.idle;
  String _receiveLine1 = '';

  void _appendBytes(List<int> d) {
    if (_bufferLength + d.length > _buffer.length) {
      var newCapacity = _buffer.length * 2;
      if (newCapacity < _bufferLength + d.length) {
        newCapacity = _bufferLength + d.length + 16384;
      }
      final newBuffer = Uint8List(newCapacity);
      newBuffer.setRange(0, _bufferLength, _buffer);
      _buffer = newBuffer;
    }
    _buffer.setRange(_bufferLength, _bufferLength + d.length, d);
    _bufferLength += d.length;
  }

  int _indexOf(int byte, int start) {
    for (var i = start; i < _bufferLength; i++) {
      if (_buffer[i] == byte) {
        return i;
      }
    }
    return -1;
  }

  int _findLineEnd(int start) {
    var searchStart = start;
    while (true) {
      final idx = _indexOf(13, searchStart);
      if (idx == -1) return -1;
      if (idx + 1 < _bufferLength && _buffer[idx + 1] == 10) {
        return idx;
      }
      searchStart = idx + 1;
    }
  }

  /// Create a new NATS Client
  Client() {
    _streamHandle();
  }

  Future<void> _sign() async {
    if (userJwtHandler != null) {
      _connectOption.jwt = userJwtHandler!();
    }
    if (authTokenHandler != null) {
      _connectOption.authToken = await authTokenHandler!();
    }
    if (_info.nonce != null) {
      if (signatureHandler != null) {
        final sig =
            signatureHandler!(Uint8List.fromList(utf8.encode(_info.nonce!)));
        _connectOption.sig = base64.encode(sig);
      } else if (_nkeys != null) {
        final sig = _nkeys?.sign(utf8.encode(_info.nonce!));
        _connectOption.sig = base64.encode(sig!);
      }
    }
  }

  List<String> _parseArgs(String line) {
    final args = <String>[];
    var start = 0;
    final len = line.length;
    while (start < len) {
      while (start < len && line.codeUnitAt(start) == 32) {
        start++;
      }
      if (start >= len) break;
      var end = start;
      while (end < len && line.codeUnitAt(end) != 32) {
        end++;
      }
      args.add(line.substring(start, end));
      start = end;
    }
    return args;
  }

  void _streamHandle() {
    _channelStream.stream.listen((dynamic dataPacket) {
      if (dataPacket is! List || dataPacket.length != 2) return;
      final connId = dataPacket[0] as int;
      final d = dataPacket[1];
      if (connId != _connectionId) {
        return;
      }
      if (status == Status.disconnected || status == Status.closed) {
        return;
      }
      if (d is List<int>) {
        _appendBytes(d);
      } else if (d is String) {
        _appendBytes(utf8.encode(d));
      }

      while (_receiveState == _ReceiveState.idle) {
        final lineEnd = _findLineEnd(_readOffset);
        if (lineEnd == -1) break;

        final lineLen = lineEnd - _readOffset;
        var isMsg = false;
        var isHmsg = false;

        if (lineLen >= 4 &&
            (_buffer[_readOffset] == 109 || _buffer[_readOffset] == 77) &&
            (_buffer[_readOffset + 1] == 115 ||
                _buffer[_readOffset + 1] == 83) &&
            (_buffer[_readOffset + 2] == 103 ||
                _buffer[_readOffset + 2] == 71) &&
            _buffer[_readOffset + 3] == 32) {
          isMsg = true;
        } else if (lineLen >= 5 &&
            (_buffer[_readOffset] == 104 || _buffer[_readOffset] == 72) &&
            (_buffer[_readOffset + 1] == 109 ||
                _buffer[_readOffset + 1] == 77) &&
            (_buffer[_readOffset + 2] == 115 ||
                _buffer[_readOffset + 2] == 83) &&
            (_buffer[_readOffset + 3] == 103 ||
                _buffer[_readOffset + 3] == 71) &&
            _buffer[_readOffset + 4] == 32) {
          isHmsg = true;
        }

        if (isMsg) {
          final line = String.fromCharCodes(Uint8List.view(
              _buffer.buffer, _buffer.offsetInBytes + _readOffset, lineLen));
          final args = _parseArgs(line);
          if (args.length < 4) {
            _readOffset = lineEnd + 2;
            continue;
          }
          final subject = args[1];
          final sid = int.parse(args[2]);
          String? replyTo;
          int len;
          if (args.length == 4) {
            len = int.parse(args[3]);
          } else {
            replyTo = args[3];
            len = int.parse(args[4]);
          }

          final requiredBytes = lineEnd + len + 4;
          if (_bufferLength < requiredBytes) {
            break;
          }

          _readOffset = lineEnd + 2;
          final payload = _buffer.sublist(_readOffset, _readOffset + len);
          _readOffset += len + 2;

          if (_subs[sid] != null) {
            _subs[sid]?.add(Message<dynamic>(
              subject,
              sid,
              payload,
              this,
              replyTo: replyTo,
            ));
          }
          continue;
        }

        if (isHmsg) {
          final line = String.fromCharCodes(Uint8List.view(
              _buffer.buffer, _buffer.offsetInBytes + _readOffset, lineLen));
          final args = _parseArgs(line);
          if (args.length < 5) {
            _readOffset = lineEnd + 2;
            continue;
          }
          final subject = args[1];
          final sid = int.parse(args[2]);
          String? replyTo;
          int len;
          int headerLength;
          if (args.length == 5) {
            headerLength = int.parse(args[3]);
            len = int.parse(args[4]);
          } else {
            replyTo = args[3];
            headerLength = int.parse(args[4]);
            len = int.parse(args[5]);
          }

          final requiredBytes = lineEnd + len + 4;
          if (_bufferLength < requiredBytes) {
            break;
          }

          _readOffset = lineEnd + 2;
          final header =
              _buffer.sublist(_readOffset, _readOffset + headerLength);
          final payload =
              _buffer.sublist(_readOffset + headerLength, _readOffset + len);
          _readOffset += len + 2;

          if (_subs[sid] != null) {
            final msg = Message<dynamic>(
              subject,
              sid,
              payload,
              this,
              replyTo: replyTo,
              header: Header.fromBytes(header),
            );
            _subs[sid]?.add(msg);
          }
          continue;
        }

        final line = String.fromCharCodes(Uint8List.view(
            _buffer.buffer, _buffer.offsetInBytes + _readOffset, lineLen));
        _processOp(line, lineEnd);
      }

      if (_readOffset == _bufferLength) {
        _readOffset = 0;
        _bufferLength = 0;
      } else if (_readOffset > 65536) {
        final remaining = _bufferLength - _readOffset;
        if (remaining > 0) {
          _buffer.setRange(0, remaining, _buffer, _readOffset);
        }
        _bufferLength = remaining;
        _readOffset = 0;
      }
    });
  }

  Uri _getNextServer() {
    if (_serverPool.isEmpty) {
      throw NatsException('Server pool is empty');
    }
    final server = _serverPool[_currentServerIndex];
    _currentServerIndex = (_currentServerIndex + 1) % _serverPool.length;
    return server;
  }

  /// Connect to the NATS server using NATS URI schema
  Future<void> connect(
    Uri uri, {
    List<Uri>? servers,
    ConnectOption? connectOption,
    int timeout = 5,
    bool retry = true,
    int retryInterval = 10,
    int retryCount = 3,
    SecurityContext? securityContext,
    Duration pingInterval = const Duration(seconds: 120),
    int maxPingsOut = 2,
    Duration? pingTimeout,
    bool randomizeServers = true,
    int maxReconnectBuffer = 1000,
  }) async {
    // Reject before touching any state: a call refused here must not leave the
    // live connection with this call's server pool, retry settings or an
    // orphaned connect completer.
    if (_clientStatus == _ClientStatus.used) {
      throw Exception(
          NatsException('client in use. must close before call connect'));
    }
    if (status != Status.disconnected && status != Status.closed) {
      return Future.error('Error: status not disconnected and not closed');
    }
    final option = connectOption ?? _connectOption;
    if (option.noResponders == true && option.headers != true) {
      throw NatsException('noResponders needs headers: the server answers '
          'a request without responders with a status header');
    }

    _retry = retry;
    this.securityContext = securityContext;
    this.pingInterval = pingInterval;
    this.maxPingsOut = maxPingsOut;
    this.pingTimeout = pingTimeout;
    this.maxReconnectBuffer = maxReconnectBuffer;
    _reconnectTimeout = timeout;
    _reconnectInterval = retryInterval;
    _reconnectCount = retryCount;

    _connectCompleter = Completer<void>();
    _serverPool = [uri];
    if (servers != null) {
      _serverPool.addAll(servers);
    }
    if (randomizeServers) {
      _serverPool.shuffle();
    }
    _currentServerIndex = 0;

    _clientStatus = _ClientStatus.used;
    if (connectOption != null) {
      _connectOption = connectOption;
    }

    _connectLoop(
      timeout: timeout,
      retryInterval: retryInterval,
      retryCount: retryCount,
    );

    return _connectCompleter.future;
  }

  void _connectLoop({
    int timeout = 5,
    required int retryInterval,
    required int retryCount,
  }) async {
    int attempts = 0;
    Object? lastError;
    final maxAttempts = _retry
        ? (retryCount == -1 ? -1 : retryCount * _serverPool.length)
        : _serverPool.length;

    while (attempts < maxAttempts || maxAttempts == -1) {
      if (attempts == 0) {
        _setStatus(Status.connecting);
      } else {
        _setStatus(Status.reconnecting);
      }

      final currentUri = _getNextServer();
      try {
        if (_channelStream.isClosed) {
          _channelStream = StreamController<dynamic>();
        }
        _connectionId++;
        final success = await _connectUri(currentUri, timeout: timeout);
        if (!success) {
          attempts++;
          if (_retry || attempts < _serverPool.length) {
            final delay = _retry ? retryInterval : 1;
            await Future<void>.delayed(Duration(seconds: delay));
            continue;
          }
          break;
        }

        if (!_connectCompleter.isCompleted) {
          _connectCompleter.complete();
        }
        return;
      } catch (err, st) {
        await _cleanUpSocketsSoft();
        // Closed by the user, or by a fatal -ERR such as an auth violation.
        if (status == Status.closed) break;
        lastError = err;
        if (onError != null) {
          onError!(err);
        }
        attempts++;
        if (_retry || attempts < _serverPool.length) {
          final delay = _retry ? retryInterval : 1;
          await Future<void>.delayed(Duration(seconds: delay));
        } else {
          if (!_connectCompleter.isCompleted) {
            _connectCompleter.completeError(
              NatsException('can not connect to any servers in the pool: $err'),
              st,
            );
          }
          _setStatus(Status.disconnected);
          break;
        }
      }
    }

    if (!_connectCompleter.isCompleted) {
      _clientStatus = _ClientStatus.closed;
      _connectCompleter.completeError(NatsException(
          'can not connect to any servers in the pool' +
              (lastError == null ? '' : ': $lastError')));
    }
  }

  Future<bool> _connectUri(Uri uri, {int timeout = 5}) async {
    // Start every attempt with no references to the previous transport. A
    // stale _secureSocket left behind by a server-side close makes the new
    // TCP listener drop INFO, so the TLS upgrade never starts (#55).
    await _cleanUpSocketsSoft();
    _buffer = Uint8List(0);
    _bufferLength = 0;
    _readOffset = 0;
    _connectOptionSent = false;
    try {
      if (uri.scheme == '') {
        throw Exception(NatsException('No scheme in uri'));
      }

      _tlsRequired = uri.scheme == 'tls';

      switch (uri.scheme) {
        case 'wss':
        case 'ws':
          final channel = WebSocketChannel.connect(uri);
          _wsChannel = channel;
          try {
            // Bounded like the TCP dial of the other schemes: an open that
            // never completes would otherwise hang this attempt for good.
            await channel.ready.timeout(Duration(seconds: timeout));
          } on TimeoutException {
            _wsChannel = null;
            // A late open must not leave a second socket behind.
            unawaited(Future<void>(() async {
              try {
                await channel.sink.close();
              } catch (_) {}
            }));
            throw NatsException(
                'no websocket open with ${uri.host}:${uri.port} '
                'within ${timeout}s');
          } catch (e) {
            _wsChannel = null;
            rethrow;
          }
          _setStatus(Status.infoHandshake);
          final connId = _connectionId;
          final currentWs = _wsChannel;
          _wsChannel?.stream.listen((dynamic event) {
            if (currentWs != _wsChannel) return;
            if (_channelStream.isClosed) return;
            _channelStream.add([connId, event]);
          }, onDone: () {
            if (currentWs != _wsChannel) return;
            _setStatus(Status.disconnected);
          }, onError: (dynamic e) {
            if (currentWs != _wsChannel) return;
            if (onError != null) {
              onError!(e);
            }
            close();
            wsErrorHandler(e);
          });
          break;

        case 'nats':
          var port = uri.port;
          if (port == 0) {
            port = 4222;
          }
          _tcpSocket = await _socketConnect(
            uri.host,
            port,
            timeout: Duration(seconds: timeout),
          );
          if (_tcpSocket == null) {
            return false;
          }
          _setStatus(Status.infoHandshake);
          final connId = _connectionId;
          final currentSocket = _tcpSocket;
          _tcpSocket!.listen((event) {
            if (currentSocket != _tcpSocket) return;
            if (_secureSocket == null) {
              if (_channelStream.isClosed) return;
              _channelStream.add([connId, event]);
            }
          }, onError: (dynamic e) {
            if (currentSocket != _tcpSocket) return;
            if (onError != null) {
              onError!(e);
            }
            _setStatus(Status.disconnected);
          }).onDone(() {
            if (currentSocket != _tcpSocket) return;
            _setStatus(Status.disconnected);
          });
          _watchSinkDone(_tcpSocket!.done, () => currentSocket == _tcpSocket);
          break;

        case 'tls':
          var port = uri.port;
          if (port == 0) {
            port = 4443;
          }
          _tcpSocket = await _socketConnect(
            uri.host,
            port,
            timeout: Duration(seconds: timeout),
          );
          if (_tcpSocket == null) return false;
          _setStatus(Status.infoHandshake);
          final connId = _connectionId;
          final currentSocket = _tcpSocket;
          _tcpSocket!.listen((event) {
            if (currentSocket != _tcpSocket) return;
            if (_secureSocket == null) {
              if (_channelStream.isClosed) return;
              _channelStream.add([connId, event]);
            }
          }, onError: (dynamic e) {
            if (currentSocket != _tcpSocket) return;
            if (onError != null) {
              onError!(e);
            }
            _setStatus(Status.disconnected);
          }).onDone(() {
            if (currentSocket != _tcpSocket) return;
            _setStatus(Status.disconnected);
          });
          _watchSinkDone(_tcpSocket!.done, () => currentSocket == _tcpSocket);
          break;

        default:
          throw Exception(NatsException('schema ${uri.scheme} not support'));
      }

      // The transport is up. The attempt succeeds only when the handshake
      // driven by INFO in _processOp gets through; a failure there throws
      // here, so the calling loop counts it and waits retryInterval like any
      // other failed attempt (#56).
      final handshake = _handshake = Completer<void>();
      try {
        await handshake.future.timeout(Duration(seconds: timeout));
      } on TimeoutException {
        final e = NatsException('no handshake with ${uri.host}:${uri.port} '
            'within ${timeout}s');
        _failHandshake(e);
        throw e;
      }
      return true;
    } catch (e) {
      rethrow;
    }
  }

  /// Give a failed WRITE the same handling a failed read already gets.
  ///
  /// A [Socket] is an [IOSink], so `add()` does not throw when the write fails - the failure completes
  /// `done` instead. Nothing was watching `done`, so a write landing on a socket the peer has already
  /// aborted had no handler anywhere and surfaced as an unhandled asynchronous error. The status guard
  /// at the top of [_add] does not prevent it: the keepalive writes on its own timer, and the socket
  /// can be gone while [status] still reads connected.
  ///
  /// [isCurrent] is tested for the same reason the stream listeners test it - a reconnect replaces the
  /// transport, and a failure from the one it replaced must not disturb the new connection.
  ///
  /// The callback takes a block body deliberately: an expression body would make this `catchError<T>`
  /// over the sink's own type and reintroduce the return-type mismatch fixed in #49.
  void _watchSinkDone(Future<dynamic> done, bool Function() isCurrent) {
    unawaited(done.catchError((dynamic e) {
      if (!isCurrent()) return null;
      if (onError != null) {
        onError!(e);
      }
      _setStatus(Status.disconnected);
      return null;
    }));
  }

  void _backendSubscriptAll() {
    _backendSubs.clear();
    _subs.forEach((sid, s) {
      _sub(s.subject, sid, queueGroup: s.queueGroup);
      _backendSubs[sid] = true;
    });
  }

  void _flushPubBuffer() {
    for (final p in _pubBuffer) {
      _pub(p);
    }
    _pubBuffer.clear();
  }

  void _processOp(String line, int lineEnd) async {
    _readOffset = lineEnd + 2;

    final splitIndex = line.indexOf(' ');
    String op, data;
    if (splitIndex != -1) {
      op = line.substring(0, splitIndex).trim().toLowerCase();
      data = line.substring(splitIndex).trim();
    } else {
      op = line.trim().toLowerCase();
      data = '';
    }

    switch (op) {
      case 'msg':
        _receiveState = _ReceiveState.msg;
        _receiveLine1 = line;
        _processMsg();
        _receiveLine1 = '';
        _receiveState = _ReceiveState.idle;
        break;

      case 'hmsg':
        _receiveState = _ReceiveState.msg;
        _receiveLine1 = line;
        _processHMsg();
        _receiveLine1 = '';
        _receiveState = _ReceiveState.idle;
        break;

      case 'info':
        // This attempt's handshake. A later await can resume after the
        // attempt already failed or timed out; it must then not touch the
        // next attempt's connection.
        final handshake = _handshake;
        bool stale() =>
            handshake != null &&
            (handshake.isCompleted || !identical(handshake, _handshake));
        try {
          _info = Info.fromJson(jsonDecode(data) as Map<String, dynamic>);

          if (_info.connectUrls != null) {
            for (var urlStr in _info.connectUrls!) {
              var formatted = urlStr;
              if (!formatted.contains('://')) {
                final currentScheme =
                    _serverPool.isNotEmpty ? _serverPool.first.scheme : 'nats';
                formatted = '$currentScheme://$formatted';
              }
              try {
                final discoveredUri = Uri.parse(formatted);
                if (!_serverPool.any((u) =>
                    u.host == discoveredUri.host &&
                    u.port == discoveredUri.port)) {
                  _serverPool.add(discoveredUri);
                }
              } catch (_) {}
            }
          }

          if (_connectOptionSent) {
            break;
          }
          _connectOptionSent = true;

          if ((_tlsRequired || (_info.tlsRequired ?? false)) &&
              _tcpSocket != null) {
            _setStatus(Status.tlsHandshake);
            try {
              final secureSocket = await SecureSocket.secure(
                _tcpSocket!,
                context: securityContext,
                onBadCertificate: (certificate) {
                  return acceptBadCert;
                },
              );
              if (stale()) {
                unawaited(Future<void>(() async {
                  try {
                    await secureSocket.close();
                  } catch (_) {}
                }));
                return;
              }

              _secureSocket = secureSocket;
              final connId = _connectionId;
              secureSocket.listen((event) {
                if (secureSocket != _secureSocket) return;
                if (_channelStream.isClosed) return;
                _channelStream.add([connId, event]);
              }, onError: (dynamic error) {
                if (secureSocket != _secureSocket) return;
                _setStatus(Status.disconnected);
                if (onError != null) {
                  onError!(error);
                }
              }, onDone: () {
                if (secureSocket != _secureSocket) return;
                _setStatus(Status.disconnected);
              });
              _watchSinkDone(
                  secureSocket.done, () => secureSocket == _secureSocket);
            } on TlsException catch (e) {
              throw NatsException('TLS handshake failed: $e');
            }
          }

          await _sign();
          if (stale()) return;
          _addConnectOption(_connectOption);

          if (_connectOption.verbose == true) {
            final ack = await _ackStream.stream.first;
            if (stale()) return;
            if (!ack) {
              throw NatsException('Verbose connection failed');
            }
          } else {
            await ping();
            if (stale()) return;
          }
          // Before the status change: a callback it fires may close the
          // client, and that must not count as a failed attempt.
          if (handshake != null && !handshake.isCompleted) {
            handshake.complete();
          }
          _setStatus(Status.connected);

          _backendSubscriptAll();
          _flushPubBuffer();

          if (!_connectCompleter.isCompleted) {
            _connectCompleter.complete();
          }
        } catch (e) {
          // The loop that owns this attempt retries it or reports the error.
          if (!stale()) _failHandshake(e);
        }
        break;

      case 'ping':
        if (status == Status.connected) {
          _add('pong');
        }
        break;

      case '-err':
        if (_connectOption.verbose == true) {
          _ackStream.sink.add(false);
        }
        final exception = NatsException.fromServerError(data);
        if (onError != null) {
          onError!(exception);
        }
        if (exception is NatsPermissionsViolation &&
            exception.operation == NatsOperation.publish) {
          // The server dropped the message, so nobody will ever reply.
          _failPendingRequests(exception, subject: exception.subject);
        }
        final authFailed = exception is NatsAuthenticationException;
        if (_handshakePending && !authFailed) {
          // e.g. "maximum connections exceeded": a failed attempt, which the
          // connect loop retries.
          _failHandshake(exception);
        } else if (!_connectCompleter.isCompleted) {
          _connectCompleter.completeError(exception);
        }
        while (_pingCompleters.isNotEmpty) {
          final completer = _pingCompleters.removeAt(0);
          if (!completer.isCompleted) {
            completer.completeError(exception);
          }
        }
        if (authFailed) {
          _retry = false;
          close();
        }
        break;

      case 'pong':
        if (_pingCompleters.isNotEmpty) {
          final completer = _pingCompleters.removeAt(0);
          if (!completer.isCompleted) {
            completer.complete();
          }
        }
        break;

      case '+ok':
        if (_connectOption.verbose == true) {
          _ackStream.sink.add(true);
        }
        break;
    }
  }

  void _processMsg() {
    final s = _receiveLine1.split(' ').where((str) => str.isNotEmpty).toList();
    final subject = s[1];
    final sid = int.parse(s[2]);
    String? replyTo;
    int length;

    if (s.length == 4) {
      length = int.parse(s[3]);
    } else {
      replyTo = s[3];
      length = int.parse(s[4]);
    }

    if ((_bufferLength - _readOffset) < length) return;
    final payload = _buffer.sublist(_readOffset, _readOffset + length);

    _readOffset += length + 2; // Move past payload and trailing \r\n

    if (_subs[sid] != null) {
      _subs[sid]?.add(Message<dynamic>(
        subject,
        sid,
        payload,
        this,
        replyTo: replyTo,
      ));
    }
  }

  void _processHMsg() {
    final s = _receiveLine1.split(' ').where((str) => str.isNotEmpty).toList();
    final subject = s[1];
    final sid = int.parse(s[2]);
    String? replyTo;
    int length;
    int headerLength;

    if (s.length == 5) {
      headerLength = int.parse(s[3]);
      length = int.parse(s[4]);
    } else {
      replyTo = s[3];
      headerLength = int.parse(s[4]);
      length = int.parse(s[5]);
    }

    if ((_bufferLength - _readOffset) < length) return;
    final header = _buffer.sublist(_readOffset, _readOffset + headerLength);
    final payload =
        _buffer.sublist(_readOffset + headerLength, _readOffset + length);

    _readOffset += length + 2; // Move past payload and trailing \r\n

    if (_subs[sid] != null) {
      final msg = Message<dynamic>(
        subject,
        sid,
        payload,
        this,
        replyTo: replyTo,
        header: Header.fromBytes(header),
      );
      _subs[sid]?.add(msg);
    }
  }

  /// Get server maximum payload size configuration
  int? maxPayload() => _info.maxPayload;

  /// Send PING request and wait for PONG.
  ///
  /// With [timeout], fails with a [TimeoutException] when no PONG arrives in
  /// time -- a bounded check that the connection is still alive, for example
  /// when an app returns to the foreground. Without it, a PING into a
  /// connection that has silently died completes only when the client
  /// notices the disconnect.
  Future<void> ping({Duration? timeout}) {
    final completer = Completer<void>();
    _pingCompleters.add(completer);
    _add('ping');
    if (timeout == null) {
      return completer.future;
    }
    // The completer stays queued: PONGs are matched to PINGs in order.
    return completer.future.timeout(timeout);
  }

  void _addConnectOption(ConnectOption c) {
    _add('connect ' + jsonEncode(c.toJson()));
  }

  /// Default buffer action for pub
  var defaultPubBuffer = true;

  /// Publish a byte payload (Uint8List) to a subject.
  /// Returns true if successfully sent/buffered.
  Future<bool> pub(
    String? subject,
    Uint8List data, {
    String? replyTo,
    bool? buffer,
    Header? header,
  }) async {
    buffer ??= defaultPubBuffer;
    if (status != Status.connected) {
      if (buffer) {
        if (maxReconnectBuffer != -1 &&
            _pubBuffer.length >= maxReconnectBuffer) {
          throw NatsException('reconnect buffer full');
        }
        _pubBuffer.add(_Pub(subject, data, replyTo));
        return true;
      } else {
        return false;
      }
    }

    String cmd;
    final headerByte = header?.toBytes();
    if (header == null) {
      cmd = 'pub';
    } else {
      cmd = 'hpub';
    }
    cmd += ' $subject';
    if (replyTo != null) {
      cmd += ' $replyTo';
    }

    if (headerByte != null) {
      cmd += ' ${headerByte.length}  ${headerByte.length + data.length}';
      _add(cmd);
      final dataWithHeader = headerByte.toList();
      dataWithHeader.addAll(data.toList());
      _addByte(dataWithHeader);
    } else {
      cmd += ' ${data.length}';
      _add(cmd);
      _addByte(data);
    }

    if (_connectOption.verbose == true) {
      final ack = await _ackStream.stream.first;
      return ack;
    }
    return true;
  }

  /// Publish a string payload to a subject
  Future<bool> pubString(
    String subject,
    String str, {
    String? replyTo,
    bool buffer = true,
    Header? header,
  }) async {
    return pub(
      subject,
      Uint8List.fromList(utf8.encode(str)),
      replyTo: replyTo,
      buffer: buffer,
      header: header,
    );
  }

  Future<bool> _pub(_Pub p) async {
    if (p.replyTo == null) {
      _add('pub ${p.subject} ${p.data.length}');
    } else {
      _add('pub ${p.subject} ${p.replyTo} ${p.data.length}');
    }
    _addByte(p.data);

    if (_connectOption.verbose == true) {
      final ack = await _ackStream.stream.first;
      return ack;
    }
    return true;
  }

  T Function(String) _getJsonDecoder<T>() {
    final c = _jsonDecoder[T];
    if (c == null) {
      throw NatsException('no decoder for type $T');
    }
    return c as T Function(String);
  }

  /// Subscribe to a subject (with optional queue group and json decoder)
  Subscription<T> sub<T>(
    String subject, {
    String? queueGroup,
    T Function(String)? jsonDecoder,
  }) {
    _ssid++;

    if (T != dynamic && jsonDecoder == null) {
      jsonDecoder = _getJsonDecoder<T>();
    }

    final s = Subscription<T>(
      _ssid,
      subject,
      this,
      queueGroup: queueGroup,
      jsonDecoder: jsonDecoder,
    );
    _subs[_ssid] = s;

    if (status == Status.connected) {
      _sub(subject, _ssid, queueGroup: queueGroup);
      _backendSubs[_ssid] = true;
    }
    return s;
  }

  void _sub(String? subject, int sid, {String? queueGroup}) {
    if (queueGroup == null) {
      _add('sub $subject $sid');
    } else {
      _add('sub $subject $queueGroup $sid');
    }
  }

  /// Unsubscribe from a subscription
  bool unSub(Subscription<dynamic> s) {
    final sid = s.sid;

    if (_subs[sid] == null) return false;
    _unSub(sid);
    _subs.remove(sid);
    s.close();
    _backendSubs.remove(sid);
    return true;
  }

  /// Unsubscribe from a subscription using its subscriber ID
  bool unSubById(int sid) {
    if (_subs[sid] == null) return false;
    return unSub(_subs[sid]!);
  }

  void _unSub(int sid, {String? maxMsgs}) {
    if (maxMsgs == null) {
      _add('unsub $sid');
    } else {
      _add('unsub $sid $maxMsgs');
    }
  }

  void _add(String str) {
    if (status == Status.closed || status == Status.disconnected) {
      return;
    }
    try {
      if (_wsChannel != null) {
        _wsChannel?.sink.add(utf8.encode(str + '\r\n'));
        return;
      } else if (_secureSocket != null) {
        _secureSocket!.add(utf8.encode(str + '\r\n'));
        return;
      } else if (_tcpSocket != null) {
        _tcpSocket!.add(utf8.encode(str + '\r\n'));
        return;
      }
      throw Exception(NatsException('no connection'));
    } catch (e) {
      _setStatus(Status.disconnected);
      if (onError != null) {
        onError!(e);
      }
    }
  }

  void _addByte(List<int> msg) {
    if (status == Status.closed || status == Status.disconnected) {
      return;
    }
    try {
      if (_wsChannel != null) {
        _wsChannel?.sink.add(msg);
        _wsChannel?.sink.add(utf8.encode('\r\n'));
        return;
      } else if (_secureSocket != null) {
        _secureSocket?.add(msg);
        _secureSocket?.add(utf8.encode('\r\n'));
        return;
      } else if (_tcpSocket != null) {
        _tcpSocket?.add(msg);
        _tcpSocket?.add(utf8.encode('\r\n'));
        return;
      }
      throw Exception(NatsException('no connection'));
    } catch (e) {
      _setStatus(Status.disconnected);
      if (onError != null) {
        onError!(e);
      }
    }
  }

  var _inboxPrefix = '_INBOX';

  /// Get Inbox prefix configuration
  String get inboxPrefix => _inboxPrefix;

  /// Set Inbox prefix configuration
  set inboxPrefix(String i) {
    if (_clientStatus == _ClientStatus.used) {
      throw NatsException('inbox prefix can not change when connection in use');
    }
    _inboxPrefix = i;
    _inboxSubPrefix = null;
  }

  String? _inboxSubPrefix;
  Subscription<dynamic>? _inboxSub;
  StreamSubscription<Message<dynamic>>? _inboxListen;

  /// Requests in flight, keyed by reply inbox. Each owns its completer and
  /// nothing is shared between them, so a request nobody answers cannot hold
  /// up another.
  final _pendingRequests = <String, _PendingRequest>{};

  /// Requests that timed out, by reply inbox, oldest first -- kept only so
  /// a reply that turns up afterwards can be named to [onLateReply].
  final _abandonedRequests = <String, _AbandonedRequest>{};
  static const _abandonedRequestsKept = 32;

  void _onInboxMessage(Message<dynamic> msg) {
    final pending = _pendingRequests[msg.subject];
    if (pending == null) {
      final abandoned = _abandonedRequests.remove(msg.subject);
      if (abandoned != null && onLateReply != null) {
        onLateReply!(
            abandoned.subject, DateTime.now().difference(abandoned.at));
      }
      return;
    }
    if (pending.completer.isCompleted) return;
    if (msg.header?.status == 503 && msg.byte.isEmpty) {
      // The server's own answer on a no_responders connection.
      pending.completer
          .completeError(NatsNoRespondersException(pending.subject));
      return;
    }
    pending.completer.complete(msg);
  }

  /// Fails the requests in flight whose replies can no longer arrive: all of
  /// them, or only those sent to [subject].
  void _failPendingRequests(Object error, {String? subject}) {
    for (final pending in _pendingRequests.values.toList()) {
      if (subject != null && pending.subject != subject) continue;
      if (!pending.completer.isCompleted) {
        pending.completer.completeError(error);
      }
    }
  }

  /// Fails every request in flight because the connection is gone. Each was
  /// published, so each fails as sent.
  void _failPendingRequestsLost(String message) {
    for (final pending in _pendingRequests.values.toList()) {
      if (!pending.completer.isCompleted) {
        pending.completer.completeError(
            NatsConnectionLost(message, subject: pending.subject, sent: true));
      }
    }
  }

  /// How many requests are waiting for their reply. For draining a connection
  /// before closing it, and for telling one slow reply from a dead connection.
  int get pendingRequestCount => _pendingRequests.length;

  /// Request-Response pattern: send a request and wait for single response.
  ///
  /// Requests are independent: any number can be in flight at once, each on
  /// its own reply inbox. A request made while the client is not connected,
  /// or still waiting when the connection drops, fails with a
  /// [NatsConnectionLost] instead of waiting out its [timeout]; its `sent`
  /// says whether the request left the client.
  Future<Message<T>> request<T>(
    String subj,
    Uint8List data, {
    Duration timeout = const Duration(seconds: 2),
    T Function(String)? jsonDecoder,
    Header? header,
  }) async {
    if (!connected) {
      throw NatsConnectionLost('request error: client not connected',
          subject: subj, sent: false);
    }

    if (T != dynamic && jsonDecoder == null) {
      jsonDecoder = _getJsonDecoder<T>();
    }

    if (_inboxSub == null) {
      if (inboxPrefix == '_INBOX') {
        _inboxSubPrefix = inboxPrefix + '.' + Nuid().next();
      } else {
        _inboxSubPrefix = inboxPrefix;
      }
      final inboxSub = sub<dynamic>(_inboxSubPrefix! + '.>');
      _inboxSub = inboxSub;
      _inboxListen = inboxSub.stream.listen(_onInboxMessage);
    }
    final inbox = _inboxSubPrefix! + '.' + Nuid().next();
    final pending = _PendingRequest(subj);
    _pendingRequests[inbox] = pending;

    late Message<dynamic> resp;
    try {
      // Never buffered: a request held for a later connection would wait out
      // its timeout for a reply to a message that was not sent.
      final published =
          await pub(subj, data, replyTo: inbox, header: header, buffer: false);
      if (!published) {
        throw NatsConnectionLost('request error: client not connected',
            subject: subj, sent: false);
      }
      resp = await pending.completer.future.timeout(timeout);
    } on TimeoutException {
      if (onLateReply != null) {
        _abandonedRequests[inbox] = _AbandonedRequest(subj, DateTime.now());
        while (_abandonedRequests.length > _abandonedRequestsKept) {
          _abandonedRequests.remove(_abandonedRequests.keys.first);
        }
      }
      throw TimeoutException('request time > $timeout');
    } finally {
      _pendingRequests.remove(inbox);
    }

    final msg = Message<T>(
      resp.subject,
      resp.sid,
      resp.byte,
      this,
      header: resp.header,
      jsonDecoder: jsonDecoder,
    );
    return msg;
  }

  /// Request-Response helper sending String payload
  Future<Message<T>> requestString<T>(
    String subj,
    String data, {
    Duration timeout = const Duration(seconds: 2),
    T Function(String)? jsonDecoder,
    Header? header,
  }) {
    return request<T>(
      subj,
      Uint8List.fromList(data.codeUnits),
      timeout: timeout,
      jsonDecoder: jsonDecoder,
      header: header,
    );
  }

  /// Flush connection (perform ping/pong roundtrip)
  Future<void> flush() => ping();

  /// Gracefully drain all subscriptions and close connection
  Future<void> drain() async {
    final subs = List<Subscription>.from(_subs.values);
    await Future.wait(subs.map((s) => s.drain()));
    await close();
  }

  /// Gracefully drain a specific subscription
  Future<void> drainSubscription(Subscription s) async {
    final sid = s.sid;
    if (_subs[sid] == null) return;

    _unSub(sid);
    _backendSubs.remove(sid);

    await flush();

    _subs.remove(sid);
    await s.close();
  }

  /// Load NATS credentials from a credential file content
  void loadCredentials(String content) {
    final creds = Credentials.parse(content);
    seed = creds.seed;
    userJwtHandler = () => creds.jwt;
  }

  /// Load NATS credentials from a file path
  Future<void> loadCredentialsFile(String filepath) async {
    final file = File(filepath);
    final content = await file.readAsString();
    loadCredentials(content);
  }

  void _setStatus(Status newStatus) {
    if (_status == Status.closed && newStatus != Status.connecting) {
      return;
    }
    if (newStatus == Status.disconnected && _handshakePending) {
      // Not a disconnect: the connection was never established. The loop
      // running this attempt retries it, so no onDisconnect, no completed
      // connect future and no second reconnect loop.
      _failHandshake(NatsException('connection closed during handshake'));
      return;
    }
    if (newStatus == Status.disconnected || newStatus == Status.closed) {
      final exception = NatsException('Connection closed or disconnected');
      _failHandshake(exception);
      if (!_connectCompleter.isCompleted) {
        _connectCompleter.completeError(exception);
      }
      while (_pingCompleters.isNotEmpty) {
        final completer = _pingCompleters.removeAt(0);
        if (!completer.isCompleted) {
          completer.completeError(exception);
        }
      }
      _failPendingRequestsLost(exception.message!);
    }
    _status = newStatus;
    _statusController.add(newStatus);

    if (newStatus == Status.connected) {
      _wasConnected = true;
      _pingsOut = 0;
      _pingTimer?.cancel();
      _pingDeadline?.cancel();
      _pingTimer = Timer.periodic(pingInterval, (timer) {
        if (status != Status.connected) {
          timer.cancel();
          return;
        }
        if (_pingsOut >= maxPingsOut) {
          timer.cancel();
          _setStatus(Status.disconnected);
          _cleanUpSockets();
          return;
        }
        _pingsOut++;
        final completer = Completer<void>();
        _pingCompleters.add(completer);
        // A PONG reply proves the connection is alive -- reset the missed-
        // ping counter. Without this, _pingsOut only ever increases and the
        // heartbeat unconditionally disconnects after maxPingsOut ticks
        // regardless of whether the server is actually responding.
        //
        // The value callback needs a block body: as an expression body it
        // evaluates to the assigned int, so inference picks `then<int>` and the
        // null-returning onError handler cannot satisfy `FutureOr<int>`. These
        // completers are error-completed on every disconnect, and nothing
        // awaits this future, so that mismatch surfaced as an unhandled zone
        // error on the Dart VM and dart2wasm (dart2js elides the cast).
        completer.future.then((_) {
          _pingsOut = 0;
        }, onError: (_) {});
        _add('ping');
        final deadline = pingTimeout;
        if (deadline != null) {
          _pingDeadline?.cancel();
          _pingDeadline = Timer(deadline, () {
            if (completer.isCompleted || status != Status.connected) return;
            if (_pingsOut >= maxPingsOut) {
              timer.cancel();
              _setStatus(Status.disconnected);
              _cleanUpSockets();
            }
          });
        }
      });
      if (_reconnectCycle && onReconnect != null) {
        onReconnect!();
      } else if (!_reconnectCycle && onConnect != null) {
        onConnect!();
      }
      _reconnectCycle = false;
    } else if (newStatus == Status.disconnected) {
      _pingTimer?.cancel();
      _pingTimer = null;
      _pingDeadline?.cancel();
      _pingDeadline = null;
      _pingsOut = 0;
      if (onDisconnect != null) {
        onDisconnect!();
      }
      if (_wasConnected && _retry && !_reconnecting) {
        _reconnectLoopBackground();
      }
    } else if (newStatus == Status.closed) {
      _wasConnected = false;
      _reconnectCycle = false;
      _reconnecting = false;
      _pingTimer?.cancel();
      _pingTimer = null;
      _pingDeadline?.cancel();
      _pingDeadline = null;
      _pingsOut = 0;
      if (onClose != null) {
        onClose!();
      }
    }
  }

  void _reconnectLoopBackground() async {
    if (_reconnecting) return;
    _reconnecting = true;
    _reconnectCycle = true;

    int attempts = 0;
    final maxAttempts =
        _reconnectCount == -1 ? -1 : _reconnectCount * _serverPool.length;

    while (_retry &&
        status != Status.connected &&
        status != Status.closed &&
        (attempts < maxAttempts || maxAttempts == -1)) {
      _setStatus(Status.reconnecting);
      final currentUri = _getNextServer();
      try {
        if (_channelStream.isClosed) {
          _channelStream = StreamController<dynamic>();
        }
        _connectionId++;
        final success =
            await _connectUri(currentUri, timeout: _reconnectTimeout);
        if (success) {
          _reconnecting = false;
          return;
        }
      } catch (err) {
        await _cleanUpSocketsSoft();
        if (onError != null) {
          onError!(err);
        }
      }
      attempts++;
      if (_retry &&
          status != Status.connected &&
          status != Status.closed &&
          (attempts < maxAttempts || maxAttempts == -1)) {
        await Future<void>.delayed(Duration(seconds: _reconnectInterval));
      }
    }

    _reconnecting = false;
    _reconnectCycle = false;
    if (status != Status.connected && status != Status.closed) {
      await close();
    }
  }

  Future<Socket> _socketConnect(
    String host,
    int port, {
    Duration? timeout,
  }) async {
    final override = debugSocketConnect;
    if (override != null) {
      return await override(host, port, timeout: timeout) as Socket;
    }
    return Socket.connect(host, port, timeout: timeout);
  }

  Future<void> _cleanUpSockets() async {
    final ws = _wsChannel;
    _wsChannel = null;
    await ws?.sink.close();

    final secure = _secureSocket;
    _secureSocket = null;
    await secure?.close();

    final tcp = _tcpSocket;
    _tcpSocket = null;
    await tcp?.close();
  }

  /// Null socket refs first, then close with a short timeout so reconnect
  /// retries cannot hang on a zombie TCP close after abrupt network loss.
  Future<void> _cleanUpSocketsSoft() async {
    final ws = _wsChannel;
    _wsChannel = null;
    final secure = _secureSocket;
    _secureSocket = null;
    final tcp = _tcpSocket;
    _tcpSocket = null;

    try {
      if (ws != null) {
        await ws.sink.close().timeout(_socketCleanupTimeout);
      }
    } catch (_) {}
    try {
      if (secure != null) {
        await secure.close().timeout(_socketCleanupTimeout);
      }
    } catch (_) {}
    try {
      if (tcp != null) {
        await tcp.close().timeout(_socketCleanupTimeout);
      }
    } catch (_) {}
  }

  /// Close connection and prevent any future reconnect retries
  Future<void> forceClose() async {
    _retry = false;
    await close();
  }

  /// Close active connection to NATS server but keep subscriber list client side
  Future<void> close() async {
    _setStatus(Status.closed);
    _backendSubs.forEach((k, v) => _backendSubs[k] = false);

    final ws = _wsChannel;
    _wsChannel = null;
    await ws?.sink.close();

    final secure = _secureSocket;
    _secureSocket = null;
    await secure?.close();

    final tcp = _tcpSocket;
    _tcpSocket = null;
    await tcp?.close();

    await _inboxListen?.cancel();
    _inboxListen = null;
    _abandonedRequests.clear();
    final inboxSub = _inboxSub;
    if (inboxSub != null) {
      _subs.remove(inboxSub.sid);
      _backendSubs.remove(inboxSub.sid);
      await inboxSub.close();
    }
    _inboxSub = null;
    _inboxSubPrefix = null;
    _buffer = Uint8List(0);
    _bufferLength = 0;
    _readOffset = 0;
    _receiveState = _ReceiveState.idle;
    _clientStatus = _ClientStatus.closed;
  }

  /// Connect using raw TCP socket (deprecated: use connect(Uri) instead)
  Future<dynamic> tcpConnect(
    String host, {
    int port = 4222,
    ConnectOption? connectOption,
    int timeout = 5,
    bool retry = true,
    int retryInterval = 10,
  }) {
    return connect(
      Uri(scheme: 'nats', host: host, port: port),
      retry: retry,
      retryInterval: retryInterval,
      timeout: timeout,
      connectOption: connectOption,
    );
  }

  /// Close active TCP socket connection. Used for test simulation.
  Future<void> tcpClose() async {
    await _tcpSocket?.close();
    _setStatus(Status.disconnected);
  }

  /// Wait until client connects to NATS server
  Future<void> waitUntilConnected() async {
    await waitUntil(Status.connected);
  }

  /// Wait until client status updates to a specific state
  Future<void> waitUntil(Status s) async {
    if (status == s) {
      return;
    }
    await for (final st in statusStream) {
      if (st == s) {
        break;
      }
    }
  }

  /// Get JetStream context for the client
  JetStream jetStream() => JetStream(this);
}
