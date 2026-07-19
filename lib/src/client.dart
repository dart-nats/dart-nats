import 'dart:async';
import 'dart:convert';
import 'platform/platform.dart';
import 'dart:typed_data';

import 'package:meta/meta.dart';
import 'package:mutex/mutex.dart';
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

  /// User authentication callbacks
  String Function()? userJwtHandler;

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
  Timer? _pingTimer;
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
    bool randomizeServers = true,
    int maxReconnectBuffer = 1000,
  }) async {
    _retry = retry;
    this.securityContext = securityContext;
    this.pingInterval = pingInterval;
    this.maxPingsOut = maxPingsOut;
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

    if (_clientStatus == _ClientStatus.used) {
      throw Exception(
          NatsException('client in use. must close before call connect'));
    }
    if (status != Status.disconnected && status != Status.closed) {
      return Future.error('Error: status not disconnected and not closed');
    }

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

        _buffer = Uint8List(0);
        _bufferLength = 0;
        _readOffset = 0;
        return;
      } catch (err, st) {
        await _cleanUpSockets();
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
      _connectCompleter.completeError(
          NatsException('can not connect to any servers in the pool'));
    }
  }

  Future<bool> _connectUri(Uri uri, {int timeout = 5}) async {
    _connectOptionSent = false;
    try {
      if (uri.scheme == '') {
        throw Exception(NatsException('No scheme in uri'));
      }

      _tlsRequired = uri.scheme == 'tls';

      switch (uri.scheme) {
        case 'wss':
        case 'ws':
          try {
            _wsChannel = WebSocketChannel.connect(uri);
            await _wsChannel!.ready;
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
          return true;

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
          return true;

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
          });
          return true;

        default:
          throw Exception(NatsException('schema ${uri.scheme} not support'));
      }
    } catch (e) {
      rethrow;
    }
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

              _secureSocket = secureSocket;
              final connId = _connectionId;
              secureSocket.listen((event) {
                if (secureSocket != _secureSocket) return;
                if (_channelStream.isClosed) return;
                _channelStream.add([connId, event]);
              }, onError: (dynamic error) {
                if (secureSocket != _secureSocket) return;
                print('Socket error: $error');
                _setStatus(Status.disconnected);
                if (onError != null) {
                  onError!(error);
                }

                if (error is TlsException) {
                  _retry = false;
                  close();
                  throw Exception(NatsException(error.message));
                }
              }, onDone: () {
                if (secureSocket != _secureSocket) return;
                _setStatus(Status.disconnected);
              });
            } catch (e) {
              _setStatus(Status.disconnected);
              rethrow;
            }
          }

          await _sign();
          _addConnectOption(_connectOption);

          if (_connectOption.verbose == true) {
            final ack = await _ackStream.stream.first;
            if (ack) {
              _setStatus(Status.connected);
            } else {
              _setStatus(Status.disconnected);
              throw NatsException('Verbose connection failed');
            }
          } else {
            await ping();
            _setStatus(Status.connected);
          }

          _backendSubscriptAll();
          _flushPubBuffer();

          if (!_connectCompleter.isCompleted) {
            _connectCompleter.complete();
          }
        } catch (e) {
          if (!_connectCompleter.isCompleted) {
            _connectCompleter.completeError(e);
          }
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
        final exception = NatsException(data);
        if (onError != null) {
          onError!(exception);
        }
        if (!_connectCompleter.isCompleted) {
          _connectCompleter.completeError(exception);
        }
        while (_pingCompleters.isNotEmpty) {
          final completer = _pingCompleters.removeAt(0);
          if (!completer.isCompleted) {
            completer.completeError(exception);
          }
        }
        if (data.toLowerCase().contains('authorization violation') ||
            data.toLowerCase().contains('authentication')) {
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

  /// Send PING request and wait for PONG
  Future<void> ping() {
    final completer = Completer<void>();
    _pingCompleters.add(completer);
    _add('ping');
    return completer.future;
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

  final _inboxs = <String, Subscription<dynamic>>{};
  final _mutex = Mutex();
  String? _inboxSubPrefix;
  Subscription<dynamic>? _inboxSub;

  /// Request-Response pattern: send a request and wait for single response
  Future<Message<T>> request<T>(
    String subj,
    Uint8List data, {
    Duration timeout = const Duration(seconds: 2),
    T Function(String)? jsonDecoder,
    Header? header,
  }) async {
    if (!connected) {
      throw NatsException('request error: client not connected');
    }
    late Message<dynamic> resp;
    await _mutex.acquire();

    if (T != dynamic && jsonDecoder == null) {
      jsonDecoder = _getJsonDecoder<T>();
    }

    if (_inboxSubPrefix == null) {
      if (inboxPrefix == '_INBOX') {
        _inboxSubPrefix = inboxPrefix + '.' + Nuid().next();
      } else {
        _inboxSubPrefix = inboxPrefix;
      }
      _inboxSub =
          sub<dynamic>(_inboxSubPrefix! + '.>', jsonDecoder: jsonDecoder);
    }
    final inbox = _inboxSubPrefix! + '.' + Nuid().next();
    final stream = _inboxSub!.stream;

    await pub(subj, data, replyTo: inbox, header: header);

    try {
      do {
        resp = await stream.take(1).single.timeout(timeout);
      } while (resp.subject != inbox);
    } on TimeoutException {
      throw TimeoutException('request time > $timeout');
    } finally {
      _mutex.release();
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
    if (newStatus == Status.disconnected || newStatus == Status.closed) {
      final exception = NatsException('Connection closed or disconnected');
      if (!_connectCompleter.isCompleted) {
        _connectCompleter.completeError(exception);
      }
      while (_pingCompleters.isNotEmpty) {
        final completer = _pingCompleters.removeAt(0);
        if (!completer.isCompleted) {
          completer.completeError(exception);
        }
      }
    }
    _status = newStatus;
    _statusController.add(newStatus);

    if (newStatus == Status.connected) {
      _wasConnected = true;
      _pingsOut = 0;
      _pingTimer?.cancel();
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
        _add('ping');
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
    final maxAttempts = _reconnectCount == -1
        ? -1
        : _reconnectCount * _serverPool.length;

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
          _buffer = Uint8List(0);
          _bufferLength = 0;
          _readOffset = 0;
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

    Future<void> closeQuietly(Future<dynamic>? closing) async {
      if (closing == null) return;
      try {
        await closing.timeout(_socketCleanupTimeout);
      } catch (_) {}
    }

    await closeQuietly(ws?.sink.close());
    await closeQuietly(secure?.close());
    await closeQuietly(tcp?.close());
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
    _inboxs.clear();

    final ws = _wsChannel;
    _wsChannel = null;
    await ws?.sink.close();

    final secure = _secureSocket;
    _secureSocket = null;
    await secure?.close();

    final tcp = _tcpSocket;
    _tcpSocket = null;
    await tcp?.close();

    await _inboxSub?.close();
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
