import 'dart:async';
import 'dart:typed_data';

/// SecurityContext implementation for Web platform abstraction
class SecurityContext {}

/// Socket class implementation stub for Web platform abstraction
class Socket extends Stream<Uint8List> {
  /// Connects to a TCP socket
  static Future<Socket> connect(dynamic host, int port, {Duration? timeout}) {
    throw UnsupportedError('TCP sockets are not supported on the web.');
  }

  @override
  StreamSubscription<Uint8List> listen(void Function(Uint8List event)? onData,
      {Function? onError, void Function()? onDone, bool? cancelOnError}) {
    throw UnsupportedError('TCP sockets are not supported on the web.');
  }

  /// Adds data to the socket stream
  void add(List<int> data) {}

  /// Never reached: [connect] throws, so no instance of this exists on the web.
  Future<dynamic> get done => Future<dynamic>.value();

  /// Closes the socket connection
  Future<void> close() async {}
}

/// SecureSocket class implementation stub for Web platform abstraction
class SecureSocket extends Stream<Uint8List> {
  /// Secures an existing socket connection
  static Future<SecureSocket> secure(dynamic socket,
      {dynamic context, bool Function(dynamic)? onBadCertificate}) {
    throw UnsupportedError('Secure TCP sockets are not supported on the web.');
  }

  @override
  StreamSubscription<Uint8List> listen(void Function(Uint8List event)? onData,
      {Function? onError, void Function()? onDone, bool? cancelOnError}) {
    throw UnsupportedError('Secure TCP sockets are not supported on the web.');
  }

  /// Adds data to the secure socket stream
  void add(List<int> data) {}

  /// Never reached: [secure] throws, so no instance of this exists on the web.
  Future<dynamic> get done => Future<dynamic>.value();

  /// Closes the secure socket connection
  Future<void> close() async {}
}

/// TlsException class implementation stub for Web platform abstraction
class TlsException {
  /// Exception message
  final String message;

  /// Constructor for TlsException
  TlsException([this.message = '']);
}

/// File class implementation stub for Web platform abstraction
class File {
  /// The file path
  final String path;

  /// Constructor for File
  File(this.path);

  /// Checks if file exists
  Future<bool> exists() async {
    return false;
  }

  /// Reads file contents as string
  Future<String> readAsString() async {
    throw UnsupportedError('File operations are not supported on the web.');
  }
}
