import 'dart:async';
import 'dart:typed_data';

/// Abstract security context stub for platform abstraction
abstract class SecurityContext {}

/// Abstract Socket class stub for platform abstraction
abstract class Socket extends Stream<Uint8List> {
  /// Connects to a socket
  static Future<Socket> connect(dynamic host, int port, {Duration? timeout}) {
    throw UnsupportedError('Sockets are not supported on this platform.');
  }

  /// Adds data to the socket stream
  void add(List<int> data);

  /// Completes when the sink is done, or with an error when a write failed. Declared here because a
  /// failed [add] reports nowhere else - it does not throw.
  Future<dynamic> get done;

  /// Closes the socket connection
  Future<void> close();
}

/// Abstract SecureSocket class stub for platform abstraction
abstract class SecureSocket extends Stream<Uint8List> {
  /// Secures an existing socket connection
  static Future<SecureSocket> secure(dynamic socket,
      {dynamic context, bool Function(dynamic)? onBadCertificate}) {
    throw UnsupportedError(
        'Secure sockets are not supported on this platform.');
  }

  /// Adds data to the secure socket stream
  void add(List<int> data);

  /// Completes when the sink is done, or with an error when a write failed. Declared here because a
  /// failed [add] reports nowhere else - it does not throw.
  Future<dynamic> get done;

  /// Closes the secure socket connection
  Future<void> close();
}

/// Abstract TlsException class stub for platform abstraction
abstract class TlsException {
  /// Exception message
  final String message;

  /// Constructor for TlsException
  TlsException([this.message = '']);
}

/// Abstract File class stub for platform abstraction
abstract class File {
  /// Factory constructor for File
  factory File(String path) {
    throw UnsupportedError(
        'File operations are not supported on this platform.');
  }

  /// Checks if file exists
  Future<bool> exists();

  /// Reads file contents as string
  Future<String> readAsString();
}
