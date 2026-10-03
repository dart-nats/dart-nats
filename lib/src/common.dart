Map<String, dynamic> _removeNull(Map<String, dynamic> data) {
  var data2 = <String, dynamic>{};

  data.forEach((s, d) {
    if (d != null) data2[s] = d;
  });
  return data2;
}

///NATS Server Info
class Info {
  /// sever id
  String? serverId;

  /// server name
  String? serverName;

  /// server version
  String? version;

  /// protocol
  int? proto;

  /// server go version
  String? go;

  /// host
  String? host;

  /// port number
  int? port;

  /// TLS Required
  bool? tlsRequired;

  /// max payload
  int? maxPayload;

  /// nounce
  String? nonce;

  ///client id assigned by server
  int? clientId;

  /// Dynamic cluster connect URLs
  List<String>? connectUrls;

  //todo
  //authen required
  //tls_required
  //tls_verify

  ///constructure
  Info(
      {this.serverId,
      this.serverName,
      this.version,
      this.proto,
      this.go,
      this.host,
      this.port,
      this.tlsRequired,
      this.maxPayload,
      this.nonce,
      this.clientId,
      this.connectUrls});

  ///constructure from json
  Info.fromJson(Map<String, dynamic> json) {
    serverId = json['server_id'];
    serverName = json['server_name'];
    version = json['version'];
    proto = json['proto'];
    go = json['go'];
    host = json['host'];
    port = json['port'];
    tlsRequired = json['tls_required'];
    maxPayload = json['max_payload'];
    nonce = json['nonce'];
    clientId = json['client_id'];
    if (json['connect_urls'] != null) {
      connectUrls = List<String>.from(json['connect_urls']);
    }
  }

  ///convert to json
  Map<String, dynamic> toJson() {
    final data = <String, dynamic>{};
    data['server_id'] = serverId;
    data['server_name'] = serverName;
    data['version'] = version;
    data['proto'] = proto;
    data['go'] = go;
    data['host'] = host;
    data['port'] = port;
    data['tls_required'] = tlsRequired;
    data['max_payload'] = maxPayload;
    data['nonce'] = nonce;
    data['client_id'] = clientId;
    data['connect_urls'] = connectUrls;

    return _removeNull(data);
  }
}

///connection option to send to server
class ConnectOption {
  ///NATS server send +OK or not (default nats server is turn on)  this client will auto tuen off as after connect
  bool? verbose;

  ///
  bool? pedantic;

  /// TLS require or not //not implement yet
  bool? tlsRequired;

  /// Auehtnticatio Token
  String? authToken;

  /// JWT
  String? jwt;

  /// NKEY
  String? nkey;

  /// signature jwt.sig = sign(hash(jwt.header + jwt.body), private-key(jwt.issuer))(jwt.issuer is part of jwt.body)
  String? sig;

  /// username
  String? user;

  /// password
  String? pass;

  /// client connection name / app name
  String? name;

  /// lang??
  String? lang;

  /// sever version
  String? version;

  /// headers
  bool? headers;

  ///protocol
  int? protocol;

  ///construcure
  ConnectOption(
      {this.verbose = false,
      this.pedantic,
      this.authToken,
      this.jwt,
      this.nkey,
      this.user,
      this.pass,
      this.tlsRequired,
      this.name,
      this.lang = 'dart',
      this.version = '0.6.0',
      this.headers = true,
      this.protocol = 1});

  ///constructure from json
  ConnectOption.fromJson(Map<String, dynamic> json) {
    verbose = json['verbose'];
    pedantic = json['pedantic'];
    tlsRequired = json['tls_required'];
    authToken = json['auth_token'];
    jwt = json['jwt'];
    nkey = json['nkey'];
    sig = json['sig'];
    user = json['user'];
    pass = json['pass'];
    name = json['name'];
    lang = json['lang'];
    version = json['version'];
    headers = json['headers'];
    protocol = json['protocol'];
  }

  ///export to json
  Map<String, dynamic> toJson() {
    final data = <String, dynamic>{};
    data['verbose'] = verbose;
    data['pedantic'] = pedantic;
    data['tls_required'] = tlsRequired;
    data['auth_token'] = authToken;
    data['jwt'] = jwt;
    data['nkey'] = nkey;
    data['sig'] = sig;
    data['user'] = user;
    data['pass'] = pass;
    data['name'] = name;
    data['lang'] = lang;
    data['version'] = version;
    data['headers'] = headers;
    data['protocol'] = protocol;

    return _removeNull(data);
  }
}

/// Nats Exception
class NatsException implements Exception {
  /// Description of the cause of the timeout.
  final String? message;

  /// NatsException
  NatsException(this.message);

  /// The exception for a server `-ERR` line, as its most specific type.
  ///
  /// [data] is the text after `-ERR`, kept verbatim as [message]. An error
  /// this library does not recognise is a plain [NatsException].
  factory NatsException.fromServerError(String data) {
    final text = data.toLowerCase();
    final permission = _permissionsViolation.firstMatch(data);
    if (permission != null) {
      return NatsPermissionsViolation(
        data,
        operation: permission.group(1)!.toLowerCase() == 'publish'
            ? NatsOperation.publish
            : NatsOperation.subscribe,
        subject: permission.group(2)!,
        queue: permission.group(3),
      );
    }
    if (text.contains('authorization violation')) {
      return NatsAuthorizationViolation(data);
    }
    if (text.contains('authentication expired') ||
        text.contains('authentication revoked')) {
      return NatsAuthenticationExpired(data);
    }
    if (text.contains('authentication')) {
      return NatsAuthenticationException(data);
    }
    if (text.contains('stale connection')) {
      return NatsStaleConnection(data);
    }
    if (text.contains('maximum connections exceeded')) {
      return NatsMaxConnectionsExceeded(data);
    }
    if (text.contains('maximum payload violation')) {
      return NatsMaxPayloadViolation(data);
    }
    return NatsException(data);
  }

  static final _permissionsViolation = RegExp(
    r'permissions violation for (publish|subscription) to "([^"]*)"'
    r'(?: using queue "([^"]*)")?',
    caseSensitive: false,
  );

  @override
  String toString() {
    var result = 'NatsException';
    if (message != null) result = '$result: $message';
    return result;
  }
}

/// What a client was doing when the server refused it.
enum NatsOperation {
  /// Publishing to a subject
  publish,

  /// Subscribing to a subject
  subscribe,
}

/// The server refused a publish or a subscription the connection has no
/// permission for. The connection stays up; the server drops the message or
/// the subscription.
class NatsPermissionsViolation extends NatsException {
  /// Whether a publish or a subscription was refused
  final NatsOperation operation;

  /// The subject that was refused
  final String subject;

  /// The queue group of a refused subscription, when it named one
  final String? queue;

  /// NatsPermissionsViolation
  NatsPermissionsViolation(
    String? message, {
    required this.operation,
    required this.subject,
    this.queue,
  }) : super(message);
}

/// The server rejected the connection's credentials or closed it over them.
/// The client closes and does not retry.
class NatsAuthenticationException extends NatsException {
  /// NatsAuthenticationException
  NatsAuthenticationException(String? message) : super(message);
}

/// The server answered CONNECT with `Authorization Violation`.
class NatsAuthorizationViolation extends NatsAuthenticationException {
  /// NatsAuthorizationViolation
  NatsAuthorizationViolation(String? message) : super(message);
}

/// The credentials of an established connection expired or were revoked.
class NatsAuthenticationExpired extends NatsAuthenticationException {
  /// NatsAuthenticationExpired
  NatsAuthenticationExpired(String? message) : super(message);
}

/// The server closed the connection as stale: it stopped hearing PONGs.
class NatsStaleConnection extends NatsException {
  /// NatsStaleConnection
  NatsStaleConnection(String? message) : super(message);
}

/// The server is at its connection limit.
class NatsMaxConnectionsExceeded extends NatsException {
  /// NatsMaxConnectionsExceeded
  NatsMaxConnectionsExceeded(String? message) : super(message);
}

/// A published message was larger than the server's `max_payload`.
class NatsMaxPayloadViolation extends NatsException {
  /// NatsMaxPayloadViolation
  NatsMaxPayloadViolation(String? message) : super(message);
}

/// nkeys Exception
class NkeysException implements Exception {
  /// Description of the cause of the timeout.
  final String? message;

  /// NkeysException
  NkeysException(this.message);

  @override
  String toString() {
    var result = 'NkeysException';
    if (message != null) result = '$result: $message';
    return result;
  }
}

/// NATS Credentials helper class to parse user credentials files (.creds)
class Credentials {
  /// The user JWT string
  final String jwt;

  /// The NKEY seed string
  final String seed;

  /// Constructor for Credentials
  Credentials({required this.jwt, required this.seed});

  /// Parse a NATS credentials file content
  factory Credentials.parse(String content) {
    final lines = content.split(RegExp(r'\r?\n'));
    bool inJwt = false;
    bool inSeed = false;
    final jwtLines = <String>[];
    final seedLines = <String>[];

    for (var line in lines) {
      final trimmed = line.trim();
      if (trimmed.contains('BEGIN') && trimmed.contains('JWT')) {
        inJwt = true;
        continue;
      }
      if (trimmed.contains('END') && trimmed.contains('JWT')) {
        inJwt = false;
        continue;
      }
      if (trimmed.contains('BEGIN') &&
          (trimmed.contains('SEED') || trimmed.contains('NKEY'))) {
        inSeed = true;
        continue;
      }
      if (trimmed.contains('END') &&
          (trimmed.contains('SEED') || trimmed.contains('NKEY'))) {
        inSeed = false;
        continue;
      }

      if (inJwt) {
        jwtLines.add(trimmed);
      }
      if (inSeed) {
        seedLines.add(trimmed);
      }
    }

    final jwt = jwtLines.join('\n').trim();
    final seed = seedLines.join('\n').trim();

    if (jwt.isEmpty || seed.isEmpty) {
      throw NatsException(
          'Failed to parse credentials: missing USER JWT or NKEY SEED block');
    }

    return Credentials(jwt: jwt, seed: seed);
  }
}
