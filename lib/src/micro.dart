import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';
import 'client.dart';
import 'message.dart';
import 'subscription.dart';
import 'inbox.dart';

/// Configuration for NATS Microservice
class ServiceConfig {
  /// The service name (alphanumeric, dots, dashes, underscores)
  final String name;

  /// The service version (semver format)
  final String version;

  /// Optional service description
  final String? description;

  /// Optional service metadata
  final Map<String, String>? metadata;

  /// List of endpoints managed by the service
  final List<Endpoint> endpoints;

  /// Constructor for ServiceConfig
  ServiceConfig({
    required this.name,
    required this.version,
    this.description,
    this.metadata,
    this.endpoints = const [],
  });
}

/// Endpoint definition within a Microservice
class Endpoint {
  /// Name of the endpoint
  final String name;

  /// NATS subject the endpoint listens on
  final String subject;

  /// Request handler function
  final FutureOr<void> Function(Message msg) handler;

  /// Optional endpoint metadata
  final Map<String, String>? metadata;

  /// Constructor for Endpoint
  Endpoint({
    required this.name,
    required this.subject,
    required this.handler,
    this.metadata,
  });
}

/// Statistics tracker for an Endpoint
class EndpointStats {
  /// Name of the endpoint
  final String name;

  /// Subject of the endpoint
  final String subject;

  /// Total number of requests processed
  int numRequests = 0;

  /// Total number of errors encountered
  int numErrors = 0;

  /// Description of the last error
  String lastError = '';

  /// Total processing time elapsed
  Duration totalProcessingTime = Duration.zero;

  /// Constructor for EndpointStats
  EndpointStats({required this.name, required this.subject});

  /// Export stats to JSON map
  Map<String, dynamic> toJson() {
    final avgNs = numRequests == 0
        ? 0
        : (totalProcessingTime.inMicroseconds * 1000) ~/ numRequests;
    return {
      'name': name,
      'subject': subject,
      'num_requests': numRequests,
      'num_errors': numErrors,
      'last_error': lastError,
      'processing_time': totalProcessingTime.inMicroseconds * 1000,
      'average_processing_time': avgNs,
    };
  }
}

/// Manages the NATS Microservice lifecycle (ADR-32)
class MicroService {
  /// The underlying NATS client
  final Client client;

  /// The service configuration
  final ServiceConfig config;

  /// Unique service instance ID
  final String id;

  /// Timestamp when the service started
  final DateTime started;

  final List<Subscription> _subscriptions = [];
  final Map<String, EndpointStats> _stats = {};

  /// Constructor for MicroService
  MicroService(this.client, this.config, this.id)
      : started = DateTime.now().toUtc() {
    for (var ep in config.endpoints) {
      _stats[ep.name] = EndpointStats(name: ep.name, subject: ep.subject);
    }
  }

  /// Start the microservice and subscribe to endpoints and system subjects
  Future<void> start() async {
    // 1. Subscribe to each service endpoint
    for (var ep in config.endpoints) {
      final epStats = _stats[ep.name]!;
      final sub = client.sub<dynamic>(ep.subject);
      sub.stream.listen((msg) async {
        final stopwatch = Stopwatch()..start();
        epStats.numRequests++;
        try {
          await ep.handler(msg);
        } catch (e) {
          epStats.numErrors++;
          epStats.lastError = e.toString();
        } finally {
          stopwatch.stop();
          epStats.totalProcessingTime += stopwatch.elapsed;
        }
      });
      _subscriptions.add(sub);
    }

    // 2. Subscribe to control/monitoring subjects
    final monitoringSubjects = [
      // PING
      '\$SRV.PING',
      '\$SRV.PING.${config.name}',
      '\$SRV.PING.${config.name}.$id',
      // INFO
      '\$SRV.INFO',
      '\$SRV.INFO.${config.name}',
      '\$SRV.INFO.${config.name}.$id',
      // STATS
      '\$SRV.STATS',
      '\$SRV.STATS.${config.name}',
      '\$SRV.STATS.${config.name}.$id',
    ];

    for (var subName in monitoringSubjects) {
      final sub = client.sub<dynamic>(subName);
      sub.stream.listen((msg) {
        if (msg.replyTo == null || msg.replyTo!.isEmpty) return;

        if (subName.contains('PING')) {
          msg.respondString(jsonEncode({
            'id': id,
            'name': config.name,
            'version': config.version,
            'type': 'io.nats.micro.v1.ping_response',
            'metadata': config.metadata ?? {},
          }));
        } else if (subName.contains('INFO')) {
          msg.respondString(jsonEncode({
            'id': id,
            'name': config.name,
            'version': config.version,
            'type': 'io.nats.micro.v1.info_response',
            'description': config.description ?? '',
            'metadata': config.metadata ?? {},
            'endpoints': config.endpoints
                .map((e) => {
                      'name': e.name,
                      'subject': e.subject,
                      'metadata': e.metadata ?? {},
                    })
                .toList(),
          }));
        } else if (subName.contains('STATS')) {
          final endpointsList = _stats.values.map((s) => s.toJson()).toList();
          msg.respondString(jsonEncode({
            'id': id,
            'name': config.name,
            'version': config.version,
            'type': 'io.nats.micro.v1.stats_response',
            'started': started.toIso8601String(),
            'metadata': config.metadata ?? {},
            'endpoints': endpointsList,
            'stats': {
              'endpoints': endpointsList,
            }
          }));
        }
      });
      _subscriptions.add(sub);
    }
  }

  /// Stop the service and unsubscribe all subscriptions
  Future<void> stop() async {
    for (var sub in _subscriptions) {
      await sub.close();
    }
    _subscriptions.clear();
  }
}

/// Extension on Client class to add Microservice support
extension ClientMicroServiceExtension on Client {
  /// Register and start a NATS Microservice
  Future<MicroService> addService(ServiceConfig config) async {
    final id = Nuid().next();
    final service = MicroService(this, config, id);
    await service.start();
    return service;
  }
}

/// Reply to a `$SRV.PING` discovery request (`io.nats.micro.v1.ping_response`)
class PingResponse {
  /// Unique service instance ID that replied
  final String id;

  /// Service name
  final String name;

  /// Service version
  final String version;

  /// Service metadata
  final Map<String, String> metadata;

  /// Constructor for PingResponse
  PingResponse({
    required this.id,
    required this.name,
    required this.version,
    this.metadata = const {},
  });

  /// Parse from a decoded JSON map
  factory PingResponse.fromJson(Map<String, dynamic> json) {
    return PingResponse(
      id: json['id'] as String,
      name: json['name'] as String,
      version: json['version'] as String,
      metadata:
          (json['metadata'] as Map?)?.cast<String, String>() ?? const {},
    );
  }
}

/// Endpoint summary within an [InfoResponse]
class EndpointInfo {
  /// Name of the endpoint
  final String name;

  /// NATS subject the endpoint listens on
  final String subject;

  /// Endpoint metadata
  final Map<String, String> metadata;

  /// Constructor for EndpointInfo
  EndpointInfo({
    required this.name,
    required this.subject,
    this.metadata = const {},
  });

  /// Parse from a decoded JSON map
  factory EndpointInfo.fromJson(Map<String, dynamic> json) {
    return EndpointInfo(
      name: json['name'] as String,
      subject: json['subject'] as String,
      metadata:
          (json['metadata'] as Map?)?.cast<String, String>() ?? const {},
    );
  }
}

/// Reply to a `$SRV.INFO` discovery request (`io.nats.micro.v1.info_response`)
class InfoResponse {
  /// Unique service instance ID that replied
  final String id;

  /// Service name
  final String name;

  /// Service version
  final String version;

  /// Service description
  final String description;

  /// Service metadata
  final Map<String, String> metadata;

  /// Endpoints exposed by this service instance
  final List<EndpointInfo> endpoints;

  /// Constructor for InfoResponse
  InfoResponse({
    required this.id,
    required this.name,
    required this.version,
    this.description = '',
    this.metadata = const {},
    this.endpoints = const [],
  });

  /// Parse from a decoded JSON map
  factory InfoResponse.fromJson(Map<String, dynamic> json) {
    return InfoResponse(
      id: json['id'] as String,
      name: json['name'] as String,
      version: json['version'] as String,
      description: json['description'] as String? ?? '',
      metadata:
          (json['metadata'] as Map?)?.cast<String, String>() ?? const {},
      endpoints: (json['endpoints'] as List? ?? const [])
          .map((e) => EndpointInfo.fromJson(e as Map<String, dynamic>))
          .toList(),
    );
  }
}

/// Per-endpoint statistics within a [StatsResponse]
class EndpointStatsInfo {
  /// Name of the endpoint
  final String name;

  /// NATS subject the endpoint listens on
  final String subject;

  /// Total number of requests processed
  final int numRequests;

  /// Total number of errors encountered
  final int numErrors;

  /// Description of the last error, if any
  final String lastError;

  /// Total processing time elapsed, in nanoseconds
  final int processingTimeNs;

  /// Average processing time per request, in nanoseconds
  final int averageProcessingTimeNs;

  /// Constructor for EndpointStatsInfo
  EndpointStatsInfo({
    required this.name,
    required this.subject,
    this.numRequests = 0,
    this.numErrors = 0,
    this.lastError = '',
    this.processingTimeNs = 0,
    this.averageProcessingTimeNs = 0,
  });

  /// Parse from a decoded JSON map
  factory EndpointStatsInfo.fromJson(Map<String, dynamic> json) {
    return EndpointStatsInfo(
      name: json['name'] as String,
      subject: json['subject'] as String,
      numRequests: (json['num_requests'] as num?)?.toInt() ?? 0,
      numErrors: (json['num_errors'] as num?)?.toInt() ?? 0,
      lastError: json['last_error'] as String? ?? '',
      processingTimeNs: (json['processing_time'] as num?)?.toInt() ?? 0,
      averageProcessingTimeNs:
          (json['average_processing_time'] as num?)?.toInt() ?? 0,
    );
  }
}

/// Reply to a `$SRV.STATS` discovery request (`io.nats.micro.v1.stats_response`)
class StatsResponse {
  /// Unique service instance ID that replied
  final String id;

  /// Service name
  final String name;

  /// Service version
  final String version;

  /// When this service instance started
  final DateTime started;

  /// Service metadata
  final Map<String, String> metadata;

  /// Per-endpoint statistics
  final List<EndpointStatsInfo> endpoints;

  /// Constructor for StatsResponse
  StatsResponse({
    required this.id,
    required this.name,
    required this.version,
    required this.started,
    this.metadata = const {},
    this.endpoints = const [],
  });

  /// Parse from a decoded JSON map
  factory StatsResponse.fromJson(Map<String, dynamic> json) {
    var endpointsList = json['endpoints'] as List?;
    if (endpointsList == null) {
      final stats = json['stats'] as Map<String, dynamic>?;
      if (stats != null) {
        endpointsList = stats['endpoints'] as List?;
      }
    }
    return StatsResponse(
      id: json['id'] as String,
      name: json['name'] as String,
      version: json['version'] as String,
      started: DateTime.parse(json['started'] as String),
      metadata:
          (json['metadata'] as Map?)?.cast<String, String>() ?? const {},
      endpoints: (endpointsList ?? const [])
          .map((e) => EndpointStatsInfo.fromJson(e as Map<String, dynamic>))
          .toList(),
    );
  }
}

/// Extension on Client class adding Microservice discovery (ADR-32 client side).
///
/// Complements [ClientMicroServiceExtension.addService]: where that lets a
/// client *host* a service, this lets a client *find* services already
/// running on the account by fanning a request out to the same `$SRV.*`
/// control subjects and collecting every reply that arrives within a
/// bounded window (multiple instances can legitimately reply to one
/// discovery request, so this is not a single-reply [Client.request]).
extension ClientServiceDiscoveryExtension on Client {
  Future<List<T>> _collectServiceReplies<T>(
    String verb,
    T Function(Map<String, dynamic>) fromJson, {
    String? name,
    String? id,
    required Duration timeout,
  }) async {
    if (id != null && name == null) {
      throw ArgumentError(
          'Cannot target a specific service instance by ID without specifying the service name.');
    }

    var subject = '\$SRV.$verb';
    if (name != null) {
      subject += '.$name';
      if (id != null) {
        subject += '.$id';
      }
    }

    final replySubject = newInbox(inboxPrefix: inboxPrefix);
    final replySub = sub<dynamic>(replySubject);
    final results = <T>[];

    final listener = replySub.stream.listen((msg) {
      try {
        results.add(fromJson(jsonDecode(msg.string) as Map<String, dynamic>));
      } catch (_) {
        // Ignore replies that don't conform to the expected shape rather
        // than letting one misbehaving responder abort discovery for
        // everyone else.
      }
    });

    await pub(subject, Uint8List(0), replyTo: replySubject);
    await Future<void>.delayed(timeout);

    await listener.cancel();
    unSub(replySub);

    return results;
  }

  /// Discover running services by fanning out a `$SRV.PING` request and
  /// collecting every reply that arrives within [timeout]. Pass [name] to
  /// target one service (every instance of it replies), or both [name] and
  /// [id] to target a single instance.
  Future<List<PingResponse>> discoverServices({
    String? name,
    String? id,
    Duration timeout = const Duration(milliseconds: 500),
  }) {
    return _collectServiceReplies(
      'PING',
      PingResponse.fromJson,
      name: name,
      id: id,
      timeout: timeout,
    );
  }

  /// Fetch `$SRV.INFO` (endpoints and subjects) for every running service,
  /// or a single service/instance when [name]/[id] are given.
  Future<List<InfoResponse>> getServicesInfo({
    String? name,
    String? id,
    Duration timeout = const Duration(milliseconds: 500),
  }) {
    return _collectServiceReplies(
      'INFO',
      InfoResponse.fromJson,
      name: name,
      id: id,
      timeout: timeout,
    );
  }

  /// Fetch `$SRV.STATS` (request/error counts, latency) for every running
  /// service, or a single service/instance when [name]/[id] are given.
  Future<List<StatsResponse>> getServicesStats({
    String? name,
    String? id,
    Duration timeout = const Duration(milliseconds: 500),
  }) {
    return _collectServiceReplies(
      'STATS',
      StatsResponse.fromJson,
      name: name,
      id: id,
      timeout: timeout,
    );
  }
}
