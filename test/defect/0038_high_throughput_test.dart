import 'dart:async';
import 'dart:typed_data';
import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

// Issue #38: https://github.com/dart-nats/dart-nats/issues/38

// Resolves high throughput performance degradation / stream bottlenecks under high message throughput.

void main() {
  group('High Throughput (Issue #38)', () {
    test('0038 Process 50,000 messages under high load without bottleneck',
        () async {
      final client = Client();
      await client.connect(Uri.parse('nats://localhost:4222'));

      final subject = 'high.throughput.test';
      final sub = client.sub(subject);

      final totalMessages = 50000;
      int receivedCount = 0;
      final completer = Completer<void>();

      // Listen to the message stream
      final subscription = sub.stream.listen((msg) {
        receivedCount++;
        if (receivedCount >= totalMessages) {
          if (!completer.isCompleted) {
            completer.complete();
          }
        }
      });

      // Prepare 100 bytes payload
      final payload = Uint8List(100);
      for (var i = 0; i < 100; i++) {
        payload[i] = i % 256;
      }

      final stopwatch = Stopwatch()..start();

      // Publish 50,000 messages as fast as possible without delay
      for (var i = 0; i < totalMessages; i++) {
        client.pub(subject, payload, buffer: false);
      }

      // Wait for the stream to process all messages (timeout after 20 seconds to prevent hang if there is a bottleneck)
      await completer.future.timeout(const Duration(seconds: 20));
      stopwatch.stop();

      print(
          'Processed $totalMessages messages in ${stopwatch.elapsedMilliseconds}ms (${(totalMessages / (stopwatch.elapsedMilliseconds / 1000)).toStringAsFixed(2)} msgs/sec)');

      expect(receivedCount, equals(totalMessages));

      await subscription.cancel();
      await client.close();
    });
  });
}
