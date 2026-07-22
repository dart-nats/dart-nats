import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

void main() {
  group('AccountInfo.fromJson', () {
    test('reads usage/limits from the top level of a single-tier response', () {
      // Captured verbatim from a live `nats:latest -js` server's
      // `$JS.API.INFO` response after creating one stream. Single-tier
      // (non-multi-tenant) accounts — the common case — have no `tier` key
      // at all; the account's own usage/limits are top-level fields.
      final json = {
        'type': 'io.nats.jetstream.api.v1.account_info_response',
        'memory': 0,
        'storage': 0,
        'reserved_memory': 0,
        'reserved_storage': 18446744073709552000.0,
        'streams': 1,
        'consumers': 0,
        'limits': {
          'max_memory': -1,
          'max_storage': -1,
          'max_streams': -1,
          'max_consumers': -1,
        },
        'api': {'level': 1, 'total': 8, 'errors': 0},
      };

      final info = AccountInfo.fromJson(json);

      expect(info.tier.streams, 1);
      expect(info.tier.consumers, 0);
      expect(info.api.total, 8);
      expect(info.api.level, 1);
      expect(info.domain, '');
      expect(info.tiers, isEmpty);
    });

    test(
        'parses multi-tier accounts into the tiers map, keeping the '
        'top-level fields as the account-wide aggregate', () {
      final json = {
        'streams': 2,
        'api': {'total': 1},
        'tiers': {
          'R1': {'streams': 2, 'memory': 100},
          'R3': {'streams': 0, 'memory': 0},
        },
      };

      final info = AccountInfo.fromJson(json);

      expect(info.tier.streams, 2);
      expect(info.tiers.keys, containsAll(['R1', 'R3']));
      expect(info.tiers['R1']!.streams, 2);
      expect(info.tiers['R1']!.memory, 100);
    });

    test('defaults missing fields to zero/empty', () {
      final info = AccountInfo.fromJson({});
      expect(info.domain, '');
      expect(info.tier.memory, 0);
      expect(info.tier.streams, 0);
      expect(info.api.total, 0);
      expect(info.tiers, isEmpty);
    });
  });

  group('Tier.fromJson', () {
    test(
        'does not throw on a huge double sentinel (a server\'s uint64 "-1 '
        'unlimited" for reserved_storage, observed to round-trip through '
        'JSON as 18446744073709552000.0)', () {
      final tier = Tier.fromJson({'reserved_storage': 18446744073709552000.0});
      expect(tier.reservedStorage, greaterThan(0));
    });
  });
}
