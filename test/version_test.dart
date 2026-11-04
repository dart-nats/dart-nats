import 'package:dart_nats/dart_nats.dart';
import 'package:dart_nats/src/version.dart';
import 'package:test/test.dart';

import '../tool/write_version.dart';

void main() {
  test('CONNECT reports this package as the client version', () {
    final option = ConnectOption();
    expect(option.lang, 'dart');
    expect(option.version, packageVersion);
    expect(option.toJson()['version'], packageVersion);
  });

  test('a version set by the caller is kept', () {
    expect(ConnectOption(version: '9.9.9').version, '9.9.9');
  });

  group('tool/write_version.dart', () {
    test('reads the version line, not other version-like keys', () {
      const pubspec = 'name: dart_nats\n'
          'description: x\n'
          'version: 1.7.1\n'
          'environment:\n'
          "  sdk: '>=2.15.0 <4.0.0'\n";
      expect(pubspecVersion(pubspec), '1.7.1');
      expect(pubspecVersion('version: 2.0.0-dev.1  \n'), '2.0.0-dev.1');
      expect(pubspecVersion('name: x\n'), isNull);
    });

    test('writes a constant the client can use', () {
      expect(versionFile('1.2.3'),
          contains("const String packageVersion = '1.2.3';"));
    });
  });
}
