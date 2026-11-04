// Writes lib/src/version.dart from pubspec.yaml's `version:`, so the
// version the client reports in CONNECT is always the package's own.
//
// The publish job in .github/workflows/ci.yml runs this right before
// `dart pub publish`, so a release never needs it run by hand. Run it
// yourself (`dart run tool/write_version.dart`) to bring the checked-in
// copy up to date after changing the version.
import 'dart:io';

/// The `version:` value in [pubspec], or null if there isn't one.
String? pubspecVersion(String pubspec) =>
    RegExp(r'^version:\s*(\S+)\s*$', multiLine: true)
        .firstMatch(pubspec)
        ?.group(1);

String versionFile(String version) => '''
// Written by tool/write_version.dart from pubspec.yaml. The publish job runs
// it before every release, so a published package always has its own version
// here; don't edit by hand.

/// This package's version. The client sends it as `version` in CONNECT, which
/// servers show in their connection lists (`/connz`, `nats server report
/// connections`).
const String packageVersion = '$version';
''';

void main() {
  final version = pubspecVersion(File('pubspec.yaml').readAsStringSync());
  if (version == null) {
    stderr.writeln('pubspec.yaml has no version: line');
    exit(1);
  }
  File('lib/src/version.dart').writeAsStringSync(versionFile(version));
  print('lib/src/version.dart: $version');
}
