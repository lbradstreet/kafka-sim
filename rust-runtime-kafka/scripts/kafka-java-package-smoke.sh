#!/usr/bin/env bash
# Run on the target architecture/JDK; uses only the released jar's native resources.
set -euo pipefail
if [[ $# != 4 ]]; then
  echo 'usage: kafka-java-package-smoke.sh JAVA_HOME RELEASE_JAR DEPENDENCY_DIRECTORY SOURCE_ROOT' >&2
  exit 2
fi
jdk=$1
jar=$2
deps=$3
root=$4
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
modulepath="$jar:$deps"
classpath="$jar:$deps/*"
"$jdk/bin/javac" --release 25 --module-path "$modulepath" -d "$work" \
  "$root/kafka/kr-kafka-java/src/test/packaging/module-info.java" \
  "$root/kafka/kr-kafka-java/src/test/packaging/release/smoke/PackagedStartup.java"
"$jdk/bin/java" --enable-native-access=ALL-UNNAMED -cp "$work:$classpath" release.smoke.PackagedStartup
"$jdk/bin/java" --enable-native-access=io.krkafka --module-path "$work:$modulepath" \
  --module release.smoke/release.smoke.PackagedStartup
