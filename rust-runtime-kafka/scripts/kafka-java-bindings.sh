#!/bin/sh
# Generation uses the exact checksummed jextract distribution in the manifest.
set -eu
exec python3 "$(dirname "$0")/kafka-java-bindings.py" "$@"
