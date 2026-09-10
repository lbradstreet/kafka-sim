#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly ROOT
# shellcheck source=scripts/sbe-codegen-common.sh
source "${ROOT}/scripts/sbe-codegen-common.sh"

if [[ "$#" -ne 3 ]]; then
    echo "usage: $0 <schema.xml> <global-name> <output.js>" >&2
    exit 2
fi

sbe_bundle_ir_from_schema "$1" "$2" "$3"
