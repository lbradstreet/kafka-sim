# shellcheck shell=bash
# Shared helpers for the trace-viewer check scripts. This file is sourced by
# the public scripts in this directory; sourcing it changes the working
# directory to the repository root.

CHECK_REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
readonly CHECK_REPO_ROOT
cd "${CHECK_REPO_ROOT}" || exit 1

# The viewer-agnostic core sources checked by every viewer script.
# shellcheck disable=SC2034 # consumed by the sourcing scripts
readonly -a TRACE_VIEWER_SHARED_JS=(
    tools/trace-tool/trace-viewer-core.js
    tools/trace-tool/trace-viewer-core.test.js
    tools/trace-tool/trace-viewer-ui.js
    tools/trace-tool/trace-viewer-ui.test.js
)

# require_deno <description>
require_deno() {
    if ! command -v deno >/dev/null 2>&1; then
        echo "deno is required for the $1 checks" >&2
        exit 1
    fi
}

# require_registered_test <test-name> <description> <cargo test args...>
#
# Fails when the named test is not registered in the listed target, so a
# renamed or deleted scenario test cannot silently turn the check into a
# no-op.
require_registered_test() {
    local test_name="$1"
    local description="$2"
    shift 2
    local listing
    listing="$(cargo test "$@" --locked -- --list)"
    if [[ "${listing}" != *"${test_name}: test"* ]]; then
        echo "required ${description} is not registered: ${test_name}" >&2
        exit 1
    fi
}
