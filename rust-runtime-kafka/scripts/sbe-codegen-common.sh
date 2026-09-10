#!/usr/bin/env bash

# Shared SBE tool pin and generation helpers. This file is sourced by the
# public scripts in this directory; keep their command lines stable.

readonly SBE_VERSION="1.38.1"
readonly SBE_SHA256="26e29e5503faa70cc56158fcdacd6369ecfc3088b9a61c90b882feb54451c9b5"
readonly SBE_JAR="${SBE_JAR:-/tmp/sbe-all-${SBE_VERSION}.jar}"
SBE_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly SBE_REPO_ROOT
readonly SBE_IR_MODULE_GENERATOR="${SBE_REPO_ROOT}/tools/trace-tool/generate-sbe-ir-module.js"

sbe_require_tooling() {
    if [[ ! -f "${SBE_JAR}" ]]; then
        echo "missing pinned SBE tool: ${SBE_JAR}" >&2
        echo "set SBE_JAR to an sbe-all-${SBE_VERSION}.jar path" >&2
        return 1
    fi

    if ! command -v deno >/dev/null 2>&1; then
        echo "deno is required to bundle the browser SBE IR" >&2
        return 1
    fi

    local actual_sha256
    actual_sha256="$(shasum -a 256 "${SBE_JAR}" | awk '{print $1}')"
    if [[ "${actual_sha256}" != "${SBE_SHA256}" ]]; then
        echo "unexpected SHA-256 for ${SBE_JAR}" >&2
        echo "expected ${SBE_SHA256}" >&2
        echo "actual   ${actual_sha256}" >&2
        return 1
    fi
}

sbe_generate() {
    local target_language="$1"
    local output_dir="$2"
    local schema="$3"

    java \
        --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
        -Dsbe.generate.ir=true \
        -Dsbe.target.language="${target_language}" \
        -Dsbe.output.dir="${output_dir}" \
        -Dsbe.errorLog=yes \
        -jar "${SBE_JAR}" \
        "${schema}"
}

sbe_bundle_ir() {
    local input_ir="$1"
    local global_name="$2"
    local output_module="$3"

    deno run --no-config \
        --allow-read="${input_ir}" \
        --allow-write="${output_module}" \
        "${SBE_IR_MODULE_GENERATOR}" \
        "${global_name}" \
        "${SBE_VERSION}" \
        "${input_ir}" \
        "${output_module}"
}

# Generates one schema's browser IR module (via the Java generator, which
# emits exactly one .sbeir file) and installs it at <output.js>.
sbe_bundle_ir_from_schema() (
    set -euo pipefail

    local schema="$1"
    local global_name="$2"
    local output="$3"

    if [[ ! -f "${schema}" ]]; then
        echo "missing SBE schema: ${schema}" >&2
        exit 1
    fi

    local temp_output
    temp_output="$(mktemp -d "${TMPDIR:-/tmp}/browser-sbe-ir.XXXXXX")"
    trap 'rm -rf "${temp_output}"' EXIT

    sbe_require_tooling
    sbe_generate Java "${temp_output}" "${schema}"

    local ir_files=("${temp_output}"/*.sbeir)
    if [[ "${#ir_files[@]}" -ne 1 || ! -f "${ir_files[0]}" ]]; then
        echo "expected the SBE tool to emit exactly one .sbeir file" >&2
        exit 1
    fi

    local generated_module="${temp_output}/schema-sbe-ir.js"
    sbe_bundle_ir "${ir_files[0]}" "${global_name}" "${generated_module}"

    mkdir -p "$(dirname "${output}")"
    cp "${generated_module}" "${output}"
)

sbe_regenerate_rust_and_ir() (
    set -euo pipefail

    local check_only="$1"
    local schema="$2"
    local generated_crate="$3"
    local global_name="$4"
    local rust_output="$5"
    local ir_output="$6"
    local description="$7"
    local schema_stem
    schema_stem="$(basename "${schema}" .xml)"

    local temp_output
    temp_output="$(mktemp -d "${TMPDIR:-/tmp}/${schema_stem}-sbe.XXXXXX")"
    trap 'rm -rf "${temp_output}"' EXIT

    sbe_require_tooling
    sbe_generate Rust "${temp_output}" "${schema}"

    local generated_output="${temp_output}/${generated_crate}/src"
    local generated_ir="${temp_output}/${schema_stem}.sbeir"
    local generated_module="${temp_output}/${schema_stem}-sbe-ir.js"
    sbe_bundle_ir "${generated_ir}" "${global_name}" "${generated_module}"

    find "${generated_output}" -type f -name '*.rs' -print0 \
        | xargs -0 rustfmt --edition 2024
    find "${generated_output}" -type f -name '*.rs' -print0 \
        | xargs -0 perl -pi -e 's/[ \t]+$//'

    if [[ "${check_only}" == "--check" ]]; then
        diff -ru "${rust_output}" "${generated_output}"
        diff -u "${ir_output}" "${generated_module}"
        echo "${description} SBE codecs match ${schema}"
        exit 0
    fi

    rm -rf "${rust_output}"
    mkdir -p "${rust_output}"
    cp "${generated_output}/"*.rs "${rust_output}/"
    cp "${generated_module}" "${ir_output}"
)
