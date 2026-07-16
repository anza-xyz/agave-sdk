#!/usr/bin/env bash

set -euo pipefail

here="$(dirname "$0")"
src_root="$(readlink -f "${here}/..")"
cd "${src_root}"

if [[ -n ${BASE_SHA:-} && -n ${PACKAGE:-} ]]; then
  echo "Only one of BASE_SHA or PACKAGE should be provided"
  exit 1
fi

if [[ -n ${PACKAGE:-} ]]; then
  cargo semver-checks --package "${PACKAGE}"
  exit 0
fi

if [[ -z ${BASE_SHA:-} ]]; then
  echo "Either BASE_SHA or PACKAGE should be provided"
  exit 1
fi

mapfile -t members < <(toml get Cargo.toml workspace.members | jq -r '.[]')
changed_manifests=()

for member in "${members[@]}"; do
  manifest="${member%/}/Cargo.toml"
  package="$(toml get -r "${manifest}" package.name)"
  current_version="$(toml get -r "${manifest}" package.version)"
  base_manifest="$(mktemp)"

  if git show "${BASE_SHA}:${manifest}" > "${base_manifest}" 2>/dev/null; then
    base_version="$(toml get -r "${base_manifest}" package.version)"
  else
    base_version=""
  fi
  rm -f "${base_manifest}"

  echo "${package}: ${base_version:-<new>} -> ${current_version}"
  if [[ "${base_version}" != "${current_version}" ]]; then
    changed_manifests+=("${manifest}")
  fi
done

if [[ ${#changed_manifests[@]} -eq 0 ]]; then
  echo "No workspace member versions changed"
  exit 0
fi

for manifest in "${changed_manifests[@]}"; do
  cargo semver-checks --manifest-path "${manifest}"
done
