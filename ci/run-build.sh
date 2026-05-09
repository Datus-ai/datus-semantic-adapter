#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PACKAGES=(
  "datus-semantic-core"
  "datus-semantic-metricflow"
)

usage() {
  cat <<'USAGE'
Usage: ci/run-build.sh [--list] [--dry-run] [package ...]

Builds semantic adapter workspace packages.

Options:
  --list      List configured packages.
  --dry-run   Print selected build commands without running them.
  -h, --help  Show this help.
USAGE
}

require_command() {
  local command_name="$1"
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Missing required command: $command_name" >&2
    exit 127
  fi
}

list_packages() {
  local package
  for package in "${PACKAGES[@]}"; do
    printf '%s\n' "$package"
  done
}

requested_packages=()
dry_run=0
dist_dir="${DIST_DIR:-$ROOT_DIR/dist}"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --list)
      list_packages
      exit 0
      ;;
    --dry-run)
      dry_run=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      while [ "$#" -gt 0 ]; do
        requested_packages+=("$1")
        shift
      done
      ;;
    -*)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
    *)
      requested_packages+=("$1")
      shift
      ;;
  esac
done

should_run_package() {
  local package="$1"
  if [ "${#requested_packages[@]}" -eq 0 ]; then
    return 0
  fi

  local requested
  for requested in "${requested_packages[@]}"; do
    if [ "$requested" = "$package" ]; then
      return 0
    fi
  done
  return 1
}

require_command uv

for package in "${PACKAGES[@]}"; do
  if ! should_run_package "$package"; then
    continue
  fi

  output_dir="$dist_dir/$package"

  echo ""
  echo "=== Build package: $package ==="
  if [ "$dry_run" -eq 1 ]; then
    echo "uv build --package $package --out-dir $output_dir"
    continue
  fi

  rm -rf "$output_dir"
  mkdir -p "$output_dir"
  uv build --package "$package" --out-dir "$output_dir"
done
