#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
DOCKERFILE="${DOCKERFILE:-$ROOT_DIR/sigenergy_optimiser/Dockerfile}"
CONTEXT_DIR="${CONTEXT_DIR:-$ROOT_DIR}"
IMAGE_TAG="${IMAGE_TAG:-sigenergy-optimiser:test}"
PLATFORMS="${PLATFORMS:-linux/amd64,linux/arm64,linux/arm/v7}"
MODE="${MODE:-ci}"
BUILDER_NAME="${BUILDER_NAME:-sigenergy-optimiser-test}"

usage() {
  cat <<EOF
Usage: $0 [ci|single]

Modes:
  ci      Reproduce the GitHub Action build with buildx across:
          ${PLATFORMS}
  single  Build only the current host platform and load the image locally.

Environment overrides:
  DOCKERFILE   Dockerfile path (default: ${DOCKERFILE})
  CONTEXT_DIR  Docker build context (default: ${CONTEXT_DIR})
  IMAGE_TAG    Local test tag (default: ${IMAGE_TAG})
  PLATFORMS    Buildx platforms CSV for ci mode (default: ${PLATFORMS})
  BUILDER_NAME Buildx builder name (default: ${BUILDER_NAME})

Examples:
  $0 ci
  $0 single
  DOCKERFILE=$ROOT_DIR/Dockerfile $0 single
EOF
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  usage
  exit 0
fi

if [ "${1:-}" != "" ]; then
  MODE="$1"
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is required but was not found in PATH." >&2
  exit 1
fi

if [ ! -f "$DOCKERFILE" ]; then
  echo "Dockerfile not found: $DOCKERFILE" >&2
  exit 1
fi

if [ ! -d "$CONTEXT_DIR" ]; then
  echo "Build context not found: $CONTEXT_DIR" >&2
  exit 1
fi

echo "Build test configuration"
echo "  mode:       $MODE"
echo "  dockerfile: $DOCKERFILE"
echo "  context:    $CONTEXT_DIR"
echo "  tag:        $IMAGE_TAG"

run_single() {
  echo "Running single-platform local docker build"
  docker build \
    --file "$DOCKERFILE" \
    --tag "$IMAGE_TAG" \
    "$CONTEXT_DIR"
}

ensure_buildx_builder() {
  if ! docker buildx inspect "$BUILDER_NAME" >/dev/null 2>&1; then
    docker buildx create --name "$BUILDER_NAME" --use >/dev/null
  else
    docker buildx use "$BUILDER_NAME" >/dev/null
  fi
  docker buildx inspect --bootstrap >/dev/null
}

run_ci() {
  echo "Running multi-platform buildx test"
  echo "  platforms:  $PLATFORMS"
  ensure_buildx_builder
  docker buildx build \
    --file "$DOCKERFILE" \
    --platform "$PLATFORMS" \
    --tag "$IMAGE_TAG" \
    --progress=plain \
    --output=type=cacheonly \
    "$CONTEXT_DIR"
}

case "$MODE" in
  ci)
    run_ci
    ;;
  single)
    run_single
    ;;
  *)
    echo "Unsupported mode: $MODE" >&2
    usage >&2
    exit 1
    ;;
esac
