#!/bin/bash
# Usage: ./release.sh [major|minor|patch]   (default: patch)
set -euo pipefail

BUMP=${1:-patch}
ADDON_CONFIG="sigenergy_optimiser/config.yaml"

# ── Validate bump type ────────────────────────────────────────────────────────
case "$BUMP" in
  major|minor|patch) ;;
  *) echo "Usage: $0 [major|minor|patch]" >&2; exit 1 ;;
esac

# ── Require clean working tree ────────────────────────────────────────────────
if [ -n "$(git status --porcelain)" ]; then
  echo "Error: working tree is dirty — commit or stash changes first." >&2
  exit 1
fi

# ── Read current version from add-on config.yaml ─────────────────────────────
if ! git ls-files --error-unmatch "$ADDON_CONFIG" >/dev/null 2>&1; then
  echo "Error: $ADDON_CONFIG is not tracked by git." >&2
  exit 1
fi

CURRENT=$(grep '^version:' "$ADDON_CONFIG" | sed 's/version: *"\?//;s/"\?$//')
if [ -z "$CURRENT" ]; then
  echo "Error: could not read version from $ADDON_CONFIG" >&2
  exit 1
fi

MAJOR=$(echo "$CURRENT" | cut -d. -f1)
MINOR=$(echo "$CURRENT" | cut -d. -f2)
PATCH=$(echo "$CURRENT" | cut -d. -f3)

case "$BUMP" in
  major) MAJOR=$((MAJOR+1)); MINOR=0; PATCH=0 ;;
  minor) MINOR=$((MINOR+1)); PATCH=0 ;;
  patch) PATCH=$((PATCH+1)) ;;
esac

NEW="${MAJOR}.${MINOR}.${PATCH}"
TAG="v${NEW}"

echo "Bumping $CURRENT → $NEW ($BUMP)"

# ── Update version in add-on config.yaml ─────────────────────────────────────
sed -i "s/^version: .*/version: \"${NEW}\"/" "$ADDON_CONFIG"

# ── Commit, tag, push ─────────────────────────────────────────────────────────
git add "$ADDON_CONFIG"
git commit -m "Release ${TAG}"
git tag "$TAG"
git push
git push origin "$TAG"

echo ""
echo "Released ${TAG} — GitHub Actions will now build and publish the container."
echo "Add-on image will be available at:"
echo "  ghcr.io/$(git remote get-url origin | sed 's|.*github.com[:/]||;s|\.git$||')"
