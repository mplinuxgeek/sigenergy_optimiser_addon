#!/bin/bash
# Usage: ./release.sh [major|minor|patch]   (default: patch)
set -euo pipefail

BUMP=${1:-patch}
ADDON_CONFIG="sigenergy_optimiser/config.yaml"
WORKFLOW_FILE=".github/workflows/build.yml"
POLL_INTERVAL=5
MAX_WAIT_SECONDS=3600
COMMIT_MESSAGE=""

github_repo() {
  git remote get-url origin | sed -E 's#(git@github.com:|https://github.com/)##; s#\.git$##'
}

current_branch() {
  git rev-parse --abbrev-ref HEAD
}

github_api() {
  local url=$1
  if [ -n "${GITHUB_TOKEN:-}" ]; then
    curl -fsSL \
      -H "Accept: application/vnd.github+json" \
      -H "Authorization: Bearer ${GITHUB_TOKEN}" \
      "$url"
  else
    curl -fsSL \
      -H "Accept: application/vnd.github+json" \
      "$url"
  fi
}

actions_url() {
  local repo=$1
  echo "https://github.com/${repo}/actions"
}

wait_for_release_run() {
  local repo=$1
  local tag=$2
  local tag_sha=$3
  local workflow_file=$4
  local start_ts
  local run_json=""
  local run_id=""
  local status=""
  local conclusion=""
  local html_url=""
  local head_branch=""
  local elapsed=0
  local spinner='|/-\'
  local i=0

  start_ts=$(date +%s)

  while :; do
    if ! run_json=$(
      github_api "https://api.github.com/repos/${repo}/actions/workflows/$(basename "$workflow_file")/runs?event=push&per_page=20" |
        jq -c --arg sha "$tag_sha" --arg tag "$tag" '
          .workflow_runs
          | map(select(.head_sha == $sha and ((.head_branch // "") == $tag or ((.display_title // "") | contains($tag)))))
          | sort_by(.created_at)
          | last // empty
        '
    ); then
      echo ""
      echo "Warning: could not query GitHub Actions API to wait for ${tag}."
      echo "Set GITHUB_TOKEN for private repos or to avoid API limits."
      echo "Actions: $(actions_url "$repo")"
      return 0
    fi

    if [ -n "$run_json" ]; then
      run_id=$(printf '%s' "$run_json" | jq -r '.id')
      break
    fi

    elapsed=$(( $(date +%s) - start_ts ))
    if [ "$elapsed" -ge "$MAX_WAIT_SECONDS" ]; then
      echo ""
      echo "Timed out waiting for workflow run for ${tag}." >&2
      return 1
    fi

    printf "\rWaiting for GitHub Actions run for %s %c" "$tag" "${spinner:$i:1}"
    i=$(( (i + 1) % 4 ))
    sleep "$POLL_INTERVAL"
  done

  while :; do
    if ! run_json=$(github_api "https://api.github.com/repos/${repo}/actions/runs/${run_id}"); then
      echo ""
      echo "Warning: lost access while polling GitHub Actions for ${tag}."
      echo "Set GITHUB_TOKEN for private repos or to avoid API limits."
      echo "Actions: $(actions_url "$repo")"
      return 0
    fi
    status=$(printf '%s' "$run_json" | jq -r '.status')
    conclusion=$(printf '%s' "$run_json" | jq -r '.conclusion // ""')
    html_url=$(printf '%s' "$run_json" | jq -r '.html_url')
    head_branch=$(printf '%s' "$run_json" | jq -r '.head_branch // ""')

    if [ "$status" = "completed" ]; then
      printf "\r"
      if [ "$conclusion" = "success" ]; then
        echo "GitHub Actions completed successfully for ${tag}."
        echo "Workflow: ${html_url}"
        return 0
      fi

      echo "GitHub Actions failed for ${tag}."
      echo "Conclusion: ${conclusion:-unknown}"
      echo "Workflow: ${html_url}"
      return 1
    fi

    elapsed=$(( $(date +%s) - start_ts ))
    if [ "$elapsed" -ge "$MAX_WAIT_SECONDS" ]; then
      echo ""
      echo "Timed out waiting for workflow completion for ${tag}." >&2
      [ -n "$html_url" ] && echo "Workflow: ${html_url}"
      return 1
    fi

    printf "\rWaiting for GitHub Actions run for %s (%s/%s) %c" "$tag" "$head_branch" "$status" "${spinner:$i:1}"
    i=$(( (i + 1) % 4 ))
    sleep "$POLL_INTERVAL"
  done
}

prompt_commit_message() {
  local default_message=$1

  if [ -r /dev/tty ]; then
    read -e -r -p "Commit message [${default_message}]: " COMMIT_MESSAGE </dev/tty || true
  fi

  COMMIT_MESSAGE=${COMMIT_MESSAGE:-$default_message}
}

# ── Validate bump type ────────────────────────────────────────────────────────
case "$BUMP" in
  major|minor|patch) ;;
  *) echo "Usage: $0 [major|minor|patch]" >&2; exit 1 ;;
esac

# ── Read current version from add-on config.yaml ─────────────────────────────
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
REPO=$(github_repo)
BRANCH=$(current_branch)

if git rev-parse -q --verify "refs/tags/${TAG}" >/dev/null 2>&1; then
  echo "Error: tag ${TAG} already exists." >&2
  exit 1
fi

echo "Bumping $CURRENT → $NEW ($BUMP)"

# ── Update version in add-on config.yaml ─────────────────────────────────────
sed -i "s/^version: .*/version: \"${NEW}\"/" "$ADDON_CONFIG"

# ── Stage and commit everything ───────────────────────────────────────────────
git add -A

if git diff --cached --quiet; then
  echo "Error: nothing staged for commit." >&2
  exit 1
fi

prompt_commit_message "Release ${TAG}"
git commit -m "$COMMIT_MESSAGE"

# ── Push branch, tag, push tag ────────────────────────────────────────────────
git tag "$TAG"
git push origin "$BRANCH"
git push origin "$TAG"
TAG_SHA=$(git rev-list -n 1 "$TAG")

echo ""
echo "Released ${TAG} — waiting for GitHub Actions to build and publish the container."
echo "Add-on image will be available at:"
echo "  ghcr.io/${REPO}"

wait_for_release_run "$REPO" "$TAG" "$TAG_SHA" "$WORKFLOW_FILE"
