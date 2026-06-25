#!/usr/bin/env bash
#
# tests/e2e/common/cleanup-ghcr-image.sh
#
# Delete a single tag of a GHCR container image. Used by
# csi-integration.yml:cleanup-image and the 3 csi-e2e jobs to
# clean up the test image they pushed.
#
# Usage:
#   . tests/e2e/common/cleanup-ghcr-image.sh
#   cleanup_ghcr_image <image_ref> [owner]
#
#   image_ref: full ref like ghcr.io/dyrnq/mntrs-csi:ci-test-abc123
#   owner:    defaults to the segment after ghcr.io/ in image_ref
#             (e.g. dyrnq or an org like my-org). Required if image_ref
#             is missing the registry prefix.
#
# Two callers per CI run — but the script is idempotent: deleting a
# tag that doesn't exist returns 404 from the gh API, which we
# absorb with `|| true`. Safe to run unconditionally on success OR
# failure (caller decides via `if: always()`).
#
# Why we don't just `DELETE /versions` (the old behavior):
#   The previous csi-integration.yml cleanup-image job called
#   `DELETE /orgs/<owner>/packages/container/mntrs-csi/versions` which
#   deletes ALL versions of the package — including any other run
#   that pushed its own image in parallel. Under CI concurrency,
#   a fast finish from one run would nuke the still-in-use image
#   of a slower run, breaking downstream retries. The version_id
#   lookup + targeted DELETE used here is safe under concurrency.

# Guard against double-include.
if [[ -n "${__CLEANUP_GHCR_IMAGE_LOADED:-}" ]]; then
    return 0 2>/dev/null || true
fi
__CLEANUP_GHCR_IMAGE_LOADED=1

cleanup_ghcr_image() {
    local image_ref="$1"
    local owner="${2:-}"

    if [ -z "$image_ref" ]; then
        echo "::error::cleanup_ghcr_image: image_ref is required"
        return 1
    fi

    # Parse "ghcr.io/<owner>/<pkg>:<tag>" → owner, pkg, tag.
    # Use parameter expansion — robust against edge cases like a
    # registry port (ghcr.io:443/...). The first '/' splits registry
    # from path; the next '/' splits owner from pkg; ':' splits pkg
    # from tag.
    if [ -z "$owner" ]; then
        # Strip the registry prefix (everything up to and including
        # the first '/'). Result: "<owner>/<pkg>:<tag>".
        local path_after_registry="${image_ref#*/}"
        owner="${path_after_registry%%/*}"
    fi

    local path_after_owner="${image_ref#*/*/}"   # "<pkg>:<tag>"
    local pkg="${path_after_owner%%:*}"          # "<pkg>"
    local tag="${path_after_owner##*:}"          # "<tag>"

    if [ -z "$owner" ] || [ -z "$pkg" ] || [ -z "$tag" ]; then
        echo "::error::cleanup_ghcr_image: failed to parse '$image_ref' (got owner='$owner' pkg='$pkg' tag='$tag')"
        return 1
    fi

    # Auth: GHA injects GH_TOKEN in CI; for local dev, gh auth login
    # must be done beforehand. || true because the user may have
    # already authenticated.
    if [ -n "${GH_TOKEN:-}" ] && ! gh auth status >/dev/null 2>&1; then
        echo "$GH_TOKEN" | gh auth login --with-token 2>/dev/null || true
    fi

    if ! gh auth status >/dev/null 2>&1; then
        echo "::warning::gh not authenticated; skipping cleanup of $image_ref"
        return 0
    fi

    # List all versions of the package, filter for our tag, extract
    # the version_id. GitHub Packages has no "delete by tag" API; we
    # must look up the version_id, then DELETE that one version.
    #
    # The versions endpoint returns paginated JSON. We pull 100 per
    # page (max), which is well above our needs (~3-5 versions/week
    # at current CI frequency). If we ever exceed 100 active tags,
    # we'll need pagination — but a 100-tag backlog is its own bug.
    local version_id
    version_id=$(
        gh api "/orgs/${owner}/packages/container/${pkg}/versions?per_page=100" 2>/dev/null \
            | grep -B2 -F "\"name\": \"${tag}\"" \
            | grep -oE '"id": [0-9]+' \
            | head -1 \
            | grep -oE '[0-9]+' \
            || true
    )

    if [ -z "$version_id" ]; then
        # Already gone, or the tag doesn't exist (e.g. push failed
        # earlier in the run). Idempotent — silent success.
        echo "::notice::cleanup_ghcr_image: tag '$tag' not found on $pkg (already cleaned up?)"
        return 0
    fi

    # Delete ONLY this version_id — leaves every other concurrent
    # run's image alone.
    if gh api --method DELETE \
        "/orgs/${owner}/packages/container/${pkg}/versions/${version_id}" \
        >/dev/null 2>&1; then
        echo "::notice::deleted $image_ref (version_id=$version_id)"
    else
        echo "::warning::failed to delete $image_ref (version_id=$version_id) — likely a permission or rate-limit issue"
        return 0  # don't fail the run on cleanup failure
    fi
}

# Direct invocation dispatch (when not sourced).
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    set -euo pipefail
    cleanup_ghcr_image "$@"
fi
