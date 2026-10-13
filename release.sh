#!/usr/bin/env bash
#
# Release every workspace crate that has changed since its last release.
# See the Releasing section of DEVELOPING.md.
#
# For each workspace member, the release tag is `<crate>-<version>`. The
# script decides what to do per crate:
#
#   * The version in Cargo.toml has no tag yet: it was bumped by hand (for
#     example because `cargo semver-checks` demanded it). Release it as is.
#   * The version is tagged and nothing under the crate directory or the
#     root Cargo.toml changed since that tag: skip it.
#   * The version is tagged but the crate changed since then: run
#     `cargo set-version --bump patch`, commit the bump, and release the
#     new version.
#
# Before anything is tagged, HEAD must already be on the remote branch with a
# green CI run, so a release is never cut from untested code.
#
# Each released crate gets a tag on the (possibly new) HEAD. The branch is
# pushed first, then each tag in its own push: GitHub creates no push events
# at all when more than three tags arrive in one push, and without an event
# the release workflow never runs. It publishes each tagged crate to
# crates.io.
#
# Usage:
#   ./release.sh            bump, tag and push
#   ./release.sh --dry-run  show what would happen, change nothing

set -euo pipefail

remote="${RELEASE_REMOTE:-origin}"
branch="${RELEASE_BRANCH:-main}"
ci_workflow="${RELEASE_CI_WORKFLOW:-build-and-check.yml}"
dry_run=false

for arg in "$@"; do
    case "$arg" in
        -n|--dry-run) dry_run=true ;;
        -h|--help)
            sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "error: unknown argument '$arg' (try --help)" >&2
            exit 2
            ;;
    esac
done

die() {
    echo "error: $*" >&2
    exit 1
}

# `cargo set-version` comes from cargo-edit, installed as the `cargo-set-version`
# binary on PATH like any other cargo subcommand.
for tool in git cargo jq gh cargo-set-version; do
    command -v "$tool" >/dev/null && continue
    case "$tool" in
        cargo-set-version) die "'cargo set-version' is required; install it with: cargo install cargo-edit" ;;
        *) die "'$tool' is required but not installed" ;;
    esac
done

cd "$(git rev-parse --show-toplevel)"

# A release tag must point at a commit that is exactly what the working tree
# contains, and that commit must be on the release branch so the tag is
# reachable from it once pushed.
current_branch="$(git rev-parse --abbrev-ref HEAD)"
[ "$current_branch" = "$branch" ] \
    || die "releases are cut from '$branch', but you are on '$current_branch'"

[ -z "$(git status --porcelain --untracked-files=no)" ] \
    || die "the working tree has uncommitted changes; commit or stash them first"

echo "Fetching '$remote' so existing release tags are known..."
git fetch --quiet --tags "$remote" "$branch"

git merge-base --is-ancestor "$remote/$branch" HEAD \
    || die "'$branch' is behind '$remote/$branch'; pull first"

# CI only runs on pushed commits, so a release must start from a HEAD that is
# already on the remote and has a green run there.
git merge-base --is-ancestor HEAD "$remote/$branch" \
    || die "HEAD has commits that are not on '$remote/$branch'; push them and wait for CI first"

echo "Checking the '$ci_workflow' run for $(git rev-parse --short HEAD)..."
ci="$(gh run list --workflow "$ci_workflow" --commit "$(git rev-parse HEAD)" --limit 1 \
    --json status,conclusion,url \
    --jq '.[0] // empty | "\(.status)|\(.conclusion)|\(.url)"')"
IFS='|' read -r ci_status ci_conclusion ci_url <<<"$ci"
case "$ci_status/$ci_conclusion" in
    completed/success) echo "  CI is green: $ci_url" ;;
    /)                 die "no '$ci_workflow' run found for HEAD; has it been pushed?" ;;
    completed/*)       die "CI finished with '$ci_conclusion': $ci_url" ;;
    *)                 die "CI is still '$ci_status'; wait for it to finish: $ci_url" ;;
esac

# `name version dir` for every publishable workspace crate. `--no-deps`
# restricts the metadata to workspace members.
crates() {
    cargo metadata --no-deps --format-version 1 \
        | jq -r '.packages[] | select(.publish != false)
                 | "\(.name) \(.version) \(.manifest_path | rtrimstr("/Cargo.toml"))"'
}

# Next patch version for a semver string, ignoring any pre-release suffix.
next_patch() {
    local major minor patch
    IFS=. read -r major minor patch <<<"$1"
    patch="${patch%%[-+]*}"
    echo "$major.$minor.$((patch + 1))"
}

# Refuse to release a version that is older than one already tagged for this
# crate; that almost always means a bump was lost in a merge.
check_not_downgrade() {
    local crate="$1" version="$2" latest
    latest="$(
        { git tag --list "$crate-[0-9]*" | sed "s/^$crate-//"; echo "$version"; } \
            | sort -V | tail -n 1
    )"
    [ "$latest" = "$version" ] \
        || die "$crate $version is older than the already released $crate $latest"
}

commit_message() {
    local joined
    joined="$(IFS=,; echo "${bump_summary[*]}")"
    echo "chore: release ${joined//,/, }"
}

# Sort crates into: already bumped by hand (release as is), changed since the
# last tag (bump patch first), and unchanged (skip).
release_tags=()     # tags to create
bump_crates=()      # crates that need `cargo set-version --bump patch`
bump_versions=()    # the version each one must land on, same order
bump_summary=()     # "crate old -> new", for the commit message
while read -r crate version dir; do
    tag="$crate-$version"

    if ! git rev-parse --quiet --verify "refs/tags/$tag" >/dev/null; then
        check_not_downgrade "$crate" "$version"
        echo "  $crate $version: bumped by hand, will tag $tag"
        release_tags+=("$tag")
        continue
    fi

    if git diff --quiet "$tag" HEAD -- "$dir" Cargo.toml; then
        echo "  $crate $version: unchanged since $tag"
        continue
    fi

    next="$(next_patch "$version")"
    check_not_downgrade "$crate" "$next"
    echo "  $crate $version: changed since $tag, will bump to $next"
    bump_crates+=("$crate")
    bump_versions+=("$next")
    bump_summary+=("$crate $next")
    release_tags+=("$crate-$next")
done < <(crates)

if [ "${#release_tags[@]}" -eq 0 ]; then
    echo "Nothing to release: no crate has changed since its last release."
    exit 0
fi

if $dry_run; then
    echo "Dry run: would run:"
    for crate in ${bump_crates[@]+"${bump_crates[@]}"}; do
        echo "  cargo set-version --bump patch -p $crate"
    done
    [ "${#bump_crates[@]}" -eq 0 ] || echo "  git commit -m '$(commit_message)'"
    echo "  git tag ${release_tags[*]}"
    echo "  git push $remote $branch"
    for tag in "${release_tags[@]}"; do
        echo "  git push $remote $tag"
    done
    exit 0
fi

if [ "${#bump_crates[@]}" -gt 0 ]; then
    for crate in "${bump_crates[@]}"; do
        cargo set-version --bump patch -p "$crate"
    done
    git add -- '*/Cargo.toml'

    # Make sure the bumps landed on the versions the tags were computed from.
    for i in "${!bump_crates[@]}"; do
        actual="$(crates | awk -v crate="${bump_crates[$i]}" '$1 == crate { print $2 }')"
        [ "$actual" = "${bump_versions[$i]}" ] \
            || die "${bump_crates[$i]} is at $actual after the bump, expected ${bump_versions[$i]}; bump left staged"
    done

    # The pre-commit hooks (fmt, clippy, tests, semver-checks) run here. If
    # they fail, the bumped Cargo.toml files are left staged for inspection.
    message="$(commit_message)"
    git commit --quiet -m "$message" \
        || die "release commit failed; the version bumps are left staged"
    echo "Committed: $message"
fi

for tag in "${release_tags[@]}"; do
    git tag "$tag"
done

# Drop local tags that never reached the remote so a rerun recreates and
# pushes them. Tags already pushed stay; the release commit, if any, stays
# too so the bumps are not lost.
unpushed=("${release_tags[@]}")
push_failed() {
    git tag --delete "${unpushed[@]}" >/dev/null
    die "$1; removed the unpushed local tag(s): ${unpushed[*]}"
}

echo "Pushing '$branch' to '$remote'..."
git push "$remote" "$branch" || push_failed "pushing '$branch' failed"

# One push per tag so each one raises its own push event on GitHub.
for tag in "${release_tags[@]}"; do
    echo "Pushing $tag..."
    git push "$remote" "$tag" || push_failed "pushing $tag failed"
    unpushed=("${unpushed[@]:1}")
done

echo "Pushed. Watch the 'release' workflow in the GitHub Actions tab:"
for tag in "${release_tags[@]}"; do
    echo "  $tag"
done
