#!/usr/bin/env bash
#
# Print the GitHub release notes for one crate's release tag, as Markdown.
#
# The release workflow runs this when a `<crate>-<version>` tag is pushed and
# posts the output as the body of the GitHub release. Nothing is kept in the
# tree: the notes are derived from the commits between the crate's previous
# release tag and this one that touched the crate's directory or the root
# Cargo.toml, which are the same paths release.sh uses to decide that a crate
# changed. Commits are grouped by their conventional-commit type.
#
# Usage:
#   ./release-notes.sh <crate>-<version> [<ref>]
#
# <ref> is the commit the release is cut from and defaults to the tag itself.
# Pass a ref (release.sh --dry-run passes HEAD) to preview the notes for a
# tag that does not exist yet.

set -euo pipefail

[ "$#" -ge 1 ] && [ "$#" -le 2 ] || {
    sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//' >&2
    exit 2
}
tag="$1"
ref="${2:-$tag}"

die() {
    echo "error: $*" >&2
    exit 1
}

[[ "$tag" =~ ^(.+)-([0-9]+\.[0-9]+\.[0-9]+.*)$ ]] \
    || die "'$tag' is not a <crate>-<version> release tag"
crate="${BASH_REMATCH[1]}"
version="${BASH_REMATCH[2]}"

cd "$(git rev-parse --show-toplevel)"

# The crate directory and repository URL come from the manifest, so the
# notes follow the crate if it is ever moved.
# `read` fails on empty input, which here means the crate is unknown.
read -r dir repo < <(
    cargo metadata --no-deps --format-version 1 \
        | jq -r --arg crate "$crate" '.packages[] | select(.name == $crate)
            | "\(.manifest_path | rtrimstr("/Cargo.toml")) \(.repository // "")"'
) || die "'$crate' is not a workspace member"
dir="${dir#"$PWD"/}"

git rev-parse --quiet --verify "$ref^{commit}" >/dev/null \
    || die "'$ref' is not a commit"

# The previous release is the highest version tagged for this crate below
# this one. The first release has none and lists the crate's whole history.
previous="$(
    { git tag --list "$crate-[0-9]*"; echo "$tag"; } \
        | sort -u | sort -V | sed -n "/^$tag\$/q;p" | tail -n 1
)"
if [ -n "$previous" ]; then
    range="$previous..$ref"
else
    range="$ref"
fi

# One bullet per commit, sorted into a section by conventional-commit type.
# A `!` after the type or a `BREAKING CHANGE:` footer puts the commit under
# breaking changes whatever its type.
breaking=() added=() fixed=() perf=() docs=() other=()
while IFS=$'\x1f' read -r -d '' hash subject body; do
    short="${hash:0:7}"
    if [ -n "$repo" ]; then
        link="[$short]($repo/commit/$hash)"
    else
        link="$short"
    fi

    if [[ "$subject" =~ ^([a-z]+)(\(([a-z0-9_-]+)\))?(!)?:\ (.*)$ ]]; then
        type="${BASH_REMATCH[1]}"
        scope="${BASH_REMATCH[3]}"
        bang="${BASH_REMATCH[4]}"
        text="${BASH_REMATCH[5]}"
    else
        type="" scope="" bang="" text="$subject"
    fi
    [ -z "$scope" ] || text="$scope: $text"
    bullet="- $text ($link)"

    if [ -n "$bang" ] || grep -q '^BREAKING CHANGE:' <<<"$body"; then
        breaking+=("$bullet")
        continue
    fi
    case "$type" in
        feat) added+=("$bullet") ;;
        fix)  fixed+=("$bullet") ;;
        perf) perf+=("$bullet") ;;
        docs) docs+=("$bullet") ;;
        *)    other+=("$bullet") ;;
    esac
done < <(
    git log -z --no-merges --invert-grep --grep='^chore: release ' \
        --format='%H%x1f%s%x1f%b' "$range" -- "$dir" Cargo.toml
)

section() {
    local title="$1"
    shift
    [ "$#" -gt 0 ] || return 0
    echo
    echo "### $title"
    echo
    printf '%s\n' "$@"
}

if [ -n "$previous" ]; then
    echo "Changes to \`$dir/\` since [$previous]($repo/releases/tag/$previous)."
else
    echo "First release of \`$crate\`."
fi

total=$(( ${#breaking[@]} + ${#added[@]} + ${#fixed[@]} + ${#perf[@]} + ${#docs[@]} + ${#other[@]} ))
if [ "$total" -eq 0 ]; then
    echo
    echo "No commits touched the crate; this release only changes its version."
fi

section "Breaking changes" ${breaking[@]+"${breaking[@]}"}
section "Added" ${added[@]+"${added[@]}"}
section "Fixed" ${fixed[@]+"${fixed[@]}"}
section "Performance" ${perf[@]+"${perf[@]}"}
section "Documentation" ${docs[@]+"${docs[@]}"}
section "Other" ${other[@]+"${other[@]}"}

if [ -n "$previous" ] && [ -n "$repo" ]; then
    echo
    echo "**Full diff:** $repo/compare/$previous...$tag"
fi
