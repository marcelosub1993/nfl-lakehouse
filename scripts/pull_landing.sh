#!/usr/bin/env bash
# Download the landing files (raw ESPN JSON) from the dev UC Volume to ./tmp/cloud,
# so the local notebooks in tmp/discoveries can test the Silver logic for free.
#
# Only NEW files are downloaded: `databricks fs cp -r` without --overwrite skips files
# that already exist locally. Uses the Files API, so no compute is started (no DBU cost).
#
# Usage (from anywhere):
#   scripts/pull_landing.sh                    # teams, scoreboard and rosters
#   scripts/pull_landing.sh scoreboard         # only one dataset
#   PROFILE=other scripts/pull_landing.sh      # another CLI profile

set -euo pipefail

PROFILE="${PROFILE:-nfl_lakehouse}"
VOLUME="${VOLUME:-dbfs:/Volumes/nfl_dev/dev_marcelo_sub1993_landing/espn_raw}"

# Repo root = parent of this script's folder, so relative paths work from any directory
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="$REPO_ROOT/tmp/cloud"

if [[ $# -gt 0 ]]; then
    DATASETS=("$@")
else
    DATASETS=(teams scoreboard rosters)
fi

count_files() {
    find "$1" -type f -name '*.json' 2>/dev/null | wc -l
}

for dataset in "${DATASETS[@]}"; do
    local_dir="$TARGET/$dataset"
    mkdir -p "$local_dir"

    before=$(count_files "$local_dir")
    echo "== $dataset: $VOLUME/$dataset -> $local_dir"

    # Hide the per-file "skipped; already exists" lines, keep everything else (errors included)
    databricks fs cp -r "$VOLUME/$dataset" "$local_dir" --profile "$PROFILE" \
        | { grep -v 'skipped; already exists' || true; }

    after=$(count_files "$local_dir")
    echo "   new files: $((after - before))   total: $after"
done
