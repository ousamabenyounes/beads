#!/usr/bin/env bash
set -euo pipefail

# Fast safety checks for the public sealed-copy SQLite bridge. Authentic
# historical binaries exercise its successful data path in the upgrade corpus.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BRIDGE="$SCRIPT_DIR/../migrate-legacy-to-current.sh"
tmp=$(mktemp -d)
trap 'rm -rf -- "$tmp"' EXIT

old="$tmp/old-bd"
new="$tmp/new-bd"
printf '%s\n' \
  '#!/usr/bin/env bash' \
  'set -euo pipefail' \
  '[ -z "${BRIDGE_TEST_MARKER:-}" ] || : > "$BRIDGE_TEST_MARKER"' \
  'for arg in "$@"; do' \
  '  [ "$arg" != version ] || { printf "%s\\n" "bd version 0.49.6"; exit 0; }' \
  'done' \
  'for arg in "$@"; do' \
  '  [ "$arg" != export ] || { printf "%s\\n" "{\"id\":\"historical-1\",\"title\":\"Historical issue\",\"created_at\":\"2020-01-01T00:00:00.600000000Z\"}"; exit 0; }' \
  'done' \
  'exit 2' >"$old"
printf '%s\n' \
  '#!/usr/bin/env bash' \
  'set -euo pipefail' \
  'case "$1" in' \
  '  init)' \
  '    test -s .beads/issues.jsonl' \
  '    mkdir -p .beads/embeddeddolt/hist' \
  '    : > .beads/embeddeddolt/hist/storage' \
  '    printf "%s\\n" "{\"backend\":\"dolt\",\"dolt_mode\":\"embedded\"}" > .beads/metadata.json' \
  '    [ -z "${BRIDGE_TEST_MARKER:-}" ] || : > "$BRIDGE_TEST_MARKER"' \
  '    ;;' \
  '  export)' \
  '    [ "$2" = --all ] && [ "$3" = -o ] && [ -n "${4:-}" ] || exit 2' \
  '    printf "%s\\n" "{\"id\":\"historical-1\",\"title\":\"Historical issue\",\"created_at\":\"2020-01-01T00:00:01Z\"}" > "$4"' \
  '    ;;' \
  '  *) exit 2 ;;' \
  'esac' >"$new"
chmod +x "$old" "$new"

fingerprint() {
  (cd "$1" && find . -type f -print0 | LC_ALL=C sort -z | xargs -r -0 sha256sum) | sha256sum | awk '{print $1}'
}

run_lane() {
  local name=$1
  local source="$tmp/$name-source"
  local destination="$tmp/$name-destination"
  mkdir -p "$source/.beads"
  printf 'SQLite format 3\000' >"$source/.beads/beads.db"
  case "$name" in
    sqlite)
      printf '%s\n' '{"backend":"sqlite"}' >"$source/.beads/metadata.json"
      printf '%s\n' '{"backend":"sqlite"}' >"$source/.beads/config.json"
      ;;
    sqlite-implicit)
      printf '%s\n' '{"database":"beads.db"}' >"$source/.beads/metadata.json"
      ;;
  esac
  local before
  before=$(fingerprint "$source/.beads")
  "$BRIDGE" --source "$source" --destination "$destination" --source-version v0.49.6 \
    --old-bd "$old" --new-bd "$new" --prefix hist
  [ "$(fingerprint "$source/.beads")" = "$before" ] || {
    printf '%s bridge mutated source\n' "$name" >&2
    exit 1
  }
  test -f "$destination/sealed-source/.beads/beads.db"
  test -f "$destination/cutover/.beads/issues.jsonl"
  jq -e '.backend == "dolt" and .dolt_mode == "embedded"' "$destination/cutover/.beads/metadata.json" >/dev/null
  test -d "$destination/cutover/.beads/embeddeddolt"
  test ! -L "$destination/cutover/.beads/embeddeddolt"
  test ! -e "$destination/cutover/.beads/beads.db"
  test ! -e "$destination/cutover/.beads/dolt"
}

run_lane sqlite
run_lane sqlite-implicit
run_lane sqlite-absent

probe_mutator="$tmp/probe-mutator-bd"
printf '%s\n' \
  '#!/usr/bin/env bash' \
  'set -euo pipefail' \
  'for arg in "$@"; do' \
  '  [ "$arg" != version ] || { : > version-probe-marker; printf "%s\\n" "bd version 0.49.6"; exit 0; }' \
  'done' \
  'for arg in "$@"; do' \
  '  [ "$arg" != export ] || { printf "%s\\n" "{\"id\":\"historical-1\",\"title\":\"Historical issue\",\"created_at\":\"2020-01-01T00:00:00.600000000Z\"}"; exit 0; }' \
  'done' \
  'exit 2' >"$probe_mutator"
chmod +x "$probe_mutator"
source="$tmp/probe-source"
destination="$tmp/probe-destination"
mkdir -p "$source/.beads"
printf 'SQLite format 3\000' >"$source/.beads/beads.db"
before=$(fingerprint "$source/.beads")
(
  cd "$source"
  "$BRIDGE" --source "$source" --destination "$destination" --source-version v0.49.6 \
    --old-bd "$probe_mutator" --new-bd "$new" --prefix hist
)
[ "$(fingerprint "$source/.beads")" = "$before" ] || {
  printf 'bridge version probe mutated source\n' >&2
  exit 1
}
test ! -e "$source/version-probe-marker" || {
  printf 'bridge version probe ran from source\n' >&2
  exit 1
}
test -e "$destination/probe/version-probe-marker" || {
  printf 'bridge version probe did not run from destination/probe\n' >&2
  exit 1
}

source="$tmp/version-source"
mkdir -p "$source/.beads"
printf 'SQLite format 3\000' >"$source/.beads/beads.db"
for version in v0.9.0 v0.9.2 v0.17.1 v0.49.5 v0.50.2 v0.51.0 v1.0.0; do
  if "$BRIDGE" --source "$source" --destination "$tmp/version-$version" \
      --source-version "$version" --old-bd "$old" --new-bd "$new" --prefix hist >/dev/null 2>&1; then
    printf 'bridge accepted unqualified source version %s\n' "$version" >&2
    exit 1
  fi
done

for descriptor in metadata config; do
  for shape in postgres mysql dolt unknown provider dsn postgres-dsn mysql-dsn dolt-host outside-sqlite; do
    source="$tmp/$descriptor-$shape-source"
    destination="$tmp/$descriptor-$shape-destination"
    marker="$tmp/$descriptor-$shape-old-invoked"
    mkdir -p "$source/.beads"
    printf 'SQLite format 3\000' >"$source/.beads/beads.db"
    case "$shape" in
      postgres|mysql|dolt|unknown) payload=$(printf '{"backend":"%s"}' "$shape") ;;
      provider) payload='{"backend":"sqlite","provider":"postgres"}' ;;
      dsn) payload='{"backend":"sqlite","dsn":"postgres://example"}' ;;
      postgres-dsn) payload='{"backend":"sqlite","postgres_dsn":"postgres://example"}' ;;
      mysql-dsn) payload='{"backend":"sqlite","mysql_dsn":"mysql://example"}' ;;
      dolt-host) payload='{"backend":"sqlite","dolt_server_host":"example.invalid"}' ;;
      outside-sqlite) payload='{"backend":"sqlite","sqlite_path":"../outside.db"}' ;;
    esac
    printf '%s\n' "$payload" >"$source/.beads/$descriptor.json"
    before=$(fingerprint "$source/.beads")
    if BRIDGE_TEST_MARKER="$marker" "$BRIDGE" --source "$source" --destination "$destination" \
        --source-version v0.49.6 --old-bd "$old" --new-bd "$new" --prefix hist >/dev/null 2>&1; then
      printf 'bridge accepted %s %s storage descriptor\n' "$descriptor" "$shape" >&2
      exit 1
    fi
    test ! -e "$marker" || {
      printf 'bridge executed the old binary before rejecting %s %s\n' "$descriptor" "$shape" >&2
      exit 1
    }
    [ "$(fingerprint "$source/.beads")" = "$before" ] || {
      printf 'bridge mutated rejected %s %s source\n' "$descriptor" "$shape" >&2
      exit 1
    }
  done
done

old_v017="$tmp/old-v017-bd"
sed 's/bd version 0.49.6/bd version 0.17.0/' "$old" >"$old_v017"
chmod +x "$old_v017"
source="$tmp/canonicalizer-gate-source"
marker="$tmp/canonicalizer-gate-invoked"
mkdir -p "$source/.beads"
printf 'SQLite format 3\000' >"$source/.beads/beads.db"
printf '%s\n' '{"backend":"dolt"}' >"$source/.beads/metadata.json"
if BRIDGE_TEST_MARKER="$marker" "$BRIDGE" --source "$source" --destination "$tmp/canonicalizer-gate-destination" \
    --source-version v0.17.0 --old-bd "$old_v017" --canonicalizer-bd "$old" \
    --new-bd "$new" --prefix hist >/dev/null 2>&1; then
  printf 'bridge accepted an ambiguous pre-canonicalizer source\n' >&2
  exit 1
fi
test ! -e "$marker" || {
  printf 'bridge executed an old or canonicalizer binary before source validation\n' >&2
  exit 1
}

source="$tmp/pre-canonical-source"
mkdir -p "$source/.beads"
printf 'SQLite format 3\000' >"$source/.beads/beads.db"
if "$BRIDGE" --source "$source" --destination "$tmp/pre-canonical-destination" \
    --source-version v0.17.0 --old-bd "$old" --new-bd "$new" --prefix hist >/dev/null 2>&1; then
  printf 'bridge accepted pre-v0.49.6 source without canonicalizer\n' >&2
  exit 1
fi

source="$tmp/containment-source"
mkdir -p "$source/.beads"
printf 'SQLite format 3\000' >"$source/.beads/beads.db"
ln -s "$source" "$tmp/source-alias"
if "$BRIDGE" --source "$source" --destination "$tmp/source-alias/nested-cutover" \
    --source-version v0.49.6 --old-bd "$old" --new-bd "$new" --prefix hist >/dev/null 2>&1; then
  printf 'bridge accepted a destination that resolves inside source\n' >&2
  exit 1
fi
test ! -e "$source/nested-cutover"

bad_old="$tmp/bad-old-bd"
printf '%s\n' \
  '#!/usr/bin/env bash' \
  'set -euo pipefail' \
  'for arg in "$@"; do' \
  '  [ "$arg" != version ] || { printf "%s\\n" "bd version 0.49.6"; exit 0; }' \
  'done' \
  'for arg in "$@"; do' \
  '  [ "$arg" != export ] || { printf "%s\\n" "{\"title\":\"missing id\"}" "{\"id\":\"historical-1\",\"title\":\"Historical issue\"}"; exit 0; }' \
  'done' \
  'exit 2' >"$bad_old"
chmod +x "$bad_old"
marker="$tmp/candidate-was-invoked"
if BRIDGE_TEST_MARKER="$marker" "$BRIDGE" --source "$source" \
    --destination "$tmp/invalid-jsonl-destination" --source-version v0.49.6 --old-bd "$bad_old" \
    --new-bd "$new" --prefix hist >/dev/null 2>&1; then
  printf 'bridge accepted an invalid JSONL record\n' >&2
  exit 1
fi
test ! -e "$marker" || {
  printf 'bridge invoked the candidate before validating every JSONL record\n' >&2
  exit 1
}

lossy_new="$tmp/lossy-new-bd"
candidate_init_marker="$tmp/lossy-candidate-init-invoked"
candidate_export_marker="$tmp/lossy-candidate-export-invoked"
printf '%s\n' \
  '#!/usr/bin/env bash' \
  'set -euo pipefail' \
  'case "$1" in' \
  '  init)' \
  "    : > \"$candidate_init_marker\"" \
  '    test -s .beads/issues.jsonl' \
  '    mkdir -p .beads/embeddeddolt/hist' \
  '    : > .beads/embeddeddolt/hist/storage' \
  '    printf "%s\\n" "{\"backend\":\"dolt\",\"dolt_mode\":\"embedded\"}" > .beads/metadata.json' \
  '    ;;' \
  '  export)' \
  "    : > \"$candidate_export_marker\"" \
  '    [ "$2" = --all ] && [ "$3" = -o ] && [ -n "${4:-}" ] || exit 2' \
  '    printf "%s\\n" "{\"id\":\"historical-1\",\"title\":\"Lossy issue\",\"created_at\":\"2020-01-01T00:00:01Z\"}" > "$4"' \
  '    ;;' \
  '  *) exit 2 ;;' \
  'esac' >"$lossy_new"
chmod +x "$lossy_new"
source="$tmp/lossy-semantic-source"
destination="$tmp/lossy-semantic-destination"
mkdir -p "$source/.beads"
printf 'SQLite format 3\000' >"$source/.beads/beads.db"
before=$(fingerprint "$source/.beads")
if output=$("$BRIDGE" --source "$source" --destination "$destination" --source-version v0.49.6 \
    --old-bd "$old" --new-bd "$lossy_new" --prefix hist 2>&1); then
  printf 'bridge accepted semantically lossy candidate export\n' >&2
  exit 1
fi
grep -Fq 'candidate data does not semantically match' <<<"$output" || {
  printf 'bridge did not reject the lossy candidate export semantically:\n%s\n' "$output" >&2
  exit 1
}
test -e "$candidate_init_marker" || {
  printf 'bridge did not invoke candidate init for lossy export\n' >&2
  exit 1
}
test -e "$candidate_export_marker" || {
  printf 'bridge did not invoke candidate export for lossy export\n' >&2
  exit 1
}
[ "$(fingerprint "$source/.beads")" = "$before" ] || {
  printf 'bridge mutated source during lossy candidate verification\n' >&2
  exit 1
}

printf 'sealed-copy bridge smoke: PASS\n'
