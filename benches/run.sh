#!/bin/bash
# graphar perf bench driver: one process per stage, so VmHWM + /proc/self/io
# are per stage. Usage: benches/run.sh <out.md> [--vertices N --edges M --queries K --dir D]
# STAGES="..." overrides the stage list. Prints a markdown table (appended to <out.md>).
set -e
out=${1:?out.md}; shift
dir=/tmp/graphar-bench
for ((i=1;i<=$#;i++)); do if [ "${!i}" = "--dir" ]; then j=$((i+1)); dir=${!j}; fi; done
cargo build --release --bench graphar_bench >/dev/null
bin=$(ls -t "${CARGO_TARGET_DIR:-target}"/release/deps/graphar_bench-* | grep -v '\.d$' | head -1)
stages=${STAGES:-"gen write-vertices-table write-edges-table write-edges-floor read-vertices-serial read-vertices read-prop read-adj read-adj-keep read-adj-par read-adj-bytes read-adj-mmap read-offsets neighbors neighbors-pushdown lookups stree checksum validate"}
{
  echo "host: $(hostname) $(nproc) cores, $(date -u +%Y-%m-%dT%H:%MZ), args: $*"
  echo "| stage | wall | peak RSS | rchar | read_bytes | count |"
  echo "|---|---|---|---|---|---|"
  rm -rf "$dir"
  for s in $stages; do
    "$bin" --stage "$s" "$@" | sed -n 's/^ROW //p' | awk -F'\t' '{printf "| %s |", $1; for(i=2;i<=NF;i++) printf " %s |", $i; print ""}'
  done
} | tee -a "$out"
