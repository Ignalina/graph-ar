#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Runs every bench case and appends one JSON line per run to $OUT.
#   run.sh <work-dir> <label>
# <work-dir> holds data/<dataset>-input, data/<dataset>-info-<format> and bin/
# (graphar-bench, gar_bench_cpp, optional graphar-rs-bench). RUNS (default 5),
# DATASETS (default "ldbc mimir") and FORMATS (default "parquet csv orc")
# narrow it down.
set -euo pipefail
W=${1:?work dir}
LABEL=${2:?label}
RUNS=${RUNS:-5}
DATASETS=${DATASETS:-ldbc mimir}
FORMATS=${FORMATS:-parquet csv orc}
OUT=${OUT:-$W/raw/$LABEL.jsonl}
NATIVE=$W/bin/graphar-bench
CPP=$W/bin/gar_bench_cpp
GRS=$W/bin/graphar-rs-bench
mkdir -p "$(dirname "$OUT")" "$W/out"

# One run: <dataset> <format> <graph-writer> <run> <cmd...>; adds the peak RSS.
one() {
  local ds=$1 fmt=$2 writer=$3 run=$4; shift 4
  local tmp
  tmp=$(mktemp)
  if /usr/bin/time -f '%M' -o "$tmp.rss" "$@" > "$tmp" 2> "$tmp.err"; then
    local rss
    rss=$(tail -1 "$tmp.rss")
    while read -r line; do
      [ -n "$line" ] || continue
      echo "{\"label\":\"$LABEL\",\"dataset\":\"$ds\",\"format\":\"$fmt\",\"graph\":\"$writer\",\"run\":$run,\"rss_kb\":$rss,\"row\":$line}" >> "$OUT"
    done < "$tmp"
  else
    echo "{\"label\":\"$LABEL\",\"dataset\":\"$ds\",\"format\":\"$fmt\",\"graph\":\"$writer\",\"run\":$run,\"error\":$(head -c 300 "$tmp.err" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))'),\"cmd\":\"$*\"}" >> "$OUT"
  fi
  rm -f "$tmp" "$tmp.rss" "$tmp.err"
}

clean() {
  local d=${1:?}
  case "$d" in "$W"/out/*) rm -rf -- "$d" ;; *) echo "refusing to remove $d" >&2; exit 1 ;; esac
}

for ds in $DATASETS; do
  prop=name; [ "$ds" = ldbc ] && prop=firstName
  for fmt in $FORMATS; do
    info=$W/data/$ds-info-$fmt
    input=$W/data/$ds-input
    impls="native cpp"
    for impl in $impls; do
      bin=$NATIVE; [ "$impl" = cpp ] && bin=$CPP
      for api in table builder; do
        for r in $(seq 1 "$RUNS"); do
          g=$W/out/$ds-$fmt-$impl-$api
          clean "$g"
          one "$ds" "$fmt" "$impl-$api" "$r" "$bin" write "$api" "$input" "$info" "$g"
        done
      done
    done
    # The graphs the reads use: one more table write per writer, retried
    # until it succeeds (the C++ table writer fails now and then, see the
    # error rows), so a failed timed run never leaves a partial graph behind.
    for impl in $impls; do
      bin=$NATIVE; [ "$impl" = cpp ] && bin=$CPP
      g=$W/out/$ds-$fmt-$impl-table
      for try in 1 2 3 4 5; do
        clean "$g"
        if "$bin" write table "$input" "$info" "$g" > /dev/null 2>&1; then break; fi
        echo "{\"label\":\"$LABEL\",\"dataset\":\"$ds\",\"format\":\"$fmt\",\"graph\":\"$impl-table\",\"run\":0,\"error\":\"reference write failed, try $try\",\"cmd\":\"$bin write table\"}" >> "$OUT"
      done
    done
    if [ -x "$GRS" ]; then
      for r in $(seq 1 "$RUNS"); do
        g=$W/out/$ds-$fmt-graphar-rs-builder
        clean "$g"
        one "$ds" "$fmt" "graphar-rs-builder" "$r" "$GRS" write-builder "$input" "$info" "$g"
      done
    fi
    # Reads: every reader on the graphs written by each table writer.
    for writer in $impls; do
      g=$W/out/$ds-$fmt-$writer-table
      for impl in $impls; do
        bin=$NATIVE; [ "$impl" = cpp ] && bin=$CPP
        for r in $(seq 1 "$RUNS"); do
          one "$ds" "$fmt" "$writer-table" "$r" "$bin" read-prop "$g" "$prop"
          one "$ds" "$fmt" "$writer-table" "$r" "$bin" read-csr "$g"
          one "$ds" "$fmt" "$writer-table" "$r" "$bin" scan "$g"
        done
      done
    done
  done
done
# The upstream sample graph (written by the Spark library) read by both.
if [ -n "${UPSTREAM_TESTING:-}" ]; then
  for fmt in parquet csv orc; do
    g=$UPSTREAM_TESTING/ldbc_sample/$fmt
    for bin in "$NATIVE" "$CPP"; do
      for r in $(seq 1 "$RUNS"); do
        one ldbc-upstream "$fmt" spark "$r" "$bin" read-prop "$g" firstName
        one ldbc-upstream "$fmt" spark "$r" "$bin" read-csr "$g"
        one ldbc-upstream "$fmt" spark "$r" "$bin" scan "$g"
      done
    done
  done
fi
echo "wrote $OUT"
