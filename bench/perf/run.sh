#!/bin/bash
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

# graphar perf bench driver: one process per stage, so VmHWM + /proc/self/io
# are per stage. Usage: bench/perf/run.sh <out.md> [--vertices N --edges M --queries K --dir D]
# STAGES="..." overrides the stage list. Prints a markdown table (appended to <out.md>).
set -e
out=${1:?out.md}; shift
dir=/tmp/graphar-bench
for ((i=1;i<=$#;i++)); do if [ "${!i}" = "--dir" ]; then j=$((i+1)); dir=${!j}; fi; done
here=$(cd "$(dirname "$0")" && pwd)
cargo build --release --manifest-path "$here/Cargo.toml" >/dev/null
bin=${CARGO_TARGET_DIR:-$here/target}/release/graphar-perf-bench
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
