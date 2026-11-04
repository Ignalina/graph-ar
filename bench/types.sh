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

# Every value type between the implementations: each table writer (and the
# native row builder) writes the types dataset and the upstream with_feature
# graph; each reader scans the result. All `check` objects of one dataset and
# format must be equal.
#   types.sh <work-dir> <upstream-testing-dir>
set -uo pipefail
W=${1:?work dir}
T=${2:?testing dir}
NATIVE=$W/bin/graphar-bench
CPP=$W/bin/gar_bench_cpp
D=$W/data/types
rm -rf -- "$W/out/types"
mkdir -p "$D" "$W/out/types"
"$NATIVE" prep-types "$D/lists-input" lists
"$NATIVE" prep-types "$D/scalars-input" scalars
mkdir -p "$D/feature-graph"
cp -r "$T"/ldbc_sample/parquet/. "$D/feature-graph/"
rm -f "$D"/feature-graph/ldbc_sample.graph.yml "$D"/feature-graph/ldbc_sample_date.graph.yml "$D"/feature-graph/ldbc_sample_timestamp.graph.yml
"$NATIVE" prep-gar "$D/feature-input" "$D/feature-graph/ldbc_sample_with_feature.graph.yml" person person knows person

run() { # label writer-bin api input info
  local g=$W/out/types/$1
  local why
  if why=$("$2" write "$3" "$4" "$5" "$g" 2>&1 >/dev/null); then
    for r in "$NATIVE" "$CPP"; do
      printf '%s reader=%s ' "$1" "$(basename "$r")"
      "$r" scan "$g" 2>&1 | tail -1 | cut -c1-220
    done
  else
    echo "$1 WRITE-FAILED: ${why:0:200}"
  fi
}

for ds in lists scalars feature; do
  for fmt in parquet orc csv json; do
    case "$ds-$fmt" in lists-csv|feature-csv) continue ;; esac
    in=$D/$ds-input
    info=$D/$ds-info-$fmt
    "$NATIVE" info "$in" "$fmt" "$info"
    run "$ds-$fmt-native-table" "$NATIVE" table "$in" "$info"
    run "$ds-$fmt-native-builder" "$NATIVE" builder "$in" "$info"
    run "$ds-$fmt-cpp-table" "$CPP" table "$in" "$info"
  done
done
