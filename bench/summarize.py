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

"""Summarize run.sh JSON lines into Markdown tables (median of the runs)."""

import json
import statistics
import sys
from collections import defaultdict


def load(paths):
    rows, errors = [], []
    for p in paths:
        for line in open(p):
            r = json.loads(line)
            (errors if "error" in r else rows).append(r)
    return rows, errors


def med(xs):
    return statistics.median(xs) if xs else float("nan")


def fmt_s(x):
    if x != x:
        return "-"
    return f"{x * 1000:.1f} ms" if x < 1 else f"{x:.2f} s"


def main(paths):
    rows, errors = load(paths)
    w = defaultdict(lambda: {"secs": [], "rss": [], "bytes": None, "n": None})
    reads = defaultdict(lambda: {"secs": [], "rss": [], "checks": set()})
    for r in rows:
        x = r["row"]
        op = x["op"]
        if op.startswith("write"):
            k = (r["label"], r["dataset"], r["format"], op, x["impl"])
            w[k]["secs"].append(x["secs"])
            w[k]["rss"].append(r["rss_kb"] / 1024)
            w[k]["bytes"] = x["check"].get("bytes")
            w[k]["n"] = x["check"].get("vertices", x["check"].get("edges"))
        else:
            k = (r["label"], r["dataset"], r["format"], op, r["graph"], x["impl"])
            reads[k]["secs"].append(x["secs"])
            reads[k]["rss"].append(r["rss_kb"] / 1024)
            reads[k]["checks"].add(json.dumps(x["check"], sort_keys=True))

    print("### Writes\n")
    print("| label | dataset | format | operation | implementation | rows | median time | peak RSS | bytes on disk |")
    print("|---|---|---|---|---|---:|---:|---:|---:|")
    for k in sorted(w):
        v = w[k]
        b = f"{v['bytes'] / 1e6:.1f} MB" if v["bytes"] else "-"
        print(f"| {k[0]} | {k[1]} | {k[2]} | {k[3]} | {k[4]} | {v['n']} | {fmt_s(med(v['secs']))} | {med(v['rss']):.0f} MB | {b} |")

    print("\n### Reads\n")
    print("| label | dataset | format | operation | files written by | native | C++ | C++ / native | native RSS | C++ RSS |")
    print("|---|---|---|---|---|---:|---:|---:|---:|---:|")
    keys = sorted({k[:5] for k in reads})
    for k in keys:
        n = reads.get(k + ("graphar-native",))
        c = reads.get(k + ("graphar-cpp",))
        ns = med(n["secs"]) if n else float("nan")
        cs = med(c["secs"]) if c else float("nan")
        ratio = f"{cs / ns:.1f}x" if n and c and ns > 0 else "-"
        nr = f"{med(n['rss']):.0f} MB" if n else "-"
        cr = f"{med(c['rss']):.0f} MB" if c else "-"
        print(f"| {k[0]} | {k[1]} | {k[2]} | {k[3]} | {k[4]} | {fmt_s(ns)} | {fmt_s(cs)} | {ratio} | {nr} | {cr} |")

    print("\n### Same answer from every reader and writer\n")
    print("| label | dataset | format | operation | readers x writers | checks agree |")
    print("|---|---|---|---|---:|---|")
    agree = defaultdict(lambda: [set(), 0])
    for k, v in reads.items():
        a = agree[k[:4]]
        a[0] |= v["checks"]
        a[1] += 1
    for k in sorted(agree):
        checks, count = agree[k]
        print(f"| {k[0]} | {k[1]} | {k[2]} | {k[3]} | {count} | {'yes' if len(checks) == 1 else 'NO: ' + ' / '.join(sorted(checks))} |")

    if errors:
        print("\n### Errors\n")
        for e in errors:
            print(f"- {e['dataset']} {e['format']} {e['graph']}: `{e.get('cmd', '')}`: {e['error'].strip()[:200]}")


if __name__ == "__main__":
    main(sys.argv[1:])
