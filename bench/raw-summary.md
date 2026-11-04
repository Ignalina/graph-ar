<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Raw summary

Output of `summarize.py`: the median of each case, from the raw runs of the three labels. `before-cd86afc` and `after-d85f31f` ran on the first host, `build1-cd4e15e` on the second; see BENCH.md.

## before-cd86afc

### Writes

| label | dataset | format | operation | implementation | rows | median time | peak RSS | bytes on disk |
|---|---|---|---|---|---:|---:|---:|---:|
| before-cd86afc | ldbc | arrow | write-edges-builder | graphar-native | 6626 | 6.4 ms | 17 MB | 0.3 MB |
| before-cd86afc | ldbc | arrow | write-edges-table | graphar-native | 6626 | 2.0 ms | 13 MB | 0.3 MB |
| before-cd86afc | ldbc | arrow | write-vertices-builder | graphar-native | 903 | 1.7 ms | 17 MB | 0.1 MB |
| before-cd86afc | ldbc | arrow | write-vertices-table | graphar-native | 903 | 0.9 ms | 13 MB | 0.1 MB |
| before-cd86afc | ldbc | csv | write-edges-builder | graphar-cpp | 6626 | 8.0 ms | 40 MB | 0.3 MB |
| before-cd86afc | ldbc | csv | write-edges-builder | graphar-native | 6626 | 7.7 ms | 18 MB | 0.2 MB |
| before-cd86afc | ldbc | csv | write-edges-table | graphar-cpp | 6626 | 17.6 ms | 43 MB | 0.3 MB |
| before-cd86afc | ldbc | csv | write-edges-table | graphar-native | 6626 | 2.4 ms | 13 MB | 0.2 MB |
| before-cd86afc | ldbc | csv | write-vertices-builder | graphar-cpp | 903 | 2.7 ms | 40 MB | 0.0 MB |
| before-cd86afc | ldbc | csv | write-vertices-builder | graphar-native | 903 | 1.6 ms | 18 MB | 0.0 MB |
| before-cd86afc | ldbc | csv | write-vertices-builder | graphar-rs | 903 | 2.9 ms | 34 MB | - |
| before-cd86afc | ldbc | csv | write-vertices-table | graphar-cpp | 903 | 1.4 ms | 43 MB | 0.0 MB |
| before-cd86afc | ldbc | csv | write-vertices-table | graphar-native | 903 | 0.9 ms | 13 MB | 0.0 MB |
| before-cd86afc | ldbc | orc | write-edges-builder | graphar-cpp | 6626 | 12.0 ms | 46 MB | 0.1 MB |
| before-cd86afc | ldbc | orc | write-edges-builder | graphar-native | 6626 | 9.8 ms | 18 MB | 0.2 MB |
| before-cd86afc | ldbc | orc | write-edges-table | graphar-cpp | 6626 | 36.5 ms | 49 MB | 0.1 MB |
| before-cd86afc | ldbc | orc | write-edges-table | graphar-native | 6626 | 2.0 ms | 13 MB | 0.2 MB |
| before-cd86afc | ldbc | orc | write-vertices-builder | graphar-cpp | 903 | 12.7 ms | 46 MB | 0.0 MB |
| before-cd86afc | ldbc | orc | write-vertices-builder | graphar-native | 903 | 1.8 ms | 18 MB | 0.0 MB |
| before-cd86afc | ldbc | orc | write-vertices-builder | graphar-rs | 903 | 9.0 ms | 36 MB | - |
| before-cd86afc | ldbc | orc | write-vertices-table | graphar-cpp | 903 | 12.3 ms | 49 MB | 0.0 MB |
| before-cd86afc | ldbc | orc | write-vertices-table | graphar-native | 903 | 0.9 ms | 13 MB | 0.0 MB |
| before-cd86afc | ldbc | parquet | write-edges-builder | graphar-cpp | 6626 | 16.5 ms | 39 MB | 0.1 MB |
| before-cd86afc | ldbc | parquet | write-edges-builder | graphar-native | 6626 | 10.4 ms | 18 MB | 0.1 MB |
| before-cd86afc | ldbc | parquet | write-edges-table | graphar-cpp | 6626 | 25.3 ms | 42 MB | 0.1 MB |
| before-cd86afc | ldbc | parquet | write-edges-table | graphar-native | 6626 | 5.0 ms | 14 MB | 0.1 MB |
| before-cd86afc | ldbc | parquet | write-vertices-builder | graphar-cpp | 903 | 6.7 ms | 39 MB | 0.0 MB |
| before-cd86afc | ldbc | parquet | write-vertices-builder | graphar-native | 903 | 4.1 ms | 18 MB | 0.0 MB |
| before-cd86afc | ldbc | parquet | write-vertices-builder | graphar-rs | 903 | 5.5 ms | 33 MB | - |
| before-cd86afc | ldbc | parquet | write-vertices-table | graphar-cpp | 903 | 4.4 ms | 42 MB | 0.0 MB |
| before-cd86afc | ldbc | parquet | write-vertices-table | graphar-native | 903 | 2.3 ms | 14 MB | 0.0 MB |
| before-cd86afc | mimir | arrow | write-edges-builder | graphar-native | 4566534 | 5.64 s | 4676 MB | 190.8 MB |
| before-cd86afc | mimir | arrow | write-edges-table | graphar-native | 4566534 | 421.1 ms | 1046 MB | 190.8 MB |
| before-cd86afc | mimir | arrow | write-vertices-builder | graphar-native | 3421126 | 6.93 s | 4676 MB | 622.5 MB |
| before-cd86afc | mimir | arrow | write-vertices-table | graphar-native | 3421126 | 114.1 ms | 1046 MB | 622.5 MB |
| before-cd86afc | mimir | csv | write-edges-builder | graphar-cpp | 4566534 | 7.55 s | 4673 MB | 151.1 MB |
| before-cd86afc | mimir | csv | write-edges-builder | graphar-native | 4566534 | 6.63 s | 4677 MB | 140.5 MB |
| before-cd86afc | mimir | csv | write-edges-table | graphar-cpp | 4566534 | 1.29 s | 1188 MB | 151.1 MB |
| before-cd86afc | mimir | csv | write-edges-table | graphar-native | 4566534 | 1.29 s | 1046 MB | 140.5 MB |
| before-cd86afc | mimir | csv | write-vertices-builder | graphar-cpp | 3421126 | 7.69 s | 4673 MB | 582.7 MB |
| before-cd86afc | mimir | csv | write-vertices-builder | graphar-native | 3421126 | 8.60 s | 4677 MB | 555.3 MB |
| before-cd86afc | mimir | csv | write-vertices-builder | graphar-rs | 3421126 | 7.82 s | 4355 MB | - |
| before-cd86afc | mimir | csv | write-vertices-table | graphar-cpp | 3421126 | 726.1 ms | 1188 MB | 582.7 MB |
| before-cd86afc | mimir | csv | write-vertices-table | graphar-native | 3421126 | 1.43 s | 1046 MB | 555.3 MB |
| before-cd86afc | mimir | orc | write-edges-builder | graphar-cpp | 4566534 | 7.66 s | 4673 MB | 15.8 MB |
| before-cd86afc | mimir | orc | write-edges-builder | graphar-native | 4566534 | 5.11 s | 4676 MB | 55.4 MB |
| before-cd86afc | mimir | orc | write-edges-table | graphar-cpp | 4566534 | 1.30 s | 1234 MB | 14.8 MB |
| before-cd86afc | mimir | orc | write-edges-table | graphar-native | 4566534 | 620.8 ms | 1045 MB | 55.4 MB |
| before-cd86afc | mimir | orc | write-vertices-builder | graphar-cpp | 3421126 | 8.02 s | 4673 MB | 16.0 MB |
| before-cd86afc | mimir | orc | write-vertices-builder | graphar-native | 3421126 | 6.43 s | 4676 MB | 461.1 MB |
| before-cd86afc | mimir | orc | write-vertices-builder | graphar-rs | 3421126 | 8.91 s | 4355 MB | - |
| before-cd86afc | mimir | orc | write-vertices-table | graphar-cpp | 3421126 | 1.05 s | 1234 MB | 16.0 MB |
| before-cd86afc | mimir | orc | write-vertices-table | graphar-native | 3421126 | 321.1 ms | 1045 MB | 461.1 MB |
| before-cd86afc | mimir | parquet | write-edges-builder | graphar-cpp | 4566534 | 8.78 s | 4673 MB | 25.0 MB |
| before-cd86afc | mimir | parquet | write-edges-builder | graphar-native | 4566534 | 6.39 s | 4677 MB | 24.5 MB |
| before-cd86afc | mimir | parquet | write-edges-table | graphar-cpp | 4566534 | 1.61 s | 1202 MB | 24.6 MB |
| before-cd86afc | mimir | parquet | write-edges-table | graphar-native | 4566534 | 1.04 s | 1046 MB | 24.5 MB |
| before-cd86afc | mimir | parquet | write-vertices-builder | graphar-cpp | 3421126 | 8.96 s | 4673 MB | 35.9 MB |
| before-cd86afc | mimir | parquet | write-vertices-builder | graphar-native | 3421126 | 8.01 s | 4677 MB | 35.8 MB |
| before-cd86afc | mimir | parquet | write-vertices-builder | graphar-rs | 3421126 | 8.55 s | 4356 MB | - |
| before-cd86afc | mimir | parquet | write-vertices-table | graphar-cpp | 3421126 | 1.38 s | 1202 MB | 35.9 MB |
| before-cd86afc | mimir | parquet | write-vertices-table | graphar-native | 3421126 | 1.05 s | 1046 MB | 35.8 MB |

### Reads

| label | dataset | format | operation | files written by | native | C++ | C++ / native | native RSS | C++ RSS |
|---|---|---|---|---|---:|---:|---:|---:|---:|
| before-cd86afc | ldbc | arrow | read-csr | native-table | 0.4 ms | - | - | 9 MB | - |
| before-cd86afc | ldbc | arrow | read-prop | native-table | 0.3 ms | - | - | 9 MB | - |
| before-cd86afc | ldbc | arrow | scan | native-table | 0.8 ms | - | - | 9 MB | - |
| before-cd86afc | ldbc | csv | read-csr | cpp-table | 5.2 ms | 20.6 ms | 4.0x | 11 MB | 46 MB |
| before-cd86afc | ldbc | csv | read-csr | native-table | 5.2 ms | 25.6 ms | 4.9x | 11 MB | 47 MB |
| before-cd86afc | ldbc | csv | read-prop | cpp-table | 4.6 ms | 12.7 ms | 2.8x | 11 MB | 46 MB |
| before-cd86afc | ldbc | csv | read-prop | native-table | 4.6 ms | 16.4 ms | 3.6x | 11 MB | 46 MB |
| before-cd86afc | ldbc | csv | scan | cpp-table | 10.6 ms | 58.0 ms | 5.5x | 11 MB | 56 MB |
| before-cd86afc | ldbc | csv | scan | native-table | 8.3 ms | 50.3 ms | 6.1x | 11 MB | 54 MB |
| before-cd86afc | ldbc | orc | read-csr | cpp-table | 1.0 ms | 13.3 ms | 12.8x | 10 MB | 42 MB |
| before-cd86afc | ldbc | orc | read-csr | native-table | 0.7 ms | 13.1 ms | 19.4x | 9 MB | 41 MB |
| before-cd86afc | ldbc | orc | read-prop | cpp-table | 0.9 ms | 10.0 ms | 11.0x | 10 MB | 43 MB |
| before-cd86afc | ldbc | orc | read-prop | native-table | 0.5 ms | 9.3 ms | 18.0x | 9 MB | 43 MB |
| before-cd86afc | ldbc | orc | scan | cpp-table | 3.1 ms | 24.4 ms | 7.9x | 10 MB | 45 MB |
| before-cd86afc | ldbc | orc | scan | native-table | 1.7 ms | 18.7 ms | 10.8x | 9 MB | 43 MB |
| before-cd86afc | ldbc | parquet | read-csr | cpp-table | 1.1 ms | 13.4 ms | 11.7x | 11 MB | 42 MB |
| before-cd86afc | ldbc | parquet | read-csr | native-table | 1.2 ms | 14.2 ms | 11.6x | 11 MB | 42 MB |
| before-cd86afc | ldbc | parquet | read-prop | cpp-table | 1.0 ms | 6.1 ms | 5.8x | 11 MB | 42 MB |
| before-cd86afc | ldbc | parquet | read-prop | native-table | 1.1 ms | 5.7 ms | 5.0x | 11 MB | 42 MB |
| before-cd86afc | ldbc | parquet | scan | cpp-table | 2.9 ms | 15.7 ms | 5.3x | 11 MB | 43 MB |
| before-cd86afc | ldbc | parquet | scan | native-table | 3.5 ms | 21.2 ms | 6.0x | 12 MB | 43 MB |
| before-cd86afc | ldbc-upstream | csv | read-csr | spark | 4.8 ms | 24.4 ms | 5.1x | 11 MB | 48 MB |
| before-cd86afc | ldbc-upstream | csv | read-prop | spark | 4.2 ms | 17.3 ms | 4.1x | 11 MB | 45 MB |
| before-cd86afc | ldbc-upstream | csv | scan | spark | 12.1 ms | 85.4 ms | 7.1x | 11 MB | 60 MB |
| before-cd86afc | ldbc-upstream | orc | read-csr | spark | 1.3 ms | 14.9 ms | 11.4x | 10 MB | 43 MB |
| before-cd86afc | ldbc-upstream | orc | read-prop | spark | 1.1 ms | 11.1 ms | 10.2x | 11 MB | 43 MB |
| before-cd86afc | ldbc-upstream | orc | scan | spark | 3.4 ms | 24.1 ms | 7.0x | 11 MB | 45 MB |
| before-cd86afc | ldbc-upstream | parquet | read-csr | spark | 1.2 ms | 13.9 ms | 11.7x | 11 MB | 42 MB |
| before-cd86afc | ldbc-upstream | parquet | read-prop | spark | 1.1 ms | 8.2 ms | 7.2x | 11 MB | 43 MB |
| before-cd86afc | ldbc-upstream | parquet | scan | spark | 3.2 ms | 20.9 ms | 6.6x | 12 MB | 44 MB |
| before-cd86afc | mimir | arrow | read-csr | native-table | 83.1 ms | - | - | 81 MB | - |
| before-cd86afc | mimir | arrow | read-prop | native-table | 103.0 ms | - | - | 160 MB | - |
| before-cd86afc | mimir | arrow | scan | native-table | 442.7 ms | - | - | 399 MB | - |
| before-cd86afc | mimir | csv | read-csr | cpp-table | 438.8 ms | 512.6 ms | 1.2x | 82 MB | 125 MB |
| before-cd86afc | mimir | csv | read-csr | native-table | 440.5 ms | 506.5 ms | 1.1x | 82 MB | 125 MB |
| before-cd86afc | mimir | csv | read-prop | cpp-table | 402.8 ms | 533.6 ms | 1.3x | 196 MB | 134 MB |
| before-cd86afc | mimir | csv | read-prop | native-table | 363.4 ms | 513.9 ms | 1.4x | 196 MB | 141 MB |
| before-cd86afc | mimir | csv | scan | cpp-table | 1.98 s | - | - | 442 MB | - |
| before-cd86afc | mimir | csv | scan | native-table | 1.87 s | - | - | 442 MB | - |
| before-cd86afc | mimir | orc | read-csr | cpp-table | 202.6 ms | 155.8 ms | 0.8x | 82 MB | 137 MB |
| before-cd86afc | mimir | orc | read-csr | native-table | 185.5 ms | 144.9 ms | 0.8x | 81 MB | 136 MB |
| before-cd86afc | mimir | orc | read-prop | cpp-table | 184.0 ms | 460.5 ms | 2.5x | 168 MB | 105 MB |
| before-cd86afc | mimir | orc | read-prop | native-table | 191.8 ms | 406.0 ms | 2.1x | 185 MB | 105 MB |
| before-cd86afc | mimir | orc | scan | cpp-table | 769.5 ms | 1.18 s | 1.5x | 407 MB | 213 MB |
| before-cd86afc | mimir | orc | scan | native-table | 962.9 ms | 1.09 s | 1.1x | 491 MB | 209 MB |
| before-cd86afc | mimir | parquet | read-csr | cpp-table | 172.3 ms | 205.3 ms | 1.2x | 83 MB | 144 MB |
| before-cd86afc | mimir | parquet | read-csr | native-table | 167.7 ms | 201.9 ms | 1.2x | 83 MB | 142 MB |
| before-cd86afc | mimir | parquet | read-prop | cpp-table | 163.8 ms | 345.8 ms | 2.1x | 180 MB | 99 MB |
| before-cd86afc | mimir | parquet | read-prop | native-table | 158.9 ms | 369.5 ms | 2.3x | 180 MB | 91 MB |
| before-cd86afc | mimir | parquet | scan | cpp-table | 719.4 ms | 1.04 s | 1.4x | 503 MB | 197 MB |
| before-cd86afc | mimir | parquet | scan | native-table | 732.3 ms | 1.04 s | 1.4x | 504 MB | 195 MB |

### Same answer from every reader and writer

| label | dataset | format | operation | readers x writers | checks agree |
|---|---|---|---|---:|---|
| before-cd86afc | ldbc | arrow | read-csr | 1 | yes |
| before-cd86afc | ldbc | arrow | read-prop | 1 | yes |
| before-cd86afc | ldbc | arrow | scan | 1 | yes |
| before-cd86afc | ldbc | csv | read-csr | 4 | yes |
| before-cd86afc | ldbc | csv | read-prop | 4 | yes |
| before-cd86afc | ldbc | csv | scan | 4 | NO: {"cells": 10238, "edges": 6626, "sum": 15074304420375848, "vertex_rows": 1806} / {"cells": 10238, "edges": 6626, "sum": 15074304420389100, "vertex_rows": 1806} |
| before-cd86afc | ldbc | orc | read-csr | 4 | yes |
| before-cd86afc | ldbc | orc | read-prop | 4 | yes |
| before-cd86afc | ldbc | orc | scan | 4 | yes |
| before-cd86afc | ldbc | parquet | read-csr | 4 | yes |
| before-cd86afc | ldbc | parquet | read-prop | 4 | yes |
| before-cd86afc | ldbc | parquet | scan | 4 | yes |
| before-cd86afc | ldbc-upstream | csv | read-csr | 2 | yes |
| before-cd86afc | ldbc-upstream | csv | read-prop | 2 | yes |
| before-cd86afc | ldbc-upstream | csv | scan | 2 | NO: {"cells": 10238, "edges": 6626, "sum": 15074304420375848, "vertex_rows": 1806} / {"cells": 10238, "edges": 6626, "sum": 15074304420389100, "vertex_rows": 1806} |
| before-cd86afc | ldbc-upstream | orc | read-csr | 2 | yes |
| before-cd86afc | ldbc-upstream | orc | read-prop | 2 | yes |
| before-cd86afc | ldbc-upstream | orc | scan | 2 | yes |
| before-cd86afc | ldbc-upstream | parquet | read-csr | 2 | yes |
| before-cd86afc | ldbc-upstream | parquet | read-prop | 2 | yes |
| before-cd86afc | ldbc-upstream | parquet | scan | 2 | yes |
| before-cd86afc | mimir | arrow | read-csr | 1 | yes |
| before-cd86afc | mimir | arrow | read-prop | 1 | yes |
| before-cd86afc | mimir | arrow | scan | 1 | yes |
| before-cd86afc | mimir | csv | read-csr | 4 | yes |
| before-cd86afc | mimir | csv | read-prop | 4 | yes |
| before-cd86afc | mimir | csv | scan | 2 | yes |
| before-cd86afc | mimir | orc | read-csr | 4 | yes |
| before-cd86afc | mimir | orc | read-prop | 4 | yes |
| before-cd86afc | mimir | orc | scan | 4 | yes |
| before-cd86afc | mimir | parquet | read-csr | 4 | yes |
| before-cd86afc | mimir | parquet | read-prop | 4 | yes |
| before-cd86afc | mimir | parquet | scan | 4 | yes |

### Errors

- ldbc csv cpp-table: 247: Cancelled: Plan was cancelled early.
- mimir parquet cpp-table: 247: Cancelled: Plan was cancelled early.
- mimir parquet cpp-table: 245: Cancelled: Plan was cancelled early.
- mimir csv native-table: 374: Invalid: In CSV column #1: Row #158249: CSV conversion error to null: invalid value '1'
- mimir csv native-table: 374: Invalid: In CSV column #1: Row #158249: CSV conversion error to null: invalid value '1'
- mimir csv native-table: 374: Invalid: In CSV column #1: Row #158249: CSV conversion error to null: invalid value '1'
- mimir csv native-table: 374: Invalid: In CSV column #1: Row #158249: CSV conversion error to null: invalid value '1'
- mimir csv native-table: 374: Invalid: In CSV column #1: Row #158249: CSV conversion error to null: invalid value '1'
- mimir csv cpp-table: 374: Invalid: In CSV column #1: Row #158249: CSV conversion error to null: invalid value '1'
- mimir csv cpp-table: 374: Invalid: In CSV column #1: Row #158249: CSV conversion error to null: invalid value '1'
- mimir csv cpp-table: 374: Invalid: In CSV column #1: Row #158249: CSV conversion error to null: invalid value '1'
- mimir csv cpp-table: 374: Invalid: In CSV column #1: Row #158249: CSV conversion error to null: invalid value '1'
- mimir csv cpp-table: 374: Invalid: In CSV column #1: Row #158249: CSV conversion error to null: invalid value '1'
- mimir orc cpp-table: reference write failed, try 1

## after-d85f31f

### Writes

| label | dataset | format | operation | implementation | rows | median time | peak RSS | bytes on disk |
|---|---|---|---|---|---:|---:|---:|---:|
| after-d85f31f | ldbc | arrow | write-edges-builder | graphar-native | 6626 | 4.2 ms | 15 MB | 0.3 MB |
| after-d85f31f | ldbc | arrow | write-edges-table | graphar-native | 6626 | 2.0 ms | 13 MB | 0.3 MB |
| after-d85f31f | ldbc | arrow | write-vertices-builder | graphar-native | 903 | 1.4 ms | 15 MB | 0.1 MB |
| after-d85f31f | ldbc | arrow | write-vertices-table | graphar-native | 903 | 1.0 ms | 13 MB | 0.1 MB |
| after-d85f31f | ldbc | csv | write-edges-builder | graphar-cpp | 6626 | 7.0 ms | 39 MB | 0.3 MB |
| after-d85f31f | ldbc | csv | write-edges-builder | graphar-native | 6626 | 4.4 ms | 15 MB | 0.2 MB |
| after-d85f31f | ldbc | csv | write-edges-table | graphar-cpp | 6626 | 12.3 ms | 44 MB | 0.3 MB |
| after-d85f31f | ldbc | csv | write-edges-table | graphar-native | 6626 | 2.7 ms | 13 MB | 0.2 MB |
| after-d85f31f | ldbc | csv | write-vertices-builder | graphar-cpp | 903 | 2.7 ms | 39 MB | 0.0 MB |
| after-d85f31f | ldbc | csv | write-vertices-builder | graphar-native | 903 | 1.3 ms | 15 MB | 0.0 MB |
| after-d85f31f | ldbc | csv | write-vertices-builder | graphar-rs | 903 | 2.8 ms | 34 MB | - |
| after-d85f31f | ldbc | csv | write-vertices-table | graphar-cpp | 903 | 1.4 ms | 44 MB | 0.0 MB |
| after-d85f31f | ldbc | csv | write-vertices-table | graphar-native | 903 | 0.9 ms | 13 MB | 0.0 MB |
| after-d85f31f | ldbc | orc | write-edges-builder | graphar-cpp | 6626 | 11.6 ms | 46 MB | 0.1 MB |
| after-d85f31f | ldbc | orc | write-edges-builder | graphar-native | 6626 | 5.1 ms | 16 MB | 0.2 MB |
| after-d85f31f | ldbc | orc | write-edges-table | graphar-cpp | 6626 | 44.1 ms | 49 MB | 0.1 MB |
| after-d85f31f | ldbc | orc | write-edges-table | graphar-native | 6626 | 2.1 ms | 13 MB | 0.2 MB |
| after-d85f31f | ldbc | orc | write-vertices-builder | graphar-cpp | 903 | 9.9 ms | 46 MB | 0.0 MB |
| after-d85f31f | ldbc | orc | write-vertices-builder | graphar-native | 903 | 1.5 ms | 16 MB | 0.0 MB |
| after-d85f31f | ldbc | orc | write-vertices-builder | graphar-rs | 903 | 11.8 ms | 36 MB | - |
| after-d85f31f | ldbc | orc | write-vertices-table | graphar-cpp | 903 | 9.1 ms | 49 MB | 0.0 MB |
| after-d85f31f | ldbc | orc | write-vertices-table | graphar-native | 903 | 0.9 ms | 13 MB | 0.0 MB |
| after-d85f31f | ldbc | parquet | write-edges-builder | graphar-cpp | 6626 | 12.1 ms | 39 MB | 0.1 MB |
| after-d85f31f | ldbc | parquet | write-edges-builder | graphar-native | 6626 | 6.2 ms | 17 MB | 0.1 MB |
| after-d85f31f | ldbc | parquet | write-edges-table | graphar-cpp | 6626 | 13.7 ms | 43 MB | 0.1 MB |
| after-d85f31f | ldbc | parquet | write-edges-table | graphar-native | 6626 | 4.2 ms | 14 MB | 0.1 MB |
| after-d85f31f | ldbc | parquet | write-vertices-builder | graphar-cpp | 903 | 5.6 ms | 39 MB | 0.0 MB |
| after-d85f31f | ldbc | parquet | write-vertices-builder | graphar-native | 903 | 2.9 ms | 17 MB | 0.0 MB |
| after-d85f31f | ldbc | parquet | write-vertices-builder | graphar-rs | 903 | 4.7 ms | 34 MB | - |
| after-d85f31f | ldbc | parquet | write-vertices-table | graphar-cpp | 903 | 5.2 ms | 43 MB | 0.0 MB |
| after-d85f31f | ldbc | parquet | write-vertices-table | graphar-native | 903 | 2.5 ms | 14 MB | 0.0 MB |
| after-d85f31f | mimir | arrow | write-edges-builder | graphar-native | 4566534 | 2.29 s | 2890 MB | 190.8 MB |
| after-d85f31f | mimir | arrow | write-edges-table | graphar-native | 4566534 | 178.1 ms | 1046 MB | 190.8 MB |
| after-d85f31f | mimir | arrow | write-vertices-builder | graphar-native | 3421126 | 3.25 s | 2890 MB | 622.5 MB |
| after-d85f31f | mimir | arrow | write-vertices-table | graphar-native | 3421126 | 114.6 ms | 1046 MB | 622.5 MB |
| after-d85f31f | mimir | csv | write-edges-builder | graphar-cpp | 4566534 | 7.16 s | 4673 MB | 151.1 MB |
| after-d85f31f | mimir | csv | write-edges-builder | graphar-native | 4566534 | 2.96 s | 2890 MB | 140.5 MB |
| after-d85f31f | mimir | csv | write-edges-table | graphar-cpp | 4566534 | 1.17 s | 1187 MB | 151.1 MB |
| after-d85f31f | mimir | csv | write-edges-table | graphar-native | 4566534 | 1.02 s | 1046 MB | 140.5 MB |
| after-d85f31f | mimir | csv | write-vertices-builder | graphar-cpp | 3421126 | 7.35 s | 4673 MB | 582.7 MB |
| after-d85f31f | mimir | csv | write-vertices-builder | graphar-native | 3421126 | 4.28 s | 2890 MB | 555.3 MB |
| after-d85f31f | mimir | csv | write-vertices-builder | graphar-rs | 3421126 | 7.63 s | 4355 MB | - |
| after-d85f31f | mimir | csv | write-vertices-table | graphar-cpp | 3421126 | 657.6 ms | 1187 MB | 582.7 MB |
| after-d85f31f | mimir | csv | write-vertices-table | graphar-native | 3421126 | 1.39 s | 1046 MB | 555.3 MB |
| after-d85f31f | mimir | orc | write-edges-builder | graphar-cpp | 4566534 | 7.34 s | 4673 MB | 15.8 MB |
| after-d85f31f | mimir | orc | write-edges-builder | graphar-native | 4566534 | 2.28 s | 2906 MB | 55.4 MB |
| after-d85f31f | mimir | orc | write-edges-table | graphar-cpp | 4566534 | 1.35 s | 1233 MB | 14.8 MB |
| after-d85f31f | mimir | orc | write-edges-table | graphar-native | 4566534 | 400.6 ms | 1045 MB | 55.4 MB |
| after-d85f31f | mimir | orc | write-vertices-builder | graphar-cpp | 3421126 | 7.69 s | 4673 MB | 16.0 MB |
| after-d85f31f | mimir | orc | write-vertices-builder | graphar-native | 3421126 | 3.23 s | 2906 MB | 461.1 MB |
| after-d85f31f | mimir | orc | write-vertices-builder | graphar-rs | 3421126 | 8.69 s | 4356 MB | - |
| after-d85f31f | mimir | orc | write-vertices-table | graphar-cpp | 3421126 | 1.05 s | 1233 MB | 16.0 MB |
| after-d85f31f | mimir | orc | write-vertices-table | graphar-native | 3421126 | 307.3 ms | 1045 MB | 461.1 MB |
| after-d85f31f | mimir | parquet | write-edges-builder | graphar-cpp | 4566534 | 8.05 s | 4673 MB | 25.0 MB |
| after-d85f31f | mimir | parquet | write-edges-builder | graphar-native | 4566534 | 2.60 s | 2911 MB | 7.3 MB |
| after-d85f31f | mimir | parquet | write-edges-table | graphar-cpp | 4566534 | 1.49 s | 1203 MB | 24.6 MB |
| after-d85f31f | mimir | parquet | write-edges-table | graphar-native | 4566534 | 450.2 ms | 1046 MB | 7.3 MB |
| after-d85f31f | mimir | parquet | write-vertices-builder | graphar-cpp | 3421126 | 8.39 s | 4673 MB | 35.9 MB |
| after-d85f31f | mimir | parquet | write-vertices-builder | graphar-native | 3421126 | 3.78 s | 2911 MB | 15.2 MB |
| after-d85f31f | mimir | parquet | write-vertices-builder | graphar-rs | 3421126 | 8.68 s | 4355 MB | - |
| after-d85f31f | mimir | parquet | write-vertices-table | graphar-cpp | 3421126 | 1.43 s | 1203 MB | 35.9 MB |
| after-d85f31f | mimir | parquet | write-vertices-table | graphar-native | 3421126 | 684.5 ms | 1046 MB | 15.2 MB |

### Reads

| label | dataset | format | operation | files written by | native | C++ | C++ / native | native RSS | C++ RSS |
|---|---|---|---|---|---:|---:|---:|---:|---:|
| after-d85f31f | ldbc | arrow | read-csr | native-table | 1.0 ms | - | - | 9 MB | - |
| after-d85f31f | ldbc | arrow | read-prop | native-table | 3.3 ms | - | - | 9 MB | - |
| after-d85f31f | ldbc | arrow | scan | native-table | 11.2 ms | - | - | 9 MB | - |
| after-d85f31f | ldbc | csv | read-csr | cpp-table | 7.9 ms | 27.3 ms | 3.4x | 12 MB | 48 MB |
| after-d85f31f | ldbc | csv | read-csr | native-table | 12.0 ms | 26.6 ms | 2.2x | 12 MB | 46 MB |
| after-d85f31f | ldbc | csv | read-prop | cpp-table | 7.4 ms | 17.7 ms | 2.4x | 14 MB | 47 MB |
| after-d85f31f | ldbc | csv | read-prop | native-table | 11.8 ms | 21.4 ms | 1.8x | 14 MB | 45 MB |
| after-d85f31f | ldbc | csv | scan | cpp-table | 19.7 ms | 59.3 ms | 3.0x | 16 MB | 65 MB |
| after-d85f31f | ldbc | csv | scan | native-table | 23.0 ms | 66.0 ms | 2.9x | 16 MB | 59 MB |
| after-d85f31f | ldbc | orc | read-csr | cpp-table | 2.1 ms | 13.8 ms | 6.4x | 10 MB | 42 MB |
| after-d85f31f | ldbc | orc | read-csr | native-table | 2.1 ms | 11.8 ms | 5.7x | 9 MB | 42 MB |
| after-d85f31f | ldbc | orc | read-prop | cpp-table | 2.0 ms | 11.1 ms | 5.5x | 10 MB | 44 MB |
| after-d85f31f | ldbc | orc | read-prop | native-table | 3.4 ms | 11.4 ms | 3.3x | 9 MB | 43 MB |
| after-d85f31f | ldbc | orc | scan | cpp-table | 11.3 ms | 21.2 ms | 1.9x | 10 MB | 43 MB |
| after-d85f31f | ldbc | orc | scan | native-table | 11.1 ms | 20.3 ms | 1.8x | 9 MB | 43 MB |
| after-d85f31f | ldbc | parquet | read-csr | cpp-table | 1.4 ms | 10.2 ms | 7.1x | 11 MB | 42 MB |
| after-d85f31f | ldbc | parquet | read-csr | native-table | 1.3 ms | 11.3 ms | 8.6x | 11 MB | 42 MB |
| after-d85f31f | ldbc | parquet | read-prop | cpp-table | 1.2 ms | 5.6 ms | 4.8x | 11 MB | 42 MB |
| after-d85f31f | ldbc | parquet | read-prop | native-table | 0.9 ms | 4.7 ms | 5.3x | 11 MB | 43 MB |
| after-d85f31f | ldbc | parquet | scan | cpp-table | 4.7 ms | 21.1 ms | 4.5x | 11 MB | 43 MB |
| after-d85f31f | ldbc | parquet | scan | native-table | 5.7 ms | 14.7 ms | 2.6x | 12 MB | 43 MB |
| after-d85f31f | ldbc-upstream | csv | read-csr | spark | 5.2 ms | 18.8 ms | 3.6x | 11 MB | 42 MB |
| after-d85f31f | ldbc-upstream | csv | read-prop | spark | 3.8 ms | 11.6 ms | 3.0x | 13 MB | 44 MB |
| after-d85f31f | ldbc-upstream | csv | scan | spark | 9.1 ms | 43.5 ms | 4.8x | 16 MB | 51 MB |
| after-d85f31f | ldbc-upstream | orc | read-csr | spark | 2.1 ms | 9.8 ms | 4.7x | 10 MB | 42 MB |
| after-d85f31f | ldbc-upstream | orc | read-prop | spark | 0.7 ms | 9.0 ms | 13.0x | 10 MB | 42 MB |
| after-d85f31f | ldbc-upstream | orc | scan | spark | 4.1 ms | 16.9 ms | 4.1x | 11 MB | 44 MB |
| after-d85f31f | ldbc-upstream | parquet | read-csr | spark | 1.5 ms | 8.9 ms | 6.0x | 11 MB | 42 MB |
| after-d85f31f | ldbc-upstream | parquet | read-prop | spark | 0.7 ms | 4.6 ms | 6.2x | 11 MB | 42 MB |
| after-d85f31f | ldbc-upstream | parquet | scan | spark | 3.5 ms | 13.1 ms | 3.7x | 11 MB | 43 MB |
| after-d85f31f | mimir | arrow | read-csr | native-table | 61.9 ms | - | - | 81 MB | - |
| after-d85f31f | mimir | arrow | read-prop | native-table | 20.7 ms | - | - | 158 MB | - |
| after-d85f31f | mimir | arrow | scan | native-table | 151.0 ms | - | - | 398 MB | - |
| after-d85f31f | mimir | csv | read-csr | cpp-table | 466.2 ms | 538.6 ms | 1.2x | 82 MB | 126 MB |
| after-d85f31f | mimir | csv | read-csr | native-table | 456.3 ms | 522.4 ms | 1.1x | 82 MB | 145 MB |
| after-d85f31f | mimir | csv | read-prop | cpp-table | 90.9 ms | 547.6 ms | 6.0x | 210 MB | 122 MB |
| after-d85f31f | mimir | csv | read-prop | native-table | 84.0 ms | 513.8 ms | 6.1x | 210 MB | 140 MB |
| after-d85f31f | mimir | csv | scan | cpp-table | 1.06 s | - | - | 426 MB | - |
| after-d85f31f | mimir | csv | scan | native-table | 1.06 s | - | - | 469 MB | - |
| after-d85f31f | mimir | orc | read-csr | cpp-table | 198.1 ms | 156.3 ms | 0.8x | 83 MB | 136 MB |
| after-d85f31f | mimir | orc | read-csr | native-table | 180.6 ms | 146.5 ms | 0.8x | 81 MB | 136 MB |
| after-d85f31f | mimir | orc | read-prop | cpp-table | 105.0 ms | 461.1 ms | 4.4x | 175 MB | 106 MB |
| after-d85f31f | mimir | orc | read-prop | native-table | 193.7 ms | 402.1 ms | 2.1x | 259 MB | 108 MB |
| after-d85f31f | mimir | orc | scan | cpp-table | 683.4 ms | 1.24 s | 1.8x | 417 MB | 196 MB |
| after-d85f31f | mimir | orc | scan | native-table | 900.9 ms | 1.08 s | 1.2x | 746 MB | 214 MB |
| after-d85f31f | mimir | parquet | read-csr | cpp-table | 178.5 ms | 201.5 ms | 1.1x | 83 MB | 143 MB |
| after-d85f31f | mimir | parquet | read-csr | native-table | 92.4 ms | 120.7 ms | 1.3x | 83 MB | 159 MB |
| after-d85f31f | mimir | parquet | read-prop | cpp-table | 185.2 ms | 351.3 ms | 1.9x | 193 MB | 99 MB |
| after-d85f31f | mimir | parquet | read-prop | native-table | 116.0 ms | 343.5 ms | 3.0x | 183 MB | 104 MB |
| after-d85f31f | mimir | parquet | scan | cpp-table | 625.0 ms | 1.01 s | 1.6x | 516 MB | 198 MB |
| after-d85f31f | mimir | parquet | scan | native-table | 399.5 ms | 878.5 ms | 2.2x | 504 MB | 195 MB |

### Same answer from every reader and writer

| label | dataset | format | operation | readers x writers | checks agree |
|---|---|---|---|---:|---|
| after-d85f31f | ldbc | arrow | read-csr | 1 | yes |
| after-d85f31f | ldbc | arrow | read-prop | 1 | yes |
| after-d85f31f | ldbc | arrow | scan | 1 | yes |
| after-d85f31f | ldbc | csv | read-csr | 4 | yes |
| after-d85f31f | ldbc | csv | read-prop | 4 | yes |
| after-d85f31f | ldbc | csv | scan | 4 | NO: {"cells": 10238, "edges": 6626, "sum": 15074304420375848, "vertex_rows": 1806} / {"cells": 10238, "edges": 6626, "sum": 15074304420389100, "vertex_rows": 1806} |
| after-d85f31f | ldbc | orc | read-csr | 4 | yes |
| after-d85f31f | ldbc | orc | read-prop | 4 | yes |
| after-d85f31f | ldbc | orc | scan | 4 | yes |
| after-d85f31f | ldbc | parquet | read-csr | 4 | yes |
| after-d85f31f | ldbc | parquet | read-prop | 4 | yes |
| after-d85f31f | ldbc | parquet | scan | 4 | yes |
| after-d85f31f | ldbc-upstream | csv | read-csr | 2 | yes |
| after-d85f31f | ldbc-upstream | csv | read-prop | 2 | yes |
| after-d85f31f | ldbc-upstream | csv | scan | 2 | NO: {"cells": 10238, "edges": 6626, "sum": 15074304420375848, "vertex_rows": 1806} / {"cells": 10238, "edges": 6626, "sum": 15074304420389100, "vertex_rows": 1806} |
| after-d85f31f | ldbc-upstream | orc | read-csr | 2 | yes |
| after-d85f31f | ldbc-upstream | orc | read-prop | 2 | yes |
| after-d85f31f | ldbc-upstream | orc | scan | 2 | yes |
| after-d85f31f | ldbc-upstream | parquet | read-csr | 2 | yes |
| after-d85f31f | ldbc-upstream | parquet | read-prop | 2 | yes |
| after-d85f31f | ldbc-upstream | parquet | scan | 2 | yes |
| after-d85f31f | mimir | arrow | read-csr | 1 | yes |
| after-d85f31f | mimir | arrow | read-prop | 1 | yes |
| after-d85f31f | mimir | arrow | scan | 1 | yes |
| after-d85f31f | mimir | csv | read-csr | 4 | yes |
| after-d85f31f | mimir | csv | read-prop | 4 | yes |
| after-d85f31f | mimir | csv | scan | 2 | yes |
| after-d85f31f | mimir | orc | read-csr | 4 | yes |
| after-d85f31f | mimir | orc | read-prop | 4 | yes |
| after-d85f31f | mimir | orc | scan | 4 | yes |
| after-d85f31f | mimir | parquet | read-csr | 4 | yes |
| after-d85f31f | mimir | parquet | read-prop | 4 | yes |
| after-d85f31f | mimir | parquet | scan | 4 | yes |

### Errors

- mimir parquet cpp-table: 245: Cancelled: Plan was cancelled early.
- mimir csv cpp-table: 247: Cancelled: Plan was cancelled early.
- mimir csv native-table: 374: Invalid: In CSV column #1: Row #158249: CSV conversion error to null: invalid value '1'
- mimir csv native-table: 374: Invalid: In CSV column #1: Row #158249: CSV conversion error to null: invalid value '1'
- mimir csv native-table: 374: Invalid: In CSV column #1: Row #158249: CSV conversion error to null: invalid value '1'
- mimir csv native-table: 374: Invalid: In CSV column #1: Row #158249: CSV conversion error to null: invalid value '1'
- mimir csv native-table: 374: Invalid: In CSV column #1: Row #158249: CSV conversion error to null: invalid value '1'
- mimir csv cpp-table: 374: Invalid: In CSV column #1: Row #158249: CSV conversion error to null: invalid value '1'
- mimir csv cpp-table: 374: Invalid: In CSV column #1: Row #158249: CSV conversion error to null: invalid value '1'
- mimir csv cpp-table: 374: Invalid: In CSV column #1: Row #158249: CSV conversion error to null: invalid value '1'
- mimir csv cpp-table: 374: Invalid: In CSV column #1: Row #158249: CSV conversion error to null: invalid value '1'
- mimir csv cpp-table: 374: Invalid: In CSV column #1: Row #158249: CSV conversion error to null: invalid value '1'
- mimir orc cpp-table: 247: Cancelled: Plan was cancelled early.
- mimir orc cpp-table: 245: Cancelled: Plan was cancelled early.

## build1-cd4e15e

### Writes

| label | dataset | format | operation | implementation | rows | median time | peak RSS | bytes on disk |
|---|---|---|---|---|---:|---:|---:|---:|
| build1-cd4e15e | mimir | orc | write-edges-builder | graphar-cpp | 4172961 | 6.97 s | 4278 MB | 14.4 MB |
| build1-cd4e15e | mimir | orc | write-edges-builder | graphar-native | 4172961 | 2.21 s | 2636 MB | 12.2 MB |
| build1-cd4e15e | mimir | orc | write-edges-table | graphar-cpp | 4172961 | 1.61 s | 1078 MB | 13.5 MB |
| build1-cd4e15e | mimir | orc | write-edges-table | graphar-native | 4172961 | 602.3 ms | 943 MB | 12.2 MB |
| build1-cd4e15e | mimir | orc | write-vertices-builder | graphar-cpp | 3124200 | 9.18 s | 4278 MB | 14.8 MB |
| build1-cd4e15e | mimir | orc | write-vertices-builder | graphar-native | 3124200 | 4.54 s | 2636 MB | 13.9 MB |
| build1-cd4e15e | mimir | orc | write-vertices-table | graphar-cpp | 3124200 | 1.08 s | 1078 MB | 14.8 MB |
| build1-cd4e15e | mimir | orc | write-vertices-table | graphar-native | 3124200 | 578.0 ms | 943 MB | 13.9 MB |
| build1-cd4e15e | mimir | parquet | write-edges-builder | graphar-cpp | 4172961 | 7.30 s | 4432 MB | 22.9 MB |
| build1-cd4e15e | mimir | parquet | write-edges-builder | graphar-native | 4172961 | 2.22 s | 2632 MB | 6.7 MB |
| build1-cd4e15e | mimir | parquet | write-edges-table | graphar-cpp | 4172961 | 1.57 s | 1079 MB | 22.4 MB |
| build1-cd4e15e | mimir | parquet | write-edges-table | graphar-native | 4172961 | 541.9 ms | 942 MB | 6.7 MB |
| build1-cd4e15e | mimir | parquet | write-vertices-builder | graphar-cpp | 3124200 | 9.61 s | 4432 MB | 33.1 MB |
| build1-cd4e15e | mimir | parquet | write-vertices-builder | graphar-native | 3124200 | 4.36 s | 2632 MB | 14.1 MB |
| build1-cd4e15e | mimir | parquet | write-vertices-table | graphar-cpp | 3124200 | 1.35 s | 1079 MB | 33.1 MB |
| build1-cd4e15e | mimir | parquet | write-vertices-table | graphar-native | 3124200 | 820.7 ms | 942 MB | 14.1 MB |

### Reads

| label | dataset | format | operation | files written by | native | C++ | C++ / native | native RSS | C++ RSS |
|---|---|---|---|---|---:|---:|---:|---:|---:|
| build1-cd4e15e | mimir | orc | read-csr | cpp-table | 235.1 ms | 198.4 ms | 0.8x | 77 MB | 135 MB |
| build1-cd4e15e | mimir | orc | read-csr | native-table | 221.6 ms | 204.4 ms | 0.9x | 76 MB | 136 MB |
| build1-cd4e15e | mimir | orc | read-prop | cpp-table | 101.7 ms | 534.5 ms | 5.3x | 163 MB | 105 MB |
| build1-cd4e15e | mimir | orc | read-prop | native-table | 85.0 ms | 492.8 ms | 5.8x | 165 MB | 100 MB |
| build1-cd4e15e | mimir | orc | scan | cpp-table | 744.9 ms | 1.34 s | 1.8x | 376 MB | 192 MB |
| build1-cd4e15e | mimir | orc | scan | native-table | 658.2 ms | 1.29 s | 2.0x | 379 MB | 193 MB |
| build1-cd4e15e | mimir | parquet | read-csr | cpp-table | 218.7 ms | 285.2 ms | 1.3x | 78 MB | 141 MB |
| build1-cd4e15e | mimir | parquet | read-csr | native-table | 134.3 ms | 193.6 ms | 1.4x | 78 MB | 160 MB |
| build1-cd4e15e | mimir | parquet | read-prop | cpp-table | 109.1 ms | 338.2 ms | 3.1x | 178 MB | 103 MB |
| build1-cd4e15e | mimir | parquet | read-prop | native-table | 154.3 ms | 307.8 ms | 2.0x | 170 MB | 107 MB |
| build1-cd4e15e | mimir | parquet | scan | cpp-table | 486.6 ms | 1.08 s | 2.2x | 460 MB | 167 MB |
| build1-cd4e15e | mimir | parquet | scan | native-table | 391.5 ms | 871.1 ms | 2.2x | 453 MB | 194 MB |

### Same answer from every reader and writer

| label | dataset | format | operation | readers x writers | checks agree |
|---|---|---|---|---:|---|
| build1-cd4e15e | mimir | orc | read-csr | 4 | yes |
| build1-cd4e15e | mimir | orc | read-prop | 4 | yes |
| build1-cd4e15e | mimir | orc | scan | 4 | yes |
| build1-cd4e15e | mimir | parquet | read-csr | 4 | yes |
| build1-cd4e15e | mimir | parquet | read-prop | 4 | yes |
| build1-cd4e15e | mimir | parquet | scan | 4 | yes |
