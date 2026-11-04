/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// C++ side of the bench: the same commands and JSON as ../rust, on the
// GraphAr C++ library.
//
//   gar_bench_cpp write <table|builder> <input-dir> <info-dir> <out-graph-dir>
//   gar_bench_cpp read-prop <graph-dir> <property>
//   gar_bench_cpp read-csr <graph-dir>
//   gar_bench_cpp scan <graph-dir>

#include <algorithm>
#include <any>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/compute/api.h"
#include "arrow/io/api.h"
#include "parquet/arrow/reader.h"

#include "graphar/api/arrow_reader.h"
#include "graphar/api/arrow_writer.h"
#include "graphar/api/high_level_writer.h"
#include "graphar/graph_info.h"
#include "graphar/reader_util.h"

namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;

#define CHECK_OK(expr)                                              \
  do {                                                              \
    auto _s = (expr);                                               \
    if (!_s.ok()) {                                                 \
      std::cerr << __LINE__ << ": " << _s.message() << std::endl; \
      std::exit(1);                                                 \
    }                                                               \
  } while (0)

template <typename T>
T unwrap(graphar::Result<T> r, int line) {
  if (!r.status().ok()) {
    std::cerr << line << ": " << r.status().message() << std::endl;
    std::exit(1);
  }
  return std::move(r).value();
}
template <typename T>
T unwrap(arrow::Result<T> r, int line) {
  if (!r.ok()) {
    std::cerr << line << ": " << r.status().ToString() << std::endl;
    std::exit(1);
  }
  return std::move(r).ValueOrDie();
}
#define UNWRAP(expr) unwrap((expr), __LINE__)

static double secs_since(Clock::time_point t) {
  return std::chrono::duration<double>(Clock::now() - t).count();
}

static void emit(const std::string& op, double secs, const std::string& check) {
  std::cout << "{\"impl\":\"graphar-cpp\",\"op\":\"" << op
            << "\",\"secs\":" << secs << ",\"check\":{" << check << "}}"
            << std::endl;
}

static std::shared_ptr<arrow::Table> read_parquet(const std::string& path) {
  auto file = UNWRAP(arrow::io::ReadableFile::Open(path));
  auto reader =
      UNWRAP(parquet::arrow::OpenFile(file, arrow::default_memory_pool()));
  std::shared_ptr<arrow::Table> table = UNWRAP(reader->ReadTable());
  return UNWRAP(table->CombineChunks());
}

struct Graph {
  std::shared_ptr<graphar::GraphInfo> info;
  std::shared_ptr<graphar::VertexInfo> vi;
  std::shared_ptr<graphar::EdgeInfo> ei;
  std::string prefix;  // absolute, ends with '/'
};

static Graph open_graph(const std::string& dir) {
  Graph g;
  // The first *.graph.yml by name (the upstream sample dirs hold several).
  std::vector<fs::path> ymls;
  for (auto& e : fs::directory_iterator(dir)) {
    auto name = e.path().filename().string();
    if (name.size() > 10 && name.substr(name.size() - 10) == ".graph.yml") {
      ymls.push_back(e.path());
    }
  }
  std::sort(ymls.begin(), ymls.end());
  if (!ymls.empty()) {
    g.info = UNWRAP(graphar::GraphInfo::Load(fs::absolute(ymls[0]).string()));
  }
  if (!g.info) {
    std::cerr << "no *.graph.yml in " << dir << std::endl;
    std::exit(1);
  }
  g.vi = g.info->GetVertexInfoByIndex(0);
  g.ei = g.info->GetEdgeInfoByIndex(0);
  g.prefix = fs::absolute(dir).string() + "/";
  return g;
}

// GetChunk(AUTO) takes the Parquet-only V2 path for every format when no
// filter is set, so CSV and ORC groups are read with V1.
static graphar::GetChunkVersion chunk_version(
    const std::shared_ptr<graphar::PropertyGroup>& group) {
  return group->GetFileType() == graphar::FileType::PARQUET
             ? graphar::GetChunkVersion::AUTO
             : graphar::GetChunkVersion::V1;
}

static uint64_t dir_bytes(const fs::path& p) {
  uint64_t total = 0;
  if (!fs::exists(p)) return 0;
  for (auto& e : fs::recursive_directory_iterator(p)) {
    if (e.is_regular_file()) total += e.file_size();
  }
  return total;
}

// Order-independent sum, the same as ../rust: integers, days and milliseconds
// by value, floats by their bits, booleans counted, strings by length, lists
// by their length plus their items.
template <typename ArrayT>
static uint64_t sum_strings(const std::shared_ptr<arrow::Array>& a) {
  auto s = std::static_pointer_cast<ArrayT>(a);
  uint64_t sum = 0;
  for (int64_t i = 0; i < s->length(); i++)
    if (s->IsValid(i)) sum += s->value_length(i);
  return sum;
}

template <typename ArrayT, typename F>
static uint64_t sum_values(const std::shared_ptr<arrow::Array>& a, F f) {
  auto p = std::static_pointer_cast<ArrayT>(a);
  uint64_t sum = 0;
  for (int64_t i = 0; i < p->length(); i++)
    if (p->IsValid(i)) sum += f(p->Value(i));
  return sum;
}

static uint64_t checksum_array(const std::shared_ptr<arrow::Array>& a) {
  auto widen = [](auto v) {
    return static_cast<uint64_t>(static_cast<int64_t>(v));
  };
  switch (a->type_id()) {
    case arrow::Type::INT64:
      return sum_values<arrow::Int64Array>(a, widen);
    case arrow::Type::INT32:
      return sum_values<arrow::Int32Array>(a, widen);
    case arrow::Type::DATE32:
      return sum_values<arrow::Date32Array>(a, widen);
    case arrow::Type::TIMESTAMP: {
      auto unit =
          std::static_pointer_cast<arrow::TimestampType>(a->type())->unit();
      int64_t div = unit == arrow::TimeUnit::NANO    ? 1000000
                    : unit == arrow::TimeUnit::MICRO ? 1000
                                                     : 1;
      return sum_values<arrow::TimestampArray>(
          a, [div](int64_t v) { return static_cast<uint64_t>(v / div); });
    }
    case arrow::Type::FLOAT:
      return sum_values<arrow::FloatArray>(a, [](float v) {
        uint32_t bits;
        std::memcpy(&bits, &v, sizeof bits);
        return static_cast<uint64_t>(bits);
      });
    case arrow::Type::DOUBLE:
      return sum_values<arrow::DoubleArray>(a, [](double v) {
        uint64_t bits;
        std::memcpy(&bits, &v, sizeof bits);
        return bits;
      });
    case arrow::Type::BOOL:
      return sum_values<arrow::BooleanArray>(
          a, [](bool v) { return static_cast<uint64_t>(v); });
    case arrow::Type::STRING:
      return sum_strings<arrow::StringArray>(a);
    case arrow::Type::LARGE_STRING:
      return sum_strings<arrow::LargeStringArray>(a);
    case arrow::Type::LIST: {
      auto l = std::static_pointer_cast<arrow::ListArray>(a);
      uint64_t sum = 0;
      for (int64_t i = 0; i < l->length(); i++) {
        if (l->IsNull(i)) continue;
        auto v = l->value_slice(i);
        sum += v->length() + checksum_array(v);
      }
      return sum;
    }
    default:
      return a->length();
  }
}

static uint64_t checksum_column(const std::shared_ptr<arrow::ChunkedArray>& c) {
  uint64_t sum = 0;
  for (auto& chunk : c->chunks()) sum += checksum_array(chunk);
  return sum;
}

// --------------------------------------------------------------------- write

static std::any value_at(const std::shared_ptr<arrow::Array>& a, int64_t i) {
  if (a->type_id() == arrow::Type::INT64)
    return std::static_pointer_cast<arrow::Int64Array>(a)->Value(i);
  return std::static_pointer_cast<arrow::StringArray>(a)->GetString(i);
}

static void write(const std::string& api, const std::string& input,
                  const std::string& info_dir, const std::string& out) {
  fs::create_directories(out);
  for (auto& e : fs::directory_iterator(info_dir)) {
    fs::copy_file(e.path(), fs::path(out) / e.path().filename(),
                  fs::copy_options::overwrite_existing);
  }
  Graph g = open_graph(out);
  auto vertices = read_parquet(input + "/vertices.parquet");
  auto edges = read_parquet(input + "/edges.parquet");
  const int64_t n = vertices->num_rows();
  const auto t = graphar::AdjListType::ordered_by_source;

  // Without explicit options the C++ writers write Parquet uncompressed;
  // ask for ZSTD, the native crate's default, so both write the same codec.
  graphar::WriterOptions::ParquetOptionBuilder pb;
  pb.compression(arrow::Compression::ZSTD);
  auto wopt = pb.build();

  auto t0 = Clock::now();
  if (api == "table") {
    auto w = UNWRAP(graphar::VertexPropertyWriter::Make(g.vi, g.prefix, wopt));
    CHECK_OK(w->WriteTable(vertices, 0));
    CHECK_OK(w->WriteVerticesNum(n));
  } else {
    auto b = UNWRAP(graphar::builder::VerticesBuilder::Make(g.vi, g.prefix, wopt));
    std::vector<std::shared_ptr<arrow::Array>> cols;
    for (auto& c : vertices->columns()) cols.push_back(c->chunk(0));
    auto names = vertices->schema()->field_names();
    for (int64_t i = 0; i < n; i++) {
      graphar::builder::Vertex v;
      for (size_t c = 0; c < cols.size(); c++) {
        if (cols[c]->IsValid(i)) v.AddProperty(names[c], value_at(cols[c], i));
      }
      CHECK_OK(b->AddVertex(v));
    }
    CHECK_OK(b->Dump());
  }
  double vsecs = secs_since(t0);

  t0 = Clock::now();
  const int64_t m = edges->num_rows();
  if (api == "table") {
    // The C++ table API writes one vertex chunk at a time; partition by the
    // source vertex chunk first (counted in the time).
    auto w = UNWRAP(graphar::EdgeChunkWriter::Make(g.ei, g.prefix, t, wopt));
    const int64_t vchunk = g.ei->GetSrcChunkSize();
    const int64_t parts = (n + vchunk - 1) / vchunk;
    auto src = std::static_pointer_cast<arrow::Int64Array>(
        edges->GetColumnByName("_graphArSrcIndex")->chunk(0));
    std::vector<std::vector<int64_t>> rows(parts);
    for (int64_t i = 0; i < m; i++) {
      int64_t s = src->Value(i);
      if (s >= 0 && s < n) rows[s / vchunk].push_back(i);
    }
    CHECK_OK(w->WriteVerticesNum(n));
    for (int64_t p = 0; p < parts; p++) {
      arrow::Int64Builder ib;
      CHECK_OK(ib.AppendValues(rows[p]));
      auto idx = UNWRAP(ib.Finish());
      auto part = UNWRAP(arrow::compute::Take(edges, idx)).table();
      CHECK_OK(w->WriteEdgesNum(p, part->num_rows()));
      CHECK_OK(w->SortAndWriteAdjListTable(part, p, 0));
      if (part->num_rows() > 0) {
        CHECK_OK(w->SortAndWritePropertyTable(part, p, 0));
      }
    }
  } else {
    auto b = UNWRAP(graphar::builder::EdgesBuilder::Make(g.ei, g.prefix, t, n, wopt));
    auto src = std::static_pointer_cast<arrow::Int64Array>(
        edges->GetColumnByName("_graphArSrcIndex")->chunk(0));
    auto dst = std::static_pointer_cast<arrow::Int64Array>(
        edges->GetColumnByName("_graphArDstIndex")->chunk(0));
    std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>> props;
    for (int c = 0; c < edges->num_columns(); c++) {
      auto name = edges->schema()->field(c)->name();
      if (name != "_graphArSrcIndex" && name != "_graphArDstIndex")
        props.emplace_back(name, edges->column(c)->chunk(0));
    }
    for (int64_t i = 0; i < m; i++) {
      graphar::builder::Edge e(src->Value(i), dst->Value(i));
      for (auto& [name, col] : props)
        if (col->IsValid(i)) e.AddProperty(name, value_at(col, i));
      CHECK_OK(b->AddEdge(e));
    }
    CHECK_OK(b->Dump());
  }
  double esecs = secs_since(t0);
  emit("write-vertices-" + api, vsecs,
       "\"vertices\":" + std::to_string(n) + ",\"bytes\":" +
           std::to_string(dir_bytes(fs::path(g.prefix) / g.vi->GetPrefix())));
  emit("write-edges-" + api, esecs,
       "\"edges\":" + std::to_string(m) + ",\"bytes\":" +
           std::to_string(dir_bytes(fs::path(g.prefix) / g.ei->GetPrefix())));
}

// ---------------------------------------------------------------------- read

static void read_prop(const std::string& dir, const std::string& prop) {
  Graph g = open_graph(dir);
  auto t0 = Clock::now();
  auto group = g.vi->GetPropertyGroup(prop);
  auto r = UNWRAP(
      graphar::VertexPropertyArrowChunkReader::Make(g.vi, group, g.prefix));
  int64_t rows = 0;
  uint64_t sum = 0;
  while (true) {
    auto tab = UNWRAP(r->GetChunk(chunk_version(group)));
    rows += tab->num_rows();
    sum += checksum_column(tab->GetColumnByName(prop));
    if (!r->next_chunk().ok()) break;
  }
  double secs = secs_since(t0);
  emit("read-prop", secs,
       "\"rows\":" + std::to_string(rows) + ",\"sum\":" + std::to_string(sum) +
           ",\"prop\":\"" + prop + "\"");
}

static void read_csr(const std::string& dir) {
  Graph g = open_graph(dir);
  const auto t = graphar::AdjListType::ordered_by_source;
  auto t0 = Clock::now();
  const int64_t n = UNWRAP(graphar::util::GetVertexNum(g.prefix, g.ei, t));
  const int64_t vchunk = g.ei->GetSrcChunkSize();
  const int64_t parts = (n + vchunk - 1) / vchunk;
  std::vector<int64_t> offsets;
  offsets.reserve(n + 1);
  offsets.push_back(0);
  std::vector<int64_t> indices;
  auto off_reader =
      UNWRAP(graphar::AdjListOffsetArrowChunkReader::Make(g.ei, t, g.prefix));
  auto adj_reader =
      UNWRAP(graphar::AdjListArrowChunkReader::Make(g.ei, t, g.prefix));
  for (int64_t p = 0; p < parts; p++) {
    const int64_t base = offsets.back();
    // Vertices in this chunk; the C++ table writer pads the last chunk's
    // offsets to chunk_size + 1 rows. GetChunk() returns only the first Arrow
    // chunk of the offset column (chunk_reader.cc: `->column(0)->chunk(0)`),
    // which for large vertex chunks is a prefix, so seek forward until the
    // offsets of all len vertices are in; the closing offset of the chunk is
    // its edge count (seek cannot address row len, it belongs to the next
    // chunk's id range).
    const int64_t len = std::min(vchunk, n - p * vchunk);
    const int64_t edges_in_part =
        UNWRAP(graphar::util::GetEdgeNum(g.prefix, g.ei, t, p));
    int64_t have = 0;  // offset rows of this chunk taken (row 0 is the leading 0)
    while (have < len) {
      CHECK_OK(off_reader->seek(p * vchunk + have));
      auto off = std::static_pointer_cast<arrow::Int64Array>(
          UNWRAP(off_reader->GetChunk()));
      if (off->length() == 0) break;
      for (int64_t i = 0; i < off->length() && have < len; i++, have++) {
        if (have > 0) offsets.push_back(base + off->Value(i));
      }
    }
    offsets.push_back(base + edges_in_part);
    if (edges_in_part == 0) continue;
    CHECK_OK(adj_reader->seek_chunk_index(p, 0));
    int64_t got = 0;
    while (got < edges_in_part) {
      auto tab = UNWRAP(adj_reader->GetChunk());
      for (auto& chunk : tab->GetColumnByName("_graphArDstIndex")->chunks()) {
        auto a = std::static_pointer_cast<arrow::Int64Array>(chunk);
        indices.insert(indices.end(), a->raw_values(),
                       a->raw_values() + a->length());
      }
      got += tab->num_rows();
      if (got < edges_in_part && !adj_reader->next_chunk().ok()) break;
    }
  }
  double secs = secs_since(t0);
  uint64_t si = 0, so = 0;
  for (auto v : indices) si += static_cast<uint64_t>(v);
  for (auto v : offsets) so += static_cast<uint64_t>(v);
  emit("read-csr", secs,
       "\"n\":" + std::to_string(offsets.size() - 1) +
           ",\"m\":" + std::to_string(indices.size()) +
           ",\"sum_indices\":" + std::to_string(si) +
           ",\"sum_offsets\":" + std::to_string(so));
}

static void scan(const std::string& dir) {
  Graph g = open_graph(dir);
  const auto t = graphar::AdjListType::ordered_by_source;
  auto t0 = Clock::now();
  int64_t vrows = 0, erows = 0;
  uint64_t cells = 0, sum = 0;
  for (auto& group : g.vi->GetPropertyGroups()) {
    auto r = UNWRAP(
        graphar::VertexPropertyArrowChunkReader::Make(g.vi, group, g.prefix));
    while (true) {
      auto tab = UNWRAP(r->GetChunk(chunk_version(group)));
      vrows += tab->num_rows();
      for (auto& p : group->GetProperties()) {
        auto c = tab->GetColumnByName(p.name);
        cells += c->length();
        sum += checksum_column(c);
      }
      if (!r->next_chunk().ok()) break;
    }
  }
  const int64_t n = UNWRAP(graphar::util::GetVertexNum(g.prefix, g.ei, t));
  const int64_t parts = (n + g.ei->GetSrcChunkSize() - 1) / g.ei->GetSrcChunkSize();
  auto adj = UNWRAP(graphar::AdjListArrowChunkReader::Make(g.ei, t, g.prefix));
  for (int64_t p = 0; p < parts; p++) {
    const int64_t want = UNWRAP(graphar::util::GetEdgeNum(g.prefix, g.ei, t, p));
    if (want == 0) continue;
    CHECK_OK(adj->seek_chunk_index(p, 0));
    int64_t got = 0;
    while (got < want) {
      auto tab = UNWRAP(adj->GetChunk());
      erows += tab->num_rows();
      sum += checksum_column(tab->GetColumnByName("_graphArDstIndex"));
      got += tab->num_rows();
      if (got < want && !adj->next_chunk().ok()) break;
    }
  }
  for (auto& group : g.ei->GetPropertyGroups()) {
    auto r = UNWRAP(graphar::AdjListPropertyArrowChunkReader::Make(
        g.ei, group, t, g.prefix));
    for (int64_t p = 0; p < parts; p++) {
      const int64_t want =
          UNWRAP(graphar::util::GetEdgeNum(g.prefix, g.ei, t, p));
      if (want == 0) continue;
      CHECK_OK(r->seek_chunk_index(p, 0));
      int64_t got = 0;
      while (got < want) {
        auto tab = UNWRAP(r->GetChunk());
        for (auto& prop : group->GetProperties()) {
          auto c = tab->GetColumnByName(prop.name);
          cells += c->length();
          sum += checksum_column(c);
        }
        got += tab->num_rows();
        if (got < want && !r->next_chunk().ok()) break;
      }
    }
  }
  double secs = secs_since(t0);
  emit("scan", secs,
       "\"vertex_rows\":" + std::to_string(vrows) +
           ",\"edges\":" + std::to_string(erows) +
           ",\"cells\":" + std::to_string(cells) +
           ",\"sum\":" + std::to_string(sum));
}

int main(int argc, char** argv) {
  std::vector<std::string> a(argv + 1, argv + argc);
  a.resize(5);
  if (a[0] == "write") {
    write(a[1], a[2], a[3], a[4]);
  } else if (a[0] == "read-prop") {
    read_prop(a[1], a[2]);
  } else if (a[0] == "read-csr") {
    read_csr(a[1]);
  } else if (a[0] == "scan") {
    scan(a[1]);
  } else {
    std::cerr << "unknown command " << a[0] << std::endl;
    return 2;
  }
  return 0;
}
