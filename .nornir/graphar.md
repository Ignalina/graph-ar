# graphar — crate card

## Reverse-engineered (from source — verify, don't trust the name)
- **One-line pitch:** A Rust reader/writer for the Apache GraphAr on-disk graph format (vertex/edge tables + YAML metadata) over multiple file formats and object stores.
- **Does:** Models GraphAr metadata (`GraphInfo`, `VertexInfo`, `EdgeInfo`, `Property`, `PropertyGroup`, `AdjListInfo`) and reads/writes vertex & edge data as Arrow `RecordBatch`es. Public surface: `VertexReader`/`EdgeReader`, `VerticesBuilder`/`EdgesBuilder` (`Vertex`, `Edge`, `PropertyValue`), `merge_property_groups`, and a validation layer (`validate_graph_dir`, `ValidationIssue`, `Severity`). IO backends: Parquet, ORC, Arrow IPC, CSV, JSON.
- **Key deps:** `arrow`/`arrow-array`/`arrow-schema`/`parquet`/`arrow-csv` (columnar IO), `orc-rust` (ORC), `object_store` + `url` + `bytes` (S3/GCS/Azure/HTTP storage), `serde_yaml`/`serde_json` (metadata), `thiserror`. No intra-workspace deps — this is the base layer.
- **Shape:** lib only; medium. Modules: `metadata/` (graph/vertex/edge/property info), `reader/`, `writer/`, `io/` (parquet/orc/ipc/json/csv), `storage`, `merge`, `validate`, `types`, `error`.
- **Observed role in the workspace:** Foundational format crate. Consumed by `graphar-falkordb` (loads its batches into FalkorDB) and transitively by `graphar-flight`, `knut-bifrost`, `knut-popviz`.

## Fill in (one line each — only the human knows these)
- Primary consumer(s): 
- Why it exists (vs the obvious alternative): 
- Hard constraint (airgap / no-C-deps / no-FFI / pure-Rust / etc.): 
- Release cadence & independence (rides a parent? own repo someday?): 
- Status (production / prototype / experimental / stubbed): 
- Must-NOT-change invariant: 
- Biggest known gap or TODO: 
- Anything a newcomer would get wrong about it: 
