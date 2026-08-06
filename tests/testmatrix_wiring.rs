//! Wiring proof for graphar's `functional_status` test-matrix surface (the
//! newest feature: the gar/v1 ChunkStore write path and the graph-info
//! validator report their health into the nornir matrix).
//!
//! INJECT-AND-ASSERT (project LAW): the *validate* surface is pure/offline, so
//! this drives the REAL `validate_graph_info` over real `GraphInfo`s and
//! asserts on the REAL emitted rows. The *write* surface's live call sits
//! inside async `ChunkStore::write_chunk` (needs an `object_store` backend), so
//! — mirroring graphar-flight — this exercises the crate's public `functional_status`
//! wrapper directly (the same forwarder `write_chunk` calls). Offline, in-process.
//!
//! One test per build (single test per binary → no race on the process-global
//! functional buffer `--features testmatrix` records into):
//!   * default build → the emit is a compiled-out no-op; assert the wrapper
//!     links and the real validator still returns its issues.
//!   * `--features testmatrix` → assert the validator records a green row for a
//!     conformant graph and a red row for a non-conformant one, and that the
//!     write-surface wrapper forwards its outcome.

use graphar::{GraphInfo, ValidateOptions, validate_graph_info};
use std::path::Path;

/// A conformant, self-contained graph (no referenced vertex/edge files → no
/// load errors; `GraphInfo::new` stamps the `gar/v1` version).
fn good_graph() -> GraphInfo {
    GraphInfo::new("social", "./")
}

/// A non-conformant graph: an empty `name` is a spec `Error`.
fn bad_graph() -> GraphInfo {
    GraphInfo::new("", "./")
}

/// Default build: `testmatrix` OFF → `functional_status` is an inlined no-op.
/// Prove it links and the validator it is wired into still returns its issues.
#[cfg(not(feature = "testmatrix"))]
#[test]
fn functional_status_is_a_linked_noop_and_validator_runs() {
    graphar::functional_status("graphar/chunk_store", "write_chunk", true, "noop");

    let ok = validate_graph_info(&good_graph(), Path::new("."), &ValidateOptions::default());
    assert!(ok.is_empty(), "a conformant graph has no issues: {ok:?}");

    let bad = validate_graph_info(&bad_graph(), Path::new("."), &ValidateOptions::default());
    assert!(!bad.is_empty(), "an empty-name graph must have issues");
}

/// `--features testmatrix`: the validator records a green `graph_info` row for
/// a conformant graph and a red one for a non-conformant graph, and the
/// write-surface wrapper forwards its outcome with the right status mapping.
#[cfg(feature = "testmatrix")]
#[test]
fn surfaces_record_green_and_red_rows() {
    use nornir_testmatrix::{drain_functional_rows, status};

    let _ = drain_functional_rows();

    // Conformant graph → green validate row.
    let issues = validate_graph_info(&good_graph(), Path::new("."), &ValidateOptions::default());
    assert!(issues.is_empty(), "conformant graph: {issues:?}");

    // Write surface: a successful chunk write forwards green.
    graphar::functional_status("graphar/chunk_store", "write_chunk", true, "vertex/chunk0");

    // Non-conformant graph → red validate row.
    let bad = validate_graph_info(&bad_graph(), Path::new("."), &ValidateOptions::default());
    assert!(!bad.is_empty(), "non-conformant graph must produce issues");

    let rows = drain_functional_rows();

    let greens: Vec<_> = rows
        .iter()
        .filter(|r| r.suite == "graphar/validate" && r.test_name == "graph_info")
        .collect();
    assert_eq!(greens.len(), 2, "two validate rows recorded; got {rows:?}");
    assert!(
        status::is_green(&greens[0].status),
        "conformant graph is GREEN: {:?}",
        greens[0]
    );
    assert_eq!(greens[0].aspect, "functional");
    assert!(
        status::is_red(&greens[1].status),
        "empty-name graph is RED: {:?}",
        greens[1]
    );

    let write = rows
        .iter()
        .find(|r| r.suite == "graphar/chunk_store" && r.test_name == "write_chunk")
        .unwrap_or_else(|| panic!("write surface must record; got {rows:?}"));
    assert!(
        status::is_green(&write.status),
        "a successful write is GREEN: {write:?}"
    );
    assert_eq!(
        write.message, "vertex/chunk0",
        "detail carries the chunk rel path"
    );
}
