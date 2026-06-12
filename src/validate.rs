//! Structural validation of a written GraphAr directory against the
//! Apache GraphAr `gar/v1` spec.
//!
//! GraphAr defines a fixed on-disk layout: a graph is described by YAML info
//! files (`*.graph.yml` / `*.vertex.yml` / `*.edge.yml`) and its data lives in
//! *chunks* under a deterministic directory tree
//! (`vertex/<label>/<group>/chunk<N>`, `edge/<src>_<type>_<dst>/<adj>/...`,
//! offsets for ordered adjacency lists, etc.). This module walks a graph
//! directory and reports every place it deviates from those rules.
//!
//! The entry points are [`validate_graph_dir`] (validate a whole graph from its
//! `*.graph.yml`) and the convenience wrappers [`GraphInfo::validate`],
//! [`VertexInfo::validate_dir`] and [`EdgeInfo::validate_dir`]. Each returns a
//! `Vec<`[`ValidationIssue`]`>`: empty means conformant. The validator is
//! read-only and never touches the network.
//!
//! It validates knut's own write convention — chunk files carry the format's
//! file extension (`chunk0.parquet`). The upstream testdata corpus instead
//! writes *extensionless* chunk files; [`ValidateOptions::extensionless_chunks`]
//! relaxes the file-name check so the opt-in corpus test (gated on
//! `GRAPHAR_TESTDATA`) can validate those graphs too.

use std::path::{Path, PathBuf};

use crate::{
    metadata::{EdgeInfo, GraphInfo, PropertyGroup, VertexInfo},
    types::FileType,
};

/// How serious a single validation finding is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// A spec violation: the directory does not conform to `gar/v1`.
    Error,
    /// A non-fatal deviation worth surfacing (e.g. an extra unreferenced file).
    Warning,
}

/// One finding from validating a GraphAr directory.
#[derive(Debug, Clone)]
pub struct ValidationIssue {
    pub severity: Severity,
    /// Human-readable description of what is wrong.
    pub message: String,
    /// The path (info file or chunk) the issue concerns, relative to the
    /// graph directory where meaningful.
    pub path: Option<PathBuf>,
}

impl ValidationIssue {
    fn error(message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Error,
            message: message.into(),
            path: None,
        }
    }

    fn error_at(message: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        Self {
            severity: Severity::Error,
            message: message.into(),
            path: Some(path.into()),
        }
    }

    fn warn_at(message: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        Self {
            severity: Severity::Warning,
            message: message.into(),
            path: Some(path.into()),
        }
    }
}

impl std::fmt::Display for ValidationIssue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match self.severity {
            Severity::Error => "error",
            Severity::Warning => "warning",
        };
        match &self.path {
            Some(p) => write!(f, "{kind}: {} ({})", self.message, p.display()),
            None => write!(f, "{kind}: {}", self.message),
        }
    }
}

/// Tunables for the structural validator.
#[derive(Debug, Clone)]
pub struct ValidateOptions {
    /// Accept extensionless chunk files (`chunk0` instead of `chunk0.parquet`).
    /// The upstream `gar/v1` testdata corpus writes chunks this way; knut's own
    /// writers always include the extension, so this defaults to `false`.
    pub extensionless_chunks: bool,
    /// If `false`, a missing data chunk for a declared property group is only a
    /// warning rather than an error. Useful when validating just the info-file
    /// schema of a corpus whose chunk count isn't known ahead of time.
    pub require_chunks: bool,
}

impl Default for ValidateOptions {
    fn default() -> Self {
        Self {
            extensionless_chunks: false,
            require_chunks: true,
        }
    }
}

impl ValidateOptions {
    /// Lenient profile for reading the upstream testdata corpus: extensionless
    /// chunk files are accepted and a missing chunk is a warning, not an error.
    pub fn corpus() -> Self {
        Self {
            extensionless_chunks: true,
            require_chunks: false,
        }
    }
}

const GAR_VERSION: &str = "gar/v1";

/// Validate a whole graph from its `*.graph.yml` info file with the default
/// (strict) options. `graph_yml` is the path to the `.graph.yml`; referenced
/// vertex/edge info files are resolved relative to its directory.
pub fn validate_graph_dir(graph_yml: impl AsRef<Path>) -> Vec<ValidationIssue> {
    validate_graph_dir_with(graph_yml, &ValidateOptions::default())
}

/// Validate a whole graph with explicit options.
pub fn validate_graph_dir_with(
    graph_yml: impl AsRef<Path>,
    opts: &ValidateOptions,
) -> Vec<ValidationIssue> {
    let graph_yml = graph_yml.as_ref();
    let base = graph_yml.parent().unwrap_or_else(|| Path::new("."));

    let gi = match GraphInfo::load(graph_yml) {
        Ok(gi) => gi,
        Err(e) => {
            return vec![ValidationIssue::error_at(
                format!("failed to load graph info: {e}"),
                graph_yml,
            )];
        }
    };
    validate_graph_info(&gi, base, opts)
}

/// Validate a loaded [`GraphInfo`] and everything it references under `base`.
pub fn validate_graph_info(
    gi: &GraphInfo,
    base: impl AsRef<Path>,
    opts: &ValidateOptions,
) -> Vec<ValidationIssue> {
    let base = base.as_ref();
    let mut issues = Vec::new();

    if gi.name.is_empty() {
        issues.push(ValidationIssue::error("graph `name` must not be empty"));
    }
    if gi.version != GAR_VERSION {
        issues.push(ValidationIssue::error(format!(
            "graph `version` must be `{GAR_VERSION}`, found `{}`",
            gi.version
        )));
    }

    // Every referenced vertex info must load and validate.
    for rel in &gi.vertices {
        let yml = base.join(rel);
        match VertexInfo::load(&yml) {
            Ok(vi) => issues.extend(validate_vertex_info(&vi, base, opts)),
            Err(e) => issues.push(ValidationIssue::error_at(
                format!("referenced vertex info failed to load: {e}"),
                yml,
            )),
        }
    }

    for rel in &gi.edges {
        let yml = base.join(rel);
        match EdgeInfo::load(&yml) {
            Ok(ei) => issues.extend(validate_edge_info(&ei, base, opts)),
            Err(e) => issues.push(ValidationIssue::error_at(
                format!("referenced edge info failed to load: {e}"),
                yml,
            )),
        }
    }

    issues
}

/// Validate a single vertex type's info file and its chunk layout under `base`.
pub fn validate_vertex_info(
    vi: &VertexInfo,
    base: impl AsRef<Path>,
    opts: &ValidateOptions,
) -> Vec<ValidationIssue> {
    let base = base.as_ref();
    let mut issues = Vec::new();

    if vi.vertex_type.is_empty() {
        issues.push(ValidationIssue::error("vertex `type` must not be empty"));
    }
    if vi.chunk_size == 0 {
        issues.push(ValidationIssue::error(format!(
            "vertex `{}` chunk_size must be > 0",
            vi.vertex_type
        )));
    }
    if vi.version != GAR_VERSION {
        issues.push(ValidationIssue::error(format!(
            "vertex `{}` version must be `{GAR_VERSION}`, found `{}`",
            vi.vertex_type, vi.version
        )));
    }
    if vi.property_groups.is_empty() {
        issues.push(ValidationIssue::error(format!(
            "vertex `{}` declares no property groups",
            vi.vertex_type
        )));
    }

    issues.extend(check_property_groups(&vi.property_groups, &vi.vertex_type));

    // For each declared property group, the chunk directory must exist and
    // chunk0 must be present (when chunks are required).
    for group in &vi.property_groups {
        let group_dir = base.join(&vi.prefix).join(group.dir_name());
        check_chunk_dir(
            &group_dir,
            &group.file_type,
            opts,
            &mut issues,
            "vertex chunk",
        );
    }

    issues
}

/// Validate a single edge triple's info file and its adjacency/offset/property
/// chunk layout under `base`.
pub fn validate_edge_info(
    ei: &EdgeInfo,
    base: impl AsRef<Path>,
    opts: &ValidateOptions,
) -> Vec<ValidationIssue> {
    let base = base.as_ref();
    let mut issues = Vec::new();

    if ei.src_type.is_empty() || ei.edge_type.is_empty() || ei.dst_type.is_empty() {
        issues.push(ValidationIssue::error(
            "edge `src_type`/`edge_type`/`dst_type` must all be non-empty",
        ));
    }
    let triple = format!("{}_{}_{}", ei.src_type, ei.edge_type, ei.dst_type);
    if ei.chunk_size == 0 {
        issues.push(ValidationIssue::error(format!(
            "edge `{triple}` chunk_size must be > 0"
        )));
    }
    if ei.src_chunk_size == 0 || ei.dst_chunk_size == 0 {
        issues.push(ValidationIssue::error(format!(
            "edge `{triple}` src_chunk_size and dst_chunk_size must be > 0"
        )));
    }
    if ei.version != GAR_VERSION {
        issues.push(ValidationIssue::error(format!(
            "edge `{triple}` version must be `{GAR_VERSION}`, found `{}`",
            ei.version
        )));
    }
    if ei.adj_lists.is_empty() {
        issues.push(ValidationIssue::error(format!(
            "edge `{triple}` declares no adjacency lists"
        )));
    }

    issues.extend(check_property_groups(&ei.property_groups, &triple));

    let edge_dir = base.join(&ei.prefix);
    for adj in &ei.adj_lists {
        let adj_type = match adj.adj_list_type() {
            Ok(t) => t,
            Err(e) => {
                issues.push(ValidationIssue::error(format!(
                    "edge `{triple}` has invalid adj_list type: {e}"
                )));
                continue;
            }
        };
        let adj_dir = edge_dir.join(adj_type.dir_name());

        // adj_list/ subtree must exist with at least part0/chunk0.
        let adj_list_dir = adj_dir.join("adj_list");
        check_part_chunk_dir(
            &adj_list_dir,
            &adj.file_type,
            opts,
            &mut issues,
            "edge adj_list",
        );

        // Ordered adjacency lists carry an offset/ directory; unordered ones
        // must not (the spec only defines offsets for ordered adj lists).
        if adj_type.is_ordered() {
            let offset_dir = adj_dir.join("offset");
            check_chunk_dir(
                &offset_dir,
                &adj.file_type,
                opts,
                &mut issues,
                "edge offset",
            );
        } else if adj_dir.join("offset").exists() {
            issues.push(ValidationIssue::warn_at(
                format!(
                    "edge `{triple}` has an offset/ directory for an unordered adjacency list ({})",
                    adj_type.dir_name()
                ),
                adj_dir.join("offset"),
            ));
        }

        // Each edge property group is partitioned the same way as the adj list.
        for group in &ei.property_groups {
            let group_dir = adj_dir.join(group.dir_name());
            check_part_chunk_dir(
                &group_dir,
                &group.file_type,
                opts,
                &mut issues,
                "edge property",
            );
        }
    }

    issues
}

/// Shared property-group sanity checks (schema-level, no IO).
fn check_property_groups(groups: &[PropertyGroup], owner: &str) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    let mut primaries = 0usize;
    let mut seen_names = std::collections::HashSet::new();

    for group in groups {
        if group.properties.is_empty() {
            issues.push(ValidationIssue::error(format!(
                "`{owner}` has a property group with no properties"
            )));
        }
        for p in &group.properties {
            if p.name.is_empty() {
                issues.push(ValidationIssue::error(format!(
                    "`{owner}` has a property with an empty name"
                )));
            }
            if !seen_names.insert(p.name.clone()) {
                issues.push(ValidationIssue::error(format!(
                    "`{owner}` declares property `{}` in more than one group; a property must \
                     belong to exactly one group",
                    p.name
                )));
            }
            if p.is_primary {
                primaries += 1;
            }
        }
    }

    if primaries > 1 {
        issues.push(ValidationIssue::error(format!(
            "`{owner}` declares {primaries} primary properties; at most one is allowed"
        )));
    }

    issues
}

/// Verify a flat chunk directory (`vertex/<label>/<group>/` or `.../offset/`)
/// exists and holds at least `chunk0`. Then confirm every `chunkN` it holds is
/// named per the spec and the numbering is gapless from 0.
fn check_chunk_dir(
    dir: &Path,
    file_type: &FileType,
    opts: &ValidateOptions,
    issues: &mut Vec<ValidationIssue>,
    what: &str,
) {
    if !dir.exists() {
        push_missing(dir, opts, issues, what);
        return;
    }
    let names = list_chunk_files(dir, issues);
    check_chunk_sequence(dir, &names, file_type, opts, issues, what);
}

/// Verify a partitioned directory (`.../adj_list/`, edge property groups) holds
/// `part0/chunk0`, and that every `partK/chunkN` is well-named and gapless.
fn check_part_chunk_dir(
    dir: &Path,
    file_type: &FileType,
    opts: &ValidateOptions,
    issues: &mut Vec<ValidationIssue>,
    what: &str,
) {
    if !dir.exists() {
        push_missing(dir, opts, issues, what);
        return;
    }
    // Collect the partN subdirectories.
    let mut parts: Vec<u64> = Vec::new();
    match std::fs::read_dir(dir) {
        Ok(rd) => {
            for entry in rd.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if let Some(rest) = name.strip_prefix("part") {
                    match rest.parse::<u64>() {
                        Ok(n) => parts.push(n),
                        Err(_) => issues.push(ValidationIssue::warn_at(
                            format!("{what}: unexpected entry `{name}` (not a partN directory)"),
                            entry.path(),
                        )),
                    }
                } else {
                    issues.push(ValidationIssue::warn_at(
                        format!("{what}: unexpected entry `{name}` (expected partN directories)"),
                        entry.path(),
                    ));
                }
            }
        }
        Err(e) => {
            issues.push(ValidationIssue::error_at(
                format!("{what}: cannot read directory: {e}"),
                dir,
            ));
            return;
        }
    }

    // Partitions are vertex-chunk-aligned: a vertex chunk with no incident
    // edges has no `partN` directory at all, so the partition numbering may be
    // sparse (start above part0, contain gaps) — that is spec-conformant
    // (verified against the upstream corpus, e.g. `modern_graph` whose only
    // partition is `part1`). We therefore require only that at least one
    // partition exists, then validate the chunk run inside each.
    if parts.is_empty() {
        push_missing(&dir.join("part0"), opts, issues, what);
        return;
    }
    parts.sort_unstable();

    for p in &parts {
        let part_dir = dir.join(format!("part{p}"));
        let names = list_chunk_files(&part_dir, issues);
        check_chunk_sequence(&part_dir, &names, file_type, opts, issues, what);
    }
}

fn push_missing(dir: &Path, opts: &ValidateOptions, issues: &mut Vec<ValidationIssue>, what: &str) {
    if opts.require_chunks {
        issues.push(ValidationIssue::error_at(
            format!("{what}: expected directory is missing"),
            dir,
        ));
    } else {
        issues.push(ValidationIssue::warn_at(
            format!("{what}: directory is missing"),
            dir,
        ));
    }
}

/// List the plain-file entries of a directory (the candidate chunk files).
fn list_chunk_files(dir: &Path, issues: &mut Vec<ValidationIssue>) -> Vec<String> {
    let mut names = Vec::new();
    match std::fs::read_dir(dir) {
        Ok(rd) => {
            for entry in rd.flatten() {
                if entry.path().is_file() {
                    names.push(entry.file_name().to_string_lossy().into_owned());
                }
            }
        }
        Err(e) => issues.push(ValidationIssue::error_at(
            format!("cannot read chunk directory: {e}"),
            dir,
        )),
    }
    names
}

/// Given the file names in a chunk directory, confirm they form a gapless
/// `chunk0..chunkN` run (each well-named for `file_type`) and at least chunk0
/// is present.
fn check_chunk_sequence(
    dir: &Path,
    names: &[String],
    file_type: &FileType,
    opts: &ValidateOptions,
    issues: &mut Vec<ValidationIssue>,
    what: &str,
) {
    // Extract chunk indices, flagging any file that isn't a spec chunk name.
    let mut indices: Vec<u64> = Vec::new();
    for name in names {
        match parse_chunk_index(name, file_type, opts) {
            Some(i) => indices.push(i),
            None => issues.push(ValidationIssue::warn_at(
                format!("{what}: unexpected file `{name}` (not a chunkN file)"),
                dir.join(name),
            )),
        }
    }

    if indices.is_empty() {
        if opts.require_chunks {
            issues.push(ValidationIssue::error_at(
                format!("{what}: no chunk files found"),
                dir,
            ));
        }
        return;
    }

    indices.sort_unstable();
    if indices[0] != 0 {
        issues.push(ValidationIssue::error_at(
            format!(
                "{what}: chunk numbering must start at chunk0, found chunk{}",
                indices[0]
            ),
            dir,
        ));
    }
    for (expected, got) in indices.iter().enumerate() {
        if *got != expected as u64 {
            issues.push(ValidationIssue::error_at(
                format!("{what}: gap in chunk numbering (expected chunk{expected})"),
                dir,
            ));
            break;
        }
    }
}

/// Parse the chunk index out of a file name, honoring the extension policy.
fn parse_chunk_index(name: &str, file_type: &FileType, opts: &ValidateOptions) -> Option<u64> {
    let with_ext = format!(".{}", file_type.extension());
    if let Some(rest) = name.strip_prefix("chunk") {
        if let Some(idx) = rest.strip_suffix(&with_ext) {
            return idx.parse::<u64>().ok();
        }
        if opts.extensionless_chunks {
            return rest.parse::<u64>().ok();
        }
    }
    None
}

impl GraphInfo {
    /// Structurally validate this graph and everything it references under
    /// `base` against the `gar/v1` spec. Returns the (possibly empty) list of
    /// issues; an empty list means the directory conforms.
    pub fn validate(&self, base: impl AsRef<Path>) -> Vec<ValidationIssue> {
        validate_graph_info(self, base, &ValidateOptions::default())
    }
}

impl VertexInfo {
    /// Structurally validate this vertex type's chunk layout under `base`.
    pub fn validate_dir(&self, base: impl AsRef<Path>) -> Vec<ValidationIssue> {
        validate_vertex_info(self, base, &ValidateOptions::default())
    }
}

impl EdgeInfo {
    /// Structurally validate this edge triple's chunk layout under `base`.
    pub fn validate_dir(&self, base: impl AsRef<Path>) -> Vec<ValidationIssue> {
        validate_edge_info(self, base, &ValidateOptions::default())
    }
}
