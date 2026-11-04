// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Chunk IO over [`object_store`] — S3, GCS, Azure, local filesystem, and any
//! S3-compatible endpoint (SeaweedFS, RustFS, MinIO, …).
//!
//! The format codecs all operate on in-memory bytes, so a chunk can live
//! anywhere `object_store` can reach. [`ChunkStore`] pairs a store with a
//! prefix and exposes async `read_chunk` / `write_chunk` mirroring
//! [`crate::io`].
//!
//! ```no_run
//! # async fn demo() -> graphar::Result<()> {
//! use graphar::{storage::ChunkStore, FileType};
//!
//! let store = ChunkStore::from_url("s3://my-bucket/graphs/social")?;
//! let batches = store.read_chunk("vertex/Person/id/chunk0.parquet", &FileType::Parquet).await?;
//! # Ok(())
//! # }
//! ```

use std::{io::Cursor, sync::Arc, time::Duration};

use arrow_array::RecordBatch;
use bytes::Bytes;
use object_store::{
    BackoffConfig, ObjectStore, ObjectStoreExt, ObjectStoreScheme, PutPayload, RetryConfig,
    aws::AmazonS3Builder, azure::MicrosoftAzureBuilder, gcp::GoogleCloudStorageBuilder,
    http::HttpBuilder, path::Path as StorePath,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use url::Url;

use crate::{
    error::{GraphArError, Result},
    types::FileType,
};

/// Retry / backoff policy for remote (`object_store`) operations.
///
/// Object-store backends — S3, GCS, Azure, and S3-compatible stores like
/// SeaweedFS / RustFS / MinIO — can fail transiently (throttling, dropped
/// connections, 5xx). `object_store` ships a built-in exponential-backoff
/// retry loop; `RetryOptions` is graphar's thin, typed front for it, so callers
/// don't have to depend on `object_store` directly. Convert into the native
/// [`object_store::RetryConfig`] via [`From`]/[`Into`].
///
/// The [`Default`] mirrors `object_store`'s own default (10 retries, 100 ms
/// initial backoff doubling up to 15 s, bounded by a 3-minute wall-clock
/// budget), so backends built through [`ChunkStore::from_url`] /
/// [`ChunkStore::s3_compatible`] retry out of the box exactly as the
/// underlying library already does. Use [`RetryOptions::none`] to opt out.
#[derive(Debug, Clone)]
pub struct RetryOptions {
    /// Maximum number of retries after the initial attempt. `0` disables retries.
    pub max_retries: usize,
    /// Backoff before the first retry.
    pub init_backoff: Duration,
    /// Upper bound on a single backoff interval.
    pub max_backoff: Duration,
    /// Wall-clock budget across all attempts; once exceeded no further retries
    /// are made. Keep below ~5 min so credentials/payloads stay valid.
    pub retry_timeout: Duration,
}

impl Default for RetryOptions {
    fn default() -> Self {
        // Matches object_store::RetryConfig / BackoffConfig defaults so the new
        // config plumbing preserves the library's existing retry behavior.
        Self {
            max_retries: 10,
            init_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(15),
            retry_timeout: Duration::from_secs(3 * 60),
        }
    }
}

impl RetryOptions {
    /// Disable retries entirely (single attempt per operation).
    pub fn none() -> Self {
        Self {
            max_retries: 0,
            ..Self::default()
        }
    }

    /// A bounded policy: at most `max_retries` retries with exponential backoff
    /// from `init_backoff` (doubling, base 2) capped at `max_backoff`.
    pub fn new(max_retries: usize, init_backoff: Duration, max_backoff: Duration) -> Self {
        Self {
            max_retries,
            init_backoff,
            max_backoff,
            ..Self::default()
        }
    }
}

impl From<RetryOptions> for RetryConfig {
    fn from(o: RetryOptions) -> Self {
        RetryConfig {
            backoff: BackoffConfig {
                init_backoff: o.init_backoff,
                max_backoff: o.max_backoff,
                base: 2.0,
            },
            max_retries: o.max_retries,
            retry_timeout: o.retry_timeout,
        }
    }
}

/// Default size above which a chunk is uploaded with `put_multipart` instead of
/// a single `put`: 16 MiB. Below it the round-trip overhead of initiating a
/// multipart upload (create → parts → complete) isn't worth it.
pub const DEFAULT_MULTIPART_THRESHOLD: usize = 16 * 1024 * 1024;

/// Default size of each multipart part: 8 MiB. Stays above the 5 MiB minimum
/// part size most object stores (S3, GCS, R2) require for all but the final
/// part, and keeps per-part buffering bounded.
pub const DEFAULT_MULTIPART_PART_SIZE: usize = 8 * 1024 * 1024;

/// Multipart-upload policy for [`ChunkStore`] writes.
///
/// `object_store` can stream a large object to a remote backend (S3, GCS,
/// Azure, S3-compatible stores) as a sequence of fixed-size parts via
/// `ObjectStore::put_multipart`, which the store assembles into one object on
/// `complete()`. This sidesteps holding/uploading one huge request body and is
/// the standard path for big objects.
///
/// `ChunkStore` switches a write to multipart when the *encoded* chunk exceeds
/// [`threshold`](Self::threshold) bytes, uploading it in
/// [`part_size`](Self::part_size)-byte parts; smaller chunks keep the single
/// `put`. The [`Default`] (16 MiB threshold, 8 MiB parts) keeps small writes —
/// and effectively all local / in-memory traffic — on the single-`put` path,
/// since multipart is a remote concern.
#[derive(Debug, Clone, Copy)]
pub struct MultipartOptions {
    /// Encoded-chunk size (bytes) at or below which a single `put` is used.
    /// Writes strictly larger than this go through `put_multipart`.
    pub threshold: usize,
    /// Size (bytes) of each multipart part. Keep ≥ 5 MiB for S3/GCS/R2
    /// compatibility (all parts but the last must meet the store's minimum).
    pub part_size: usize,
}

impl Default for MultipartOptions {
    fn default() -> Self {
        Self {
            threshold: DEFAULT_MULTIPART_THRESHOLD,
            part_size: DEFAULT_MULTIPART_PART_SIZE,
        }
    }
}

impl MultipartOptions {
    /// Explicit threshold + part size. `part_size` is clamped to at least 1 to
    /// avoid a zero-length part loop.
    pub fn new(threshold: usize, part_size: usize) -> Self {
        Self {
            threshold,
            part_size: part_size.max(1),
        }
    }
}

/// Decode a chunk held in memory.
pub fn read_chunk_bytes(bytes: Bytes, file_type: &FileType) -> Result<Vec<RecordBatch>> {
    match file_type {
        FileType::Parquet => {
            let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;
            reader.map(|b| Ok(b?)).collect()
        }
        FileType::Csv => {
            let format = arrow_csv::reader::Format::default().with_header(true);
            let (schema, _) = format.infer_schema(Cursor::new(&bytes), Some(100))?;
            let reader = arrow_csv::ReaderBuilder::new(Arc::new(schema))
                .with_header(true)
                .build(Cursor::new(&bytes))?;
            reader.map(|b| Ok(b?)).collect()
        }
        FileType::Json => {
            let mut cursor = Cursor::new(&bytes);
            let (schema, _) = arrow::json::reader::infer_json_schema(&mut cursor, None)?;
            cursor.set_position(0);
            let reader = arrow::json::ReaderBuilder::new(Arc::new(schema)).build(cursor)?;
            reader.map(|b| Ok(b?)).collect()
        }
        #[cfg(feature = "orc")]
        FileType::Orc => {
            let reader = orc_rust::ArrowReaderBuilder::try_new(bytes)?.build();
            reader.map(|b| Ok(b?)).collect()
        }
        #[cfg(not(feature = "orc"))]
        FileType::Orc => Err(crate::error::GraphArError::from(std::io::Error::other(
            "graphar was built without the `orc` feature — ORC is not compiled in. \
             Rebuild with `--features orc`.",
        ))),
        FileType::ArrowIpc => {
            let reader = arrow::ipc::reader::FileReader::try_new(Cursor::new(bytes), None)?;
            reader.map(|b| Ok(b?)).collect()
        }
    }
}

/// Encode batches into the on-disk representation of `file_type`.
pub fn write_chunk_bytes(batches: &[RecordBatch], file_type: &FileType) -> Result<Vec<u8>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let schema = batches[0].schema();
    let mut out = Vec::new();
    match file_type {
        FileType::Parquet => {
            let mut writer = parquet::arrow::ArrowWriter::try_new(&mut out, schema, None)?;
            for batch in batches {
                writer.write(batch)?;
            }
            writer.close()?;
        }
        FileType::Csv => {
            let mut writer = arrow_csv::WriterBuilder::new()
                .with_header(true)
                .build(&mut out);
            for batch in batches {
                writer.write(batch)?;
            }
        }
        FileType::Json => {
            let mut writer = arrow::json::LineDelimitedWriter::new(&mut out);
            writer.write_batches(&batches.iter().collect::<Vec<_>>())?;
            writer.finish()?;
        }
        #[cfg(feature = "orc")]
        FileType::Orc => {
            let mut writer = orc_rust::ArrowWriterBuilder::new(&mut out, schema).try_build()?;
            for batch in batches {
                writer.write(batch)?;
            }
            writer.close()?;
        }
        #[cfg(not(feature = "orc"))]
        FileType::Orc => {
            return Err(crate::error::GraphArError::from(std::io::Error::other(
                "graphar was built without the `orc` feature — ORC is not compiled in. \
                 Rebuild with `--features orc`.",
            )));
        }
        FileType::ArrowIpc => {
            let mut writer = arrow::ipc::writer::FileWriter::try_new(&mut out, &schema)?;
            for batch in batches {
                writer.write(batch)?;
            }
            writer.finish()?;
        }
    }
    Ok(out)
}

/// A GraphAr chunk store rooted at a prefix inside any `object_store` backend.
#[derive(Clone)]
pub struct ChunkStore {
    store: Arc<dyn ObjectStore>,
    prefix: StorePath,
    multipart: MultipartOptions,
}

impl ChunkStore {
    /// Wrap an existing `object_store` instance.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<StorePath>) -> Self {
        Self {
            store,
            prefix: prefix.into(),
            multipart: MultipartOptions::default(),
        }
    }

    /// Override the [multipart-upload policy](MultipartOptions) (builder style).
    ///
    /// Large chunks are uploaded with `put_multipart` once their encoded size
    /// exceeds `opts.threshold`; this tunes that threshold and the per-part
    /// size. Returns `self` for chaining off any constructor.
    pub fn with_multipart(mut self, opts: MultipartOptions) -> Self {
        self.multipart = opts;
        self
    }

    /// The active [multipart-upload policy](MultipartOptions).
    pub fn multipart_options(&self) -> MultipartOptions {
        self.multipart
    }

    /// Build from a URL: `s3://bucket/prefix`, `gs://bucket/prefix`,
    /// `az://container/prefix`, `file:///abs/path`, `http(s)://…`.
    ///
    /// Credentials come from the environment (e.g. `AWS_ACCESS_KEY_ID`,
    /// `GOOGLE_SERVICE_ACCOUNT`, …) as resolved by `object_store`. Remote
    /// backends use the [default retry policy](RetryOptions::default); call
    /// [`from_url_with_retry`](Self::from_url_with_retry) to override it.
    pub fn from_url(url: &str) -> Result<Self> {
        Self::from_url_with_retry(url, RetryOptions::default())
    }

    /// Like [`from_url`](Self::from_url), but with an explicit retry/backoff
    /// policy applied to the remote (cloud / S3-compatible) backend.
    ///
    /// The cloud builders (`AmazonS3Builder`, `GoogleCloudStorageBuilder`,
    /// `MicrosoftAzureBuilder`, `HttpBuilder`) take a typed
    /// [`object_store::RetryConfig`], which `parse_url_opts` cannot thread
    /// through, so this dispatches on the URL scheme and applies `with_retry`
    /// directly while still honoring the same `AWS_*` / `GOOGLE_*` / `AZURE_*`
    /// environment configuration. Local / in-memory backends ignore `retry`.
    pub fn from_url_with_retry(url: &str, retry: RetryOptions) -> Result<Self> {
        let url = Url::parse(url).map_err(|e| GraphArError::Other(e.to_string()))?;
        let (scheme, prefix) = ObjectStoreScheme::parse(&url)
            .map_err(|e| GraphArError::Other(format!("unrecognised object-store url: {e}")))?;
        let retry: RetryConfig = retry.into();

        // Same env filter as before: pass cloud credentials through as config
        // keys (object_store lowercases + parses them, ignoring unknown ones).
        let env = || {
            std::env::vars().filter(|(k, _)| {
                k.starts_with("AWS_") || k.starts_with("GOOGLE_") || k.starts_with("AZURE_")
            })
        };

        let store: Arc<dyn ObjectStore> = match scheme {
            ObjectStoreScheme::AmazonS3 => {
                let mut b = AmazonS3Builder::new()
                    .with_url(url.as_str())
                    .with_retry(retry);
                for (k, v) in env() {
                    if let Ok(key) = k.to_ascii_lowercase().parse() {
                        b = b.with_config(key, v);
                    }
                }
                Arc::new(b.build()?)
            }
            ObjectStoreScheme::GoogleCloudStorage => {
                let mut b = GoogleCloudStorageBuilder::new()
                    .with_url(url.as_str())
                    .with_retry(retry);
                for (k, v) in env() {
                    if let Ok(key) = k.to_ascii_lowercase().parse() {
                        b = b.with_config(key, v);
                    }
                }
                Arc::new(b.build()?)
            }
            ObjectStoreScheme::MicrosoftAzure => {
                let mut b = MicrosoftAzureBuilder::new()
                    .with_url(url.as_str())
                    .with_retry(retry);
                for (k, v) in env() {
                    if let Ok(key) = k.to_ascii_lowercase().parse() {
                        b = b.with_config(key, v);
                    }
                }
                Arc::new(b.build()?)
            }
            ObjectStoreScheme::Http => {
                let base = &url[..url::Position::BeforePath];
                let mut b = HttpBuilder::new()
                    .with_url(base.to_string())
                    .with_retry(retry);
                for (k, v) in env() {
                    if let Ok(key) = k.to_ascii_lowercase().parse() {
                        b = b.with_config(key, v);
                    }
                }
                Arc::new(b.build()?)
            }
            // Local / in-memory: no network, retry is a no-op. Fall back to the
            // library's own constructor so URL parsing stays consistent.
            _ => {
                let (store, _) = object_store::parse_url(&url)?;
                Arc::from(store)
            }
        };
        Ok(Self {
            store,
            prefix,
            multipart: MultipartOptions::default(),
        })
    }

    /// Local filesystem store rooted at `root`.
    pub fn local(root: impl AsRef<std::path::Path>) -> Result<Self> {
        let store = object_store::local::LocalFileSystem::new_with_prefix(root)?;
        Ok(Self {
            store: Arc::new(store),
            prefix: StorePath::default(),
            multipart: MultipartOptions::default(),
        })
    }

    /// S3-compatible endpoint with explicit credentials — SeaweedFS, RustFS,
    /// MinIO, Ceph RGW, …. `allow_http` is enabled because these usually run
    /// without TLS inside a private network.
    ///
    /// Uses the [default retry policy](RetryOptions::default); call
    /// [`s3_compatible_with_retry`](Self::s3_compatible_with_retry) to override.
    pub fn s3_compatible(
        endpoint: &str,
        bucket: &str,
        access_key: &str,
        secret_key: &str,
        prefix: &str,
    ) -> Result<Self> {
        Self::s3_compatible_with_retry(
            endpoint,
            bucket,
            access_key,
            secret_key,
            prefix,
            RetryOptions::default(),
        )
    }

    /// [`s3_compatible`](Self::s3_compatible) with an explicit retry/backoff
    /// policy — transient failures against SeaweedFS / RustFS / MinIO are
    /// retried with exponential backoff as configured by `retry`.
    pub fn s3_compatible_with_retry(
        endpoint: &str,
        bucket: &str,
        access_key: &str,
        secret_key: &str,
        prefix: &str,
        retry: RetryOptions,
    ) -> Result<Self> {
        let store = AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_bucket_name(bucket)
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key)
            .with_region("us-east-1")
            .with_allow_http(true)
            // path-style addressing: bucket in the path, not the hostname
            .with_virtual_hosted_style_request(false)
            .with_retry(retry.into())
            .build()?;
        Ok(Self {
            store: Arc::new(store),
            prefix: StorePath::from(prefix),
            multipart: MultipartOptions::default(),
        })
    }

    fn full_path(&self, rel: &str) -> StorePath {
        if self.prefix.as_ref().is_empty() {
            StorePath::from(rel)
        } else {
            StorePath::from(format!("{}/{}", self.prefix, rel))
        }
    }

    /// Fetch and decode one chunk.
    pub async fn read_chunk(&self, rel: &str, file_type: &FileType) -> Result<Vec<RecordBatch>> {
        let bytes = self.store.get(&self.full_path(rel)).await?.bytes().await?;
        read_chunk_bytes(bytes, file_type)
    }

    /// Encode and upload one chunk.
    ///
    /// Chunks larger than the [multipart threshold](MultipartOptions::threshold)
    /// stream to the backend via `put_multipart`; smaller ones use a single
    /// `put`.
    pub async fn write_chunk(
        &self,
        rel: &str,
        batches: &[RecordBatch],
        file_type: &FileType,
    ) -> Result<()> {
        let payload = write_chunk_bytes(batches, file_type)?;
        self.put_object(rel, payload).await
    }

    /// Raw byte fetch (info YAML files, offsets, …).
    pub async fn get_bytes(&self, rel: &str) -> Result<Bytes> {
        Ok(self.store.get(&self.full_path(rel)).await?.bytes().await?)
    }

    /// Raw byte upload. Honors the same multipart threshold as
    /// [`write_chunk`](Self::write_chunk).
    pub async fn put_bytes(&self, rel: &str, bytes: Vec<u8>) -> Result<()> {
        self.put_object(rel, bytes).await
    }

    /// Upload `bytes` to `rel`, choosing single-`put` vs `put_multipart` by the
    /// [multipart policy](MultipartOptions).
    ///
    /// For the multipart path the encoded bytes are sliced into
    /// `part_size`-byte parts and each is uploaded with `put_part`; `complete`
    /// then assembles them atomically. Parts are uploaded sequentially: chunk
    /// writes are already parallelised at a higher level, and sequential parts
    /// keep peak memory at one part plus the source buffer. On any error the
    /// in-flight upload is aborted so backends without drop-cleanup (S3, GCS)
    /// don't leak parts.
    async fn put_object(&self, rel: &str, bytes: Vec<u8>) -> Result<()> {
        let path = self.full_path(rel);
        if bytes.len() <= self.multipart.threshold {
            self.store.put(&path, bytes.into()).await?;
            return Ok(());
        }

        let part_size = self.multipart.part_size.max(1);
        let mut upload = self.store.put_multipart(&path).await?;
        let mut offset = 0;
        while offset < bytes.len() {
            let end = (offset + part_size).min(bytes.len());
            let part = PutPayload::from(bytes[offset..end].to_vec());
            if let Err(e) = upload.put_part(part).await {
                // Best-effort cleanup; surface the original write error.
                let _ = upload.abort().await;
                return Err(e.into());
            }
            offset = end;
        }
        if let Err(e) = upload.complete().await {
            let _ = upload.abort().await;
            return Err(e.into());
        }
        Ok(())
    }
}
