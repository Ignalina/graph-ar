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

use std::{io::Cursor, sync::Arc};

use arrow_array::RecordBatch;
use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, aws::AmazonS3Builder, path::Path as StorePath};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use url::Url;

use crate::{
    error::{GraphArError, Result},
    types::FileType,
};

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
        FileType::Orc => {
            let reader = orc_rust::ArrowReaderBuilder::try_new(bytes)?.build();
            reader.map(|b| Ok(b?)).collect()
        }
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
        FileType::Orc => {
            let mut writer = orc_rust::ArrowWriterBuilder::new(&mut out, schema).try_build()?;
            for batch in batches {
                writer.write(batch)?;
            }
            writer.close()?;
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
}

impl ChunkStore {
    /// Wrap an existing `object_store` instance.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<StorePath>) -> Self {
        Self { store, prefix: prefix.into() }
    }

    /// Build from a URL: `s3://bucket/prefix`, `gs://bucket/prefix`,
    /// `az://container/prefix`, `file:///abs/path`, `http(s)://…`.
    ///
    /// Credentials come from the environment (e.g. `AWS_ACCESS_KEY_ID`,
    /// `GOOGLE_SERVICE_ACCOUNT`, …) as resolved by `object_store`.
    pub fn from_url(url: &str) -> Result<Self> {
        let url = Url::parse(url).map_err(|e| GraphArError::Other(e.to_string()))?;
        let (store, prefix) = object_store::parse_url_opts(
            &url,
            std::env::vars().filter(|(k, _)| {
                k.starts_with("AWS_") || k.starts_with("GOOGLE_") || k.starts_with("AZURE_")
            }),
        )?;
        Ok(Self { store: Arc::from(store), prefix })
    }

    /// Local filesystem store rooted at `root`.
    pub fn local(root: impl AsRef<std::path::Path>) -> Result<Self> {
        let store = object_store::local::LocalFileSystem::new_with_prefix(root)?;
        Ok(Self { store: Arc::new(store), prefix: StorePath::default() })
    }

    /// S3-compatible endpoint with explicit credentials — SeaweedFS, RustFS,
    /// MinIO, Ceph RGW, …. `allow_http` is enabled because these usually run
    /// without TLS inside a private network.
    pub fn s3_compatible(
        endpoint: &str,
        bucket: &str,
        access_key: &str,
        secret_key: &str,
        prefix: &str,
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
            .build()?;
        Ok(Self { store: Arc::new(store), prefix: StorePath::from(prefix) })
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
    pub async fn write_chunk(
        &self,
        rel: &str,
        batches: &[RecordBatch],
        file_type: &FileType,
    ) -> Result<()> {
        let payload = write_chunk_bytes(batches, file_type)?;
        self.store
            .put(&self.full_path(rel), payload.into())
            .await?;
        Ok(())
    }

    /// Raw byte fetch (info YAML files, offsets, …).
    pub async fn get_bytes(&self, rel: &str) -> Result<Bytes> {
        Ok(self.store.get(&self.full_path(rel)).await?.bytes().await?)
    }

    /// Raw byte upload.
    pub async fn put_bytes(&self, rel: &str, bytes: Vec<u8>) -> Result<()> {
        self.store.put(&self.full_path(rel), bytes.into()).await?;
        Ok(())
    }
}
