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

//! ChunkStore against live S3-compatible object stores: SeaweedFS and RustFS,
//! both run as Podman containers via testcontainers.
//!
//! Run:
//! ```bash
//! cargo test -p graphar --test integration_s3 -- --ignored --nocapture
//! ```

use std::{sync::Arc, time::Duration};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use graphar::{FileType, storage::ChunkStore};
use testcontainers::{
    GenericImage, ImageExt,
    core::{ContainerPort, ExecCommand, WaitFor},
    runners::AsyncRunner,
};

fn ensure_docker_host() {
    if std::env::var("DOCKER_HOST").is_err() {
        let uid = std::process::Command::new("id")
            .arg("-u")
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .and_then(|s| s.trim().parse::<u32>().ok())
            .unwrap_or(1000);
        let sock = format!("/run/user/{uid}/podman/podman.sock");
        if std::path::Path::new(&sock).exists() {
            unsafe { std::env::set_var("DOCKER_HOST", format!("unix://{sock}")) };
        }
    }
}

fn sample_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap()
}

async fn round_trip_all_formats(store: &ChunkStore) {
    for ft in [
        FileType::Parquet,
        FileType::Csv,
        FileType::Json,
        #[cfg(feature = "orc")]
        FileType::Orc,
    ] {
        let rel = format!("vertex/person/id/chunk0.{}", ft.extension());
        store
            .write_chunk(&rel, &[sample_batch()], &ft)
            .await
            .unwrap_or_else(|e| panic!("write {ft:?}: {e}"));
        let batches = store
            .read_chunk(&rel, &ft)
            .await
            .unwrap_or_else(|e| panic!("read {ft:?}: {e}"));
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 3, "{ft:?}");
    }
}

#[tokio::test]
#[ignore = "needs podman; run with --ignored"]
async fn chunk_store_against_seaweedfs() {
    ensure_docker_host();

    let container = GenericImage::new("docker.io/chrislusf/seaweedfs", "latest")
        .with_exposed_port(ContainerPort::Tcp(8333))
        .with_wait_for(WaitFor::message_on_stderr("Starting S3 API Server"))
        .with_cmd(["server", "-s3", "-s3.port=8333"])
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .expect("start seaweedfs");
    let port = container.get_host_port_ipv4(8333).await.unwrap();

    // Create the bucket through weed shell (no CreateBucket in object_store).
    let mut exec = container
        .exec(ExecCommand::new([
            "sh",
            "-c",
            "echo 's3.bucket.create -name graphar' | weed shell",
        ]))
        .await
        .expect("weed shell bucket create");
    let _ = exec.stdout_to_vec().await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // SeaweedFS without an identity config accepts any key pair.
    let store = ChunkStore::s3_compatible(
        &format!("http://127.0.0.1:{port}"),
        "graphar",
        "any",
        "any",
        "graphs/itest",
    )
    .unwrap();
    round_trip_all_formats(&store).await;
}

#[tokio::test]
#[ignore = "needs podman; run with --ignored"]
async fn chunk_store_against_rustfs() {
    ensure_docker_host();

    let container = GenericImage::new("docker.io/rustfs/rustfs", "latest")
        .with_exposed_port(ContainerPort::Tcp(9000))
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .expect("start rustfs");
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // CreateBucket via the S3 API. object_store has no bucket admin calls,
    // but the image ships curl with SigV4 support. Default creds are
    // rustfsadmin/rustfsadmin.
    let mut exec = container
        .exec(ExecCommand::new([
            "curl",
            "-s",
            "-X",
            "PUT",
            "--user",
            "rustfsadmin:rustfsadmin",
            "--aws-sigv4",
            "aws:amz:us-east-1:s3",
            "http://127.0.0.1:9000/graphar",
        ]))
        .await
        .expect("create bucket via signed PUT");
    let _ = exec.stdout_to_vec().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let store = ChunkStore::s3_compatible(
        &format!("http://127.0.0.1:{port}"),
        "graphar",
        "rustfsadmin",
        "rustfsadmin",
        "graphs/itest",
    )
    .unwrap();
    round_trip_all_formats(&store).await;
}
