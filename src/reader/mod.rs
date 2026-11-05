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

pub mod edge_reader;
pub mod vertex_reader;

pub use edge_reader::EdgeReader;
pub use vertex_reader::{VertexChunkIter, VertexReader};

/// Default cap on decode threads: chunk decoding is memory-bound (fresh
/// buffers per chunk), and measured gains flatten past 4-8 threads while the
/// peak RSS keeps growing. `with_threads` on the readers overrides it.
pub const DEFAULT_MAX_THREADS: usize = 8;

/// Run `f` over `0..n` on up to `threads` std threads (default: the host's
/// available parallelism capped at [`DEFAULT_MAX_THREADS`], and at `n`),
/// results in index order.
///
/// Chunk decoding is CPU-bound and chunks are independent, but not equal:
/// string chunks and the last, short chunk of each partition differ in cost.
/// So the threads take the next index from a shared counter instead of a
/// fixed stride; the run ends at max over threads of the work they took,
/// which with dynamic claiming is at most (total / T) + one chunk. No pool, no
/// work stealing, nothing shared but the counter and the input. Errors come
/// back in index order too (the first failing index wins), after all threads
/// joined.
pub(crate) fn par_map<T: Send, F>(n: usize, threads: Option<usize>, f: F) -> crate::Result<Vec<T>>
where
    F: Fn(usize) -> crate::Result<T> + Sync,
{
    let threads = threads
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1)
                .min(DEFAULT_MAX_THREADS)
        })
        .clamp(1, n.max(1));
    if threads == 1 || n <= 1 {
        return (0..n).map(&f).collect();
    }
    let next = std::sync::atomic::AtomicUsize::new(0);
    let slots: Vec<std::sync::Mutex<Slot<T>>> =
        (0..n).map(|_| std::sync::Mutex::new(None)).collect();
    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                loop {
                    let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if i >= n {
                        break;
                    }
                    let r = f(i);
                    *slots[i].lock().unwrap_or_else(|e| e.into_inner()) = Some(r);
                }
            });
        }
    });
    slots
        .into_iter()
        .map(|s| {
            s.into_inner()
                .unwrap_or_else(|e| e.into_inner())
                .expect("every slot filled")
        })
        .collect()
}

type Slot<T> = Option<crate::Result<T>>;
