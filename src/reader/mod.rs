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
/// Chunk decoding is CPU-bound and chunks are independent, so this is a plain
/// scoped fan-out: thread t takes the indices `t, t + threads, ...`. No pool,
/// no work stealing, nothing shared but the input. Errors are returned in
/// index order too (the first failing index wins), after all threads joined.
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
    // One slot per index; thread t fills the slots t, t + threads, ...
    let mut slots: Vec<Slot<T>> = (0..n).map(|_| None).collect();
    std::thread::scope(|scope| {
        let mut lanes: Vec<Vec<(usize, &mut Slot<T>)>> = (0..threads).map(|_| Vec::new()).collect();
        for (i, slot) in slots.iter_mut().enumerate() {
            lanes[i % threads].push((i, slot));
        }
        for lane in lanes {
            let f = &f;
            scope.spawn(move || {
                for (i, slot) in lane {
                    *slot = Some(f(i));
                }
            });
        }
    });
    slots
        .into_iter()
        .map(|s| s.expect("every slot filled"))
        .collect()
}

type Slot<T> = Option<crate::Result<T>>;
