// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
//
//! Microbenchmarks for the inner coalesce pass in `RepartitionStrategy`.
//!
//! The DuckLake write path runs two repartition passes. The outer one slices
//! columns into fixed row blocks (`canonicalize: false`). The inner one, with
//! `canonicalize: true`, is what pays the price: it canonicalizes each
//! incoming chunk, slices it, accumulates the slices until the byte target is
//! reached, and then concatenates them.
//!
//! `repartition.rs` does that in two copies:
//!
//!   1. `chunk.execute::<Canonical>()` on every incoming chunk (line ~114)
//!   2. `ChunkedArray::into_array().execute::<Canonical>()` on the accumulated
//!      slices (line ~155), which for more than one chunk runs
//!      `append_to_builder` over the whole block
//!
//! Step 1 is what makes `nbytes()` mean *uncompressed* size, which
//! `ChunksBuffer::have_enough()` needs to place block boundaries. So step 1
//! cannot simply be dropped: on a non-canonical chunk `nbytes()` is the encoded
//! size, blocks would overshoot the target, and segments would grow.
//!
//! These benches measure both orders on identical input so the redundant copy
//! is quantified rather than assumed. `two_pass` is what ships; `one_pass` is
//! the shape an optimization would take if canonical size could be accounted
//! without materializing.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_lossless,
    clippy::unwrap_used,
    clippy::expect_used
)]

use divan::Bencher;
use vortex_array::ArrayRef;
use vortex_array::Canonical;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::VarBinArray;
use vortex_array::arrays::chunked::ChunkedArray;
use vortex_array::dtype::DType;
use vortex_array::dtype::Nullability;
use vortex_session::VortexSession;

/// Row block width used by the outer repartition pass.
const BLOCK_LEN: usize = 8192;

/// Build a column shaped like TPC-H `l_comment`: variable-length strings with
/// heavy token overlap, which is what the string path actually sees.
fn l_comment(n: usize) -> VarBinArray {
    let clauses: &[&str] = &[
        "regular courts above the",
        "requests. blithely final packages? blithely final packages are carefully",
        "after the quietly ironic packages. blithely ironic",
        "the final requests are carefully final; the final",
        "pending packages use the final, final courts. final packages are",
    ];
    let nouns: &[&str] = &["foxes", "ideas", "dependencies", "excuses", "packages"];
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state
    };
    let strings: Vec<String> = (0..n)
        .map(|_| {
            let s = next();
            format!(
                "{} {} {}. {} {}",
                clauses[(s as usize) % clauses.len()],
                nouns[((s >> 8) as usize) % nouns.len()],
                nouns[((s >> 16) as usize) % nouns.len()],
                clauses[((s >> 24) as usize) % clauses.len()],
                nouns[((s >> 32) as usize) % nouns.len()],
            )
        })
        .collect();
    VarBinArray::from_iter(
        strings.iter().map(|s| Some(s.as_bytes())),
        DType::Utf8(Nullability::NonNullable),
    )
}

/// A fixed-width column, the cheap case for the same pass.
fn lineitem_key(n: usize) -> PrimitiveArray {
    PrimitiveArray::new(
        vortex_buffer::Buffer::from((0..n as u64).map(|i| i * 7_919).collect::<Vec<u64>>()),
        vortex_array::validity::Validity::NonNullable,
    )
}

fn blocks(array: &ArrayRef, ctx: &mut ExecutionCtx) -> Vec<ArrayRef> {
    let mut out = Vec::new();
    let mut offset = 0;
    while offset < array.len() {
        let end = (offset + BLOCK_LEN).min(array.len());
        out.push(
            array
                .slice(offset..end)
                .unwrap_or_else(|e| panic!("slice failed: {e}")),
        );
        offset = end;
    }
    out
}

fn session() -> VortexSession {
    vortex_array::array_session()
}

/// Canonicalize every block, then concatenate — what `repartition.rs` does today.
#[divan::bench]
fn two_pass_l_comment(bencher: Bencher) {
    let s = session();
    let mut ctx = s.create_execution_ctx();
    let array: ArrayRef = l_comment(200_000).into_array();
    let src = blocks(&array, &mut ctx);
    bencher.bench_local(|| {
        let mut ctx = s.create_execution_ctx();
        let mut canonical_blocks = Vec::with_capacity(src.len());
        for b in &src {
            canonical_blocks.push(
                b.clone()
                    .execute::<Canonical>(&mut ctx)
                    .unwrap_or_else(|e| panic!("canonicalize failed: {e}"))
                    .into_array(),
            );
        }
        let chunked = ChunkedArray::try_new(canonical_blocks, array.dtype().clone())
            .unwrap_or_else(|e| panic!("chunked failed: {e}"));
        let out = chunked
            .into_array()
            .execute::<Canonical>(&mut ctx)
            .unwrap_or_else(|e| panic!("canonicalize concat failed: {e}"))
            .into_array();
        divan::black_box(out)
    });
}

/// Canonicalize every block, then concatenate — what `repartition.rs` does today.
#[divan::bench]
fn two_pass_i64(bencher: Bencher) {
    let s = session();
    let mut ctx = s.create_execution_ctx();
    let array: ArrayRef = lineitem_key(200_000).into_array();
    let src = blocks(&array, &mut ctx);
    bencher.bench_local(|| {
        let mut ctx = s.create_execution_ctx();
        let mut canonical_blocks = Vec::with_capacity(src.len());
        for b in &src {
            canonical_blocks.push(
                b.clone()
                    .execute::<Canonical>(&mut ctx)
                    .unwrap_or_else(|e| panic!("canonicalize failed: {e}"))
                    .into_array(),
            );
        }
        let chunked = ChunkedArray::try_new(canonical_blocks, array.dtype().clone())
            .unwrap_or_else(|e| panic!("chunked failed: {e}"));
        let out = chunked
            .into_array()
            .execute::<Canonical>(&mut ctx)
            .unwrap_or_else(|e| panic!("canonicalize concat failed: {e}"))
            .into_array();
        divan::black_box(out)
    });
}

/// Concatenate the original blocks and canonicalize once — the shape an
/// optimization would take, if canonical size could be accounted for without
/// materializing the first pass.
#[divan::bench]
fn one_pass_l_comment(bencher: Bencher) {
    let s = session();
    let mut ctx = s.create_execution_ctx();
    let array: ArrayRef = l_comment(200_000).into_array();
    let src = blocks(&array, &mut ctx);
    bencher.bench_local(|| {
        let mut ctx = s.create_execution_ctx();
        let chunked = ChunkedArray::try_new(src.clone(), array.dtype().clone())
            .unwrap_or_else(|e| panic!("chunked failed: {e}"));
        let out = chunked
            .into_array()
            .execute::<Canonical>(&mut ctx)
            .unwrap_or_else(|e| panic!("canonicalize concat failed: {e}"))
            .into_array();
        divan::black_box(out)
    });
}

/// Concatenate the original blocks and canonicalize once — the shape an
/// optimization would take, if canonical size could be accounted for without
/// materializing the first pass.
#[divan::bench]
fn one_pass_i64(bencher: Bencher) {
    let s = session();
    let mut ctx = s.create_execution_ctx();
    let array: ArrayRef = lineitem_key(200_000).into_array();
    let src = blocks(&array, &mut ctx);
    bencher.bench_local(|| {
        let mut ctx = s.create_execution_ctx();
        let chunked = ChunkedArray::try_new(src.clone(), array.dtype().clone())
            .unwrap_or_else(|e| panic!("chunked failed: {e}"));
        let out = chunked
            .into_array()
            .execute::<Canonical>(&mut ctx)
            .unwrap_or_else(|e| panic!("canonicalize concat failed: {e}"))
            .into_array();
        divan::black_box(out)
    });
}

/// Size accounting only: what `ChunksBuffer::have_enough()` reads to place block
/// boundaries. This is why the first pass cannot simply be dropped — it is what
/// makes `nbytes()` the uncompressed size.
#[divan::bench]
fn canonical_nbytes_accounting(bencher: Bencher) {
    let s = session();
    let mut ctx = s.create_execution_ctx();
    let array: ArrayRef = l_comment(200_000).into_array();
    let src = blocks(&array, &mut ctx);
    let raw_bytes: u64 = src.iter().map(|b| b.nbytes()).sum();
    bencher.bench_local(|| {
        let mut ctx = s.create_execution_ctx();
        let mut total = 0u64;
        for b in &src {
            let canonical = b
                .clone()
                .execute::<Canonical>(&mut ctx)
                .unwrap_or_else(|e| panic!("canonicalize failed: {e}"))
                .into_array();
            total += canonical.nbytes();
        }
        divan::black_box(total)
    });
    println!("raw nbytes total = {raw_bytes}, rows = {}", array.len());
}

fn main() {
    divan::main();
}
