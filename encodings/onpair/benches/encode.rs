// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
//
//! Encode-path microbenchmarks for the OnPair Vortex array.
//!
//! The matcher rewrite in `fast_lpm.rs` is the largest single codec CPU item
//! on the DuckLake write path (~770ms/insert on SF1 `l_comment` before the
//! change), so this bench exists to keep it honest.
//!
//! * `matcher_upstream` / `matcher_fast_lpm` — the A/B that matters. Both
//!   tokenize the *same* rows against the *same* trained dictionary, so the
//!   only difference measured is the match loop itself. Training is hoisted
//!   out of the timed region.
//! * `bucket_width_census` — not a timing bench. Reports how many long tokens
//!   share each 8-byte prefix, because the upstream matcher keeps small
//!   buckets on a linear scan and only promotes to binary search past 48
//!   entries, while `FastLpm` always binary-searches. If most buckets are
//!   narrow, that difference is a live question rather than a settled one.
//!
//! `LComment` is the shape that actually showed up in the profile; the others
//! bracket it.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_lossless,
    clippy::panic,
    clippy::tests_outside_test_module,
    clippy::redundant_clone,
    clippy::unwrap_used,
    clippy::expect_used
)]

use divan::Bencher;
use onpair::Config;
use onpair::Dictionary;
use onpair::DictionaryView;
use onpair::Parser;
use onpair::Rows;
use vortex_onpair::DEFAULT_CONFIG;
use vortex_onpair::fast_lpm;
use vortex_onpair::fast_lpm::FastLpm;
use vortex_utils::aliases::hash_map::HashMap;

/// [`Rows`] view straight over the corpus, so no intermediate `&[&[u8]]` has
/// to outlive the borrow.
struct StringRows<'a> {
    strings: &'a [String],
    total: usize,
}

impl Rows for StringRows<'_> {
    fn num_rows(&self) -> usize {
        self.strings.len()
    }

    fn total_bytes(&self) -> usize {
        self.total
    }

    fn row(&self, i: usize) -> &[u8] {
        self.strings[i].as_bytes()
    }
}

#[derive(Copy, Clone, Debug)]
enum Shape {
    /// SF1 `l_comment` shape — the profiled hot column. Long natural-language
    /// strings with high token overlap.
    LComment,
    /// URL / HTTP-log shaped — high lexical overlap, ~35-45 bytes per row.
    UrlLog,
    /// Short uniform strings, 4-8 bytes, very low cardinality.
    Short,
    /// High cardinality, every row unique. Worst case for any dictionary.
    HighCard,
}

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Shape::LComment => "l_comment",
            Shape::UrlLog => "url_log",
            Shape::Short => "short",
            Shape::HighCard => "high_card",
        }
    }
}

fn rng(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state
}

/// SF1 `l_comment` is built from a small pool of clause templates with varying
/// filler, so tokens repeat heavily across rows — which is what makes OnPair
/// worth reaching for, and the case where the short-token mask should pay off.
fn corpus(n: usize, shape: Shape) -> Vec<String> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut out = Vec::with_capacity(n);
    match shape {
        Shape::LComment => {
            let clauses: &[&str] = &[
                "regular courts above the",
                "requests. blithely final packages? blithely final packages are carefully",
                "after the quietly ironic packages. blithely ironic",
                "the final requests are carefully final; the final",
                "pending packages use the final, final courts. final packages are",
                "above the blithely final asymptotes. blithely regular courts sleep",
            ];
            let nouns: &[&str] = &[
                "foxes",
                "ideas",
                "dependencies",
                "excuses",
                "packages",
                "asymptotes",
            ];
            for _ in 0..n {
                let s = rng(&mut state);
                out.push(format!(
                    "{} {} {}. {} {} above the {} {}!",
                    clauses[(s as usize) % clauses.len()],
                    nouns[((s >> 8) as usize) % nouns.len()],
                    nouns[((s >> 16) as usize) % nouns.len()],
                    clauses[((s >> 24) as usize) % clauses.len()],
                    nouns[((s >> 32) as usize) % nouns.len()],
                    nouns[((s >> 40) as usize) % nouns.len()],
                    nouns[((s >> 48) as usize) % nouns.len()],
                ));
            }
        }
        Shape::UrlLog => {
            let templates: &[&str] = &[
                "https://www.example.com/products/{id}",
                "https://cdn.example.com/img/{id}.webp",
                "https://api.example.com/v2/orders/{id}",
                "https://www.example.com/users/{id}/profile",
                "INFO  request_id={id} status=200 method=GET",
                "WARN  request_id={id} status=429 method=POST",
                "ERROR request_id={id} status=500 method=PUT",
            ];
            for _ in 0..n {
                let s = rng(&mut state);
                let id = s as u32;
                out.push(
                    templates[(s as usize) % templates.len()].replace("{id}", &format!("{id:08x}")),
                );
            }
        }
        Shape::Short => {
            let templates: &[&str] = &["alpha", "beta", "gamma", "delta", "eps", "zeta", "eta"];
            for _ in 0..n {
                let s = rng(&mut state);
                out.push(templates[(s as usize) % templates.len()].to_string());
            }
        }
        Shape::HighCard => {
            for i in 0..n {
                out.push(format!("row-{i:010x}-{:016x}", rng(&mut state)));
            }
        }
    }
    out
}

/// One trained dictionary plus the rows it was trained on. Both matchers
/// consume this, so the A/B isolates the match loop from training.
struct Trained {
    parser: Parser,
    strings: Vec<String>,
    total: usize,
    fast: FastLpm,
}

fn train(strings: Vec<String>, config: Config) -> Trained {
    let total = strings.iter().map(|s| s.len()).sum();
    let view = StringRows {
        strings: &strings,
        total,
    };
    let parser = Parser::train_rows(&view, config);
    let fast = FastLpm::from_dictionary(parser.dict.as_view());
    Trained {
        parser,
        strings,
        total,
        fast,
    }
}

impl Trained {
    fn rows_view(&self) -> StringRows<'_> {
        StringRows {
            strings: &self.strings,
            total: self.total,
        }
    }
}

const CASES: &[(Shape, usize)] = &[
    (Shape::LComment, 100_000),
    (Shape::LComment, 400_000),
    (Shape::UrlLog, 100_000),
    (Shape::Short, 100_000),
    (Shape::HighCard, 100_000),
];

/// Upstream `Parser::parse_rows` — the pre-rewrite match loop, timed against
/// the same dictionary `matcher_fast_lpm` uses.
#[divan::bench(args = CASES)]
fn matcher_upstream(bencher: Bencher, case: (Shape, usize)) {
    let (shape, n) = case;
    let trained = train(corpus(n, shape), DEFAULT_CONFIG);
    bencher.bench_local(|| {
        let column = trained.parser.parse_rows::<_, u64>(&trained.rows_view());
        divan::black_box(column);
    });
}

/// `fast_lpm::parse_rows` — the direct-table match loop.
#[divan::bench(args = CASES)]
fn matcher_fast_lpm(bencher: Bencher, case: (Shape, usize)) {
    let (shape, n) = case;
    let trained = train(corpus(n, shape), DEFAULT_CONFIG);
    bencher.bench_local(|| {
        let (codes, offsets) = fast_lpm::parse_rows::<_, u64>(&trained.fast, &trained.rows_view());
        divan::black_box((codes, offsets));
    });
}

/// End-to-end `onpair_compress`, so the matcher delta can be read in the
/// context of the whole encode (training included).
#[divan::bench(args = CASES)]
fn compress_end_to_end(bencher: Bencher, case: (Shape, usize)) {
    use std::sync::LazyLock;

    use vortex_array::ExecutionCtx;
    use vortex_array::VortexSessionExecute;
    use vortex_array::array_session;
    use vortex_array::arrays::VarBinArray;
    use vortex_array::dtype::DType;
    use vortex_array::dtype::Nullability;
    use vortex_onpair::OnPair;
    use vortex_onpair::onpair_compress;
    use vortex_session::VortexSession;

    static SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
        let session = array_session();
        vortex_onpair::initialize(&session);
        session
    });

    let (shape, n) = case;
    let strings = corpus(n, shape);
    let varbin = VarBinArray::from_iter(
        strings.iter().map(|s| Some(s.as_bytes())),
        DType::Utf8(Nullability::NonNullable),
    );
    let array = varbin.as_array().clone();
    bencher
        .with_inputs(|| SESSION.create_execution_ctx())
        .bench_local_values(move |mut ctx: ExecutionCtx| {
            divan::black_box(
                onpair_compress(&array, DEFAULT_CONFIG, &mut ctx)
                    .unwrap_or_else(|e| panic!("onpair_compress failed: {e}"))
                    .try_downcast::<OnPair>()
                    .unwrap_or_else(|a| panic!("expected OnPair, got {}", a.encoding_id())),
            )
        });
}

/// Census of long-token bucket widths, printed rather than timed. Upstream
/// scans narrow buckets linearly and only promotes past 48 entries; FastLpm
/// always binary-searches, so the width distribution says how much that
/// choice actually matters for a given corpus.
#[divan::bench]
fn bucket_width_census() {
    const CENSUS_ROWS: usize = 100_000;
    for shape in [
        Shape::LComment,
        Shape::UrlLog,
        Shape::Short,
        Shape::HighCard,
    ] {
        let strings = corpus(CENSUS_ROWS, shape);
        let total: usize = strings.iter().map(|s| s.len()).sum();
        let parser = Parser::train_rows(
            &StringRows {
                strings: &strings,
                total,
            },
            DEFAULT_CONFIG,
        );
        let dict = parser.dict.as_view();

        let mut widths: HashMap<u64, u32> = HashMap::new();
        let (mut short, mut long, mut bytes) = (0u32, 0u32, 0usize);
        for i in 0..dict.num_tokens() {
            let t = dict.token(i as onpair::Token);
            bytes += t.len();
            if t.len() > 8 {
                long += 1;
                let prefix = u64::from_le_bytes(t[..8].try_into().unwrap());
                *widths.entry(prefix).or_default() += 1;
            } else {
                short += 1;
            }
        }

        let mut counts: Vec<u32> = widths.values().copied().collect();
        counts.sort_unstable();
        let buckets = counts.len() as u32;
        let median = counts.get(buckets as usize / 2).copied().unwrap_or(0);
        let p95 = counts
            .get((buckets as f64 * 0.95) as usize)
            .copied()
            .unwrap_or(0);
        let max = counts.last().copied().unwrap_or(0);
        let over_promote = counts.iter().filter(|c| **c > 48).count();
        let mean = if buckets == 0 {
            0.0
        } else {
            counts.iter().sum::<u32>() as f64 / f64::from(buckets)
        };

        println!(
            "{:<9} rows={CENSUS_ROWS} tokens={} short={short:<6} long={long:<6} \
             dict_bytes={bytes:<8} buckets={buckets:<6} mean={mean:.2} median={median} \
             p95={p95} max={max} over_promote_threshold={over_promote}",
            shape.name(),
            short + long,
        );
        divan::black_box((short, long, buckets, max));
    }
}

/// Correctness gate for the A/B: the two matchers must emit byte-identical
/// code streams on every bench corpus, and the census reports codes-per-byte so
/// a divergence in match length shows up as a token-count difference rather
/// than as a silent timing skew.
#[divan::bench]
fn code_stream_equivalence() {
    for shape in [
        Shape::LComment,
        Shape::UrlLog,
        Shape::Short,
        Shape::HighCard,
    ] {
        let strings = corpus(100_000, shape);
        let trained = train(strings, DEFAULT_CONFIG);

        let reference = trained.parser.parse_rows::<_, u64>(&trained.rows_view());
        let (codes, offsets) = fast_lpm::parse_rows::<_, u64>(&trained.fast, &trained.rows_view());

        assert_eq!(
            codes,
            reference.codes,
            "{}: code streams diverged",
            shape.name()
        );
        assert_eq!(
            offsets,
            reference.row_offsets,
            "{}: row offsets diverged",
            shape.name()
        );

        let total: usize = trained.strings.iter().map(|s| s.len()).sum();
        println!(
            "{:<9} codes={} bytes={total} codes_per_byte={:.3} OK",
            shape.name(),
            codes.len(),
            codes.len() as f64 / total as f64,
        );
        divan::black_box(codes.len());
    }
}

fn main() {
    divan::main();
}
