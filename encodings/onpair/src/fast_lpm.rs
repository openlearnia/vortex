// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Longest-prefix matcher over a trained OnPair dictionary.
//!
//! Same semantics as the `onpair` crate's encode-side matcher — the longest
//! dictionary token that prefixes the remaining input wins — with two
//! shortcuts over its hash-map probing:
//!
//!   * `single[256]` resolves the guaranteed length-1 fallback with a direct
//!     table lookup instead of a final hash probe.
//!   * `short_len_mask[256]` records, per first byte, which token lengths are
//!     present, so the descending-length loop only probes lengths that can
//!     match.
//!
//! The token ids returned index the trained dictionary, so the emitted code
//! stream is interchangeable with the crate's own parse.

// `load_window`/`load_le_u64` build a `u64` from a fixed-width slice, so the
// `try_into` conversions below are infallible by construction. Mirrors the
// same helpers in the upstream `onpair` crate's `lpm` module.
#![allow(clippy::unwrap_used)]

use hashbrown::HashMap;
use onpair::CompactDictionaryView;
use onpair::DictionaryView;
use onpair::Token;
use rustc_hash::FxBuildHasher;
use vortex_error::vortex_panic;

/// Tokens of this length or shorter live in the short table; longer tokens are
/// bucketed by their 8-byte prefix.
const PREFIX_LEN: usize = 8;

/// Maximum dictionary token size, taken from the upstream crate so a bump there
/// cannot silently desync the tables built here.
const MAX_TOKEN: usize = onpair::MAX_TOKEN_SIZE;

/// Maximum suffix length for long tokens.
const MAX_SUFFIX: usize = MAX_TOKEN - PREFIX_LEN;

// `present`/`short_len_mask` index a suffix length in a `u16`, so a wider token
// size would silently drop bits instead of failing to compile.
const _: () = assert!(MAX_SUFFIX < u16::BITS as usize);

/// Loads the first up-to-16 bytes of `data` as two little-endian `u64`s.
/// For `8 < n < 16` an overlapping tail load realigns bytes `n - 8..n` so the
/// window bytes land at the same positions as in the full-length case.
#[inline]
fn load_window(data: &[u8]) -> (u64, u64) {
    let n = data.len();
    if n >= MAX_TOKEN {
        return (
            u64::from_le_bytes(data[..8].try_into().unwrap()),
            u64::from_le_bytes(data[8..16].try_into().unwrap()),
        );
    }
    if n >= 8 {
        let lo = u64::from_le_bytes(data[..8].try_into().unwrap());
        let hi = if n > 8 {
            u64::from_le_bytes(data[n - 8..].try_into().unwrap()) >> ((MAX_TOKEN - n) * 8)
        } else {
            0
        };
        return (lo, hi);
    }
    let lo = if n >= 4 {
        u32::from_le_bytes(data[..4].try_into().unwrap()) as u64
            | (u32::from_le_bytes(data[n - 4..].try_into().unwrap()) as u64) << ((n - 4) * 8)
    } else if n >= 2 {
        u16::from_le_bytes(data[..2].try_into().unwrap()) as u64
            | (u16::from_le_bytes(data[n - 2..].try_into().unwrap()) as u64) << ((n - 2) * 8)
    } else {
        data[0] as u64
    };
    (lo, 0)
}

/// Packs the low `min(len, data.len(), 8)` bytes of `data` into a
/// little-endian `u64`.
#[inline]
fn load_le_u64(data: &[u8], len: usize) -> u64 {
    if len >= PREFIX_LEN && data.len() >= PREFIX_LEN {
        return u64::from_le_bytes(data[..PREFIX_LEN].try_into().unwrap());
    }
    let mut buf = [0u8; 8];
    let n = len.min(data.len());
    buf[..n].copy_from_slice(&data[..n]);
    u64::from_le_bytes(buf)
}

/// Mask of the low `len * 8` bits in a `u64`.
#[inline]
fn mask_u64(len: usize) -> u64 {
    if len >= 8 {
        u64::MAX
    } else {
        (1u64 << (len * 8)) - 1
    }
}

/// One long-token candidate: suffix bytes past the 8-byte prefix.
#[derive(Clone, Copy)]
struct LongEntry {
    suffix: u64,
    slen: u8,
    token: Token,
}

/// Bucket width past which the grouped binary search beats a linear scan.
const PROMOTE_THRESHOLD: usize = 48;

/// Long tokens sharing an 8-byte prefix.
///
/// Narrow buckets — the overwhelming majority on real text, where most 8-byte
/// prefixes are unique — stay on a linear scan. One XOR plus a trailing-zero
/// count per entry beats binary search once the bucket is only a few entries
/// wide, and the grouped form's `ends` indirection never pays for itself there.
enum LongBucket {
    /// Suffix-length-descending, so the first hit is the longest match.
    Linear(Vec<LongEntry>),
    /// `present` holds a bit per suffix length; `entries` is sorted by suffix
    /// length descending then by suffix, with `ends[s]`/`ends[s + 1]`
    /// bracketing each length's group.
    Grouped {
        entries: Vec<LongEntry>,
        ends: [u32; MAX_SUFFIX + 2],
        present: u16,
    },
}

impl LongBucket {
    fn build(entries: Vec<LongEntry>) -> Self {
        let mut entries = entries;
        entries.sort_unstable_by(|a, b| b.slen.cmp(&a.slen).then(a.suffix.cmp(&b.suffix)));

        if entries.len() <= PROMOTE_THRESHOLD {
            return LongBucket::Linear(entries);
        }

        let mut ends = [0u32; MAX_SUFFIX + 2];
        let mut present = 0u16;
        let mut counts = [0u32; MAX_SUFFIX + 1];
        for e in &entries {
            counts[e.slen as usize] += 1;
            present |= 1u16 << e.slen;
        }
        let mut acc = 0u32;
        for slen in (1..=MAX_SUFFIX).rev() {
            acc += counts[slen];
            ends[slen] = acc;
        }
        LongBucket::Grouped {
            entries,
            ends,
            present,
        }
    }

    /// Longest bucket token whose `slen` suffix bytes prefix `val`'s low bytes.
    #[inline]
    fn find(&self, val: u64, max_slen: usize) -> Option<(Token, usize)> {
        let Self::Linear(entries) = self else {
            let Self::Grouped {
                entries,
                ends,
                present,
            } = self
            else {
                unreachable!()
            };
            let mut lens = *present & ((1u16 << (max_slen + 1)) - 1);
            while lens != 0 {
                let slen = (u16::BITS - 1 - lens.leading_zeros()) as usize;
                lens &= !(1u16 << slen);

                let group = &entries[ends[slen + 1] as usize..ends[slen] as usize];
                let target = val & mask_u64(slen);
                if let Ok(i) = group.binary_search_by_key(&target, |e| e.suffix) {
                    return Some((group[i].token, slen));
                }
            }
            return None;
        };

        // Entries are suffix-length-descending, so the first match is longest.
        entries.iter().find_map(|e| {
            let elen = e.slen as usize;
            // Matching low bytes = trailing-zero bytes of the XOR.
            (elen <= max_slen && ((val ^ e.suffix).trailing_zeros() >> 3) as usize >= elen)
                .then_some((e.token, elen))
        })
    }
}

/// Longest-prefix matcher over a finished dictionary.
pub struct FastLpm {
    /// Tokens of length `2..=8` keyed by (packed bytes, length). Length-1
    /// tokens are served by `single` instead.
    short: HashMap<(u64, u8), Token, FxBuildHasher>,
    /// Tokens of length `9..=16` bucketed by their 8-byte prefix.
    long: HashMap<u64, LongBucket, FxBuildHasher>,
    /// Per first byte, bit `l` (2..=8) is set iff a length-`l` token starts
    /// with that byte.
    short_len_mask: [u16; 256],
    /// Length-1 token id per byte; every dictionary contains all 256.
    single: [Token; 256],
}

impl FastLpm {
    /// Builds a matcher over a trained dictionary's token view.
    ///
    /// The dictionary must contain all 256 single-byte tokens (the upstream
    /// trainer always emits them first). Without them the `single` fallback
    /// would resolve to token 0 and emit a wrong code stream, so this panics
    /// rather than corrupting output the way the array-initialized table would.
    pub fn from_dictionary(dict: CompactDictionaryView<'_>) -> Self {
        let n = dict.num_tokens();
        let mut me = Self {
            short: HashMap::with_capacity_and_hasher(n, FxBuildHasher),
            long: HashMap::with_hasher(FxBuildHasher),
            short_len_mask: [0; 256],
            single: [0; 256],
        };
        let mut long_entries: HashMap<u64, Vec<LongEntry>, FxBuildHasher> =
            HashMap::with_hasher(FxBuildHasher);
        let mut singles = 0u16;
        for i in 0..n {
            let token = i as Token;
            let t = dict.token(token);
            debug_assert!(!t.is_empty() && t.len() <= MAX_TOKEN);
            let len = t.len();
            if len == 1 {
                me.single[t[0] as usize] = token;
                singles += 1;
            } else if len <= PREFIX_LEN {
                me.short.insert((load_le_u64(t, len), len as u8), token);
                me.short_len_mask[t[0] as usize] |= 1u16 << len;
            } else {
                let prefix = load_le_u64(t, PREFIX_LEN);
                let slen = len - PREFIX_LEN;
                long_entries.entry(prefix).or_default().push(LongEntry {
                    suffix: load_le_u64(&t[PREFIX_LEN..], slen),
                    slen: slen as u8,
                    token,
                });
            }
        }
        if singles != 256 {
            vortex_panic!("OnPair dictionary has {singles} single-byte tokens, expected 256");
        }
        me.long.reserve(long_entries.len());
        for (prefix, entries) in long_entries {
            me.long.insert(prefix, LongBucket::build(entries));
        }
        me
    }

    /// Longest dictionary token that is a prefix of `data`, with its length.
    ///
    /// Identical semantics to the crate's matcher: probe long tokens by the
    /// 8-byte prefix first, then short tokens by descending length, then the
    /// guaranteed single-byte token.
    ///
    /// `data` must be non-empty.
    #[inline]
    pub fn find_longest_match(&self, data: &[u8]) -> (Token, usize) {
        let (lo64, hi64) = load_window(data);
        let win = data.len().min(MAX_TOKEN);

        if win > PREFIX_LEN
            && !self.long.is_empty()
            && let Some(bucket) = self.long.get(&lo64)
            && let Some((t, slen)) = bucket.find(hi64, win - PREFIX_LEN)
        {
            return (t, PREFIX_LEN + slen);
        }

        // Lengths that could match this first byte, 2..=min(win, 8).
        let mut lens = self.short_len_mask[data[0] as usize] & ((2u16 << win.min(PREFIX_LEN)) - 4);
        while lens != 0 {
            let len = (u16::BITS - 1 - lens.leading_zeros()) as usize;
            lens &= !(1u16 << len);
            if let Some(&t) = self.short.get(&(lo64 & mask_u64(len), len as u8)) {
                return (t, len);
            }
        }

        // Every dictionary contains all single-byte tokens.
        (self.single[data[0] as usize], 1)
    }
}

/// Tokenizes all rows against `lpm`, returning the code stream and row code
/// offsets. Equivalent to `onpair::Parser::parse_rows`, minus the matcher cost.
pub fn parse_rows<R: onpair::Rows + ?Sized, O: onpair::Offset>(
    lpm: &FastLpm,
    rows: &R,
) -> (Vec<Token>, Vec<O>) {
    let n = rows.num_rows();
    let mut codes: Vec<Token> = Vec::with_capacity(rows.total_bytes() / 4);
    let mut row_offsets: Vec<O> = Vec::with_capacity(n + 1);
    row_offsets.push(O::from_usize(0));

    for row in (0..n).map(|i| rows.row(i)) {
        let mut pos = 0;
        while pos < row.len() {
            let (tok, mlen) = lpm.find_longest_match(&row[pos..]);
            codes.push(tok);
            pos += mlen;
        }
        row_offsets.push(O::from_usize(codes.len()));
    }

    (codes, row_offsets)
}

#[cfg(test)]
mod tests {
    use onpair::Column;
    use onpair::Config;
    use onpair::Dictionary;
    use onpair::Parser;
    use onpair::Rows;

    use super::FastLpm;

    struct SliceRows<'a> {
        rows: &'a [&'a [u8]],
        total: usize,
    }

    impl Rows for SliceRows<'_> {
        fn num_rows(&self) -> usize {
            self.rows.len()
        }

        fn total_bytes(&self) -> usize {
            self.total
        }

        fn row(&self, i: usize) -> &[u8] {
            self.rows[i]
        }
    }

    /// Every corpus must tokenize byte-identically to the crate's own parse.
    fn assert_same_tokenization(strings: &[&[u8]]) {
        let total: usize = strings.iter().map(|s| s.len()).sum();
        let rows = SliceRows {
            rows: strings,
            total,
        };
        let parser = Parser::train_rows(&rows, Config::default());
        let reference: Column<u64> = parser.parse_rows(&rows);
        let lpm = FastLpm::from_dictionary(reference.dict.as_view());
        let (codes, offsets) = super::parse_rows::<_, u64>(&lpm, &rows);
        assert_eq!(codes, reference.codes);
        assert_eq!(offsets, reference.row_offsets);
    }

    #[test]
    fn repeated_text() {
        let rows: Vec<&[u8]> = (0..5000)
            .map(|i| {
                [
                    b"the quick brown fox jumps over the lazy dog"[..].as_ref(),
                    b"lorem ipsum dolor sit amet, consectetur adipiscing".as_ref(),
                    b"customer complains about package delivery issues".as_ref(),
                ][i % 3]
            })
            .collect();
        assert_same_tokenization(&rows);
    }

    #[test]
    fn random_ascii() {
        let mut seed = 0x9E3779B9u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let storage: Vec<Vec<u8>> = (0..3000)
            .map(|_| {
                let len = (next() % 40) as usize;
                (0..len).map(|_| b'a' + (next() % 26) as u8).collect()
            })
            .collect();
        let rows: Vec<&[u8]> = storage.iter().map(|v| v.as_slice()).collect();
        assert_same_tokenization(&rows);
    }

    #[test]
    fn short_and_binary_rows() {
        let storage: Vec<Vec<u8>> = (0u32..2000)
            .map(|i| vec![(i % 251) as u8, (i % 7) as u8])
            .chain((0..256u32).map(|b| vec![b as u8]))
            .collect();
        let rows: Vec<&[u8]> = storage.iter().map(|v| v.as_slice()).collect();
        assert_same_tokenization(&rows);
    }

    #[test]
    fn empty_and_homogeneous() {
        let storage: Vec<Vec<u8>> = std::iter::repeat_n(Vec::new(), 10)
            .chain(std::iter::repeat_n(vec![b'x'; 3], 50))
            .chain(std::iter::repeat_n(vec![b'y'; 16], 50))
            .collect();
        let rows: Vec<&[u8]> = storage.iter().map(|v| v.as_slice()).collect();
        assert_same_tokenization(&rows);
    }

    #[test]
    fn long_tokens_present() {
        // Shared 16+ byte prefixes force the long-bucket path.
        let storage: Vec<Vec<u8>> = (0..2000u32)
            .map(|i| {
                let mut v = b"abcdefghijklmnop-".to_vec();
                v.extend_from_slice(&(i % 97).to_string().into_bytes());
                v
            })
            .collect();
        let rows: Vec<&[u8]> = storage.iter().map(|v| v.as_slice()).collect();
        assert_same_tokenization(&rows);
    }
}
