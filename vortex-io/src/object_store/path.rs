// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Conversion from a literal object key to an `object_store` [`Path`].

use object_store::path::Path;

/// Convert a literal object key (or filesystem path) into an object-store [`Path`].
///
/// Object stores key their objects *literally*: an object named `a~b.vortex` has the key
/// `a~b.vortex`, and `LocalFileSystem` likewise surfaces real filenames verbatim. [`Path::parse`]
/// preserves those characters, whereas [`Path::from`] percent-encodes `~`, `%`, `[`, `]`, `#`,
/// `{`, `}`, `^`, `|`, `*`, `?`, `<`, `>`, `"`, `` ` `` and `\` — turning `a~b.vortex` into the
/// key `a%7Eb.vortex`, which no real object has. Worse, the request layer then percent-encodes
/// that `%` again (`%7E` → `%257E`), so the object is doubly encoded and the store 404s.
///
/// Using `parse` keeps caller inputs, the keys returned by listings, and the keys sent on the wire
/// on a single literal representation, so a key from `list`/`head` round-trips back through a read
/// unchanged.
///
/// `parse` rejects empty, `.`, and `..` segments; for those we fall back to [`Path::from`], which
/// normalizes them (this never applies to a key a listing produced, so it cannot break a
/// round-trip).
pub fn object_path_from_literal(path: &str) -> Path {
    Path::parse(path).unwrap_or_else(|_| Path::from(path))
}
