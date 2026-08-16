# Vortex Segment-Level Encryption (ADR)

**Status:** Accepted for VortexLake managed-table parity  
**Date:** 2026-08-15

## Context

DuckLake encrypts Parquet footers with per-file keys. VortexLake needs equivalent
protection for managed `.vortex` data files and `*-delete.vortex` positional-delete
files without giving up range reads.

## Decision

Encrypt **segments independently** with **AES-GCM**:

1. Unique 96-bit nonce per encrypted segment.
2. Authenticated associated data covers segment locator metadata (offset/length/id).
3. No key bytes are stored in the file.
4. Algorithm + version live in the previously empty `EncryptionSpec` footer field.
5. Unencrypted files remain readable (spec version / empty EncryptionSpec).

## Consequences

- Range reads stay valid: decrypt only the segments touched by a scan.
- DuckLake passes the existing per-file key through Vortex `encryption_config`
  on COPY and scan init (same shape as Parquet `footer_key_value`).
- Wrong-key / tampered ciphertext fail closed via GCM tag verification.
- Implementation order: footer schema → vortex-file writer/reader → DuckDB COPY/scan
  wiring → DuckLake insert/delete paths.

## Non-goals (this ADR)

- Key management / KMS integration (DuckLake already owns keys).
- Encrypting only the footer (rejected: insufficient for data-at-rest).
- Cross-file nonce reuse (forbidden).
