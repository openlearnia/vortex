// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Segment-level AES-GCM encryption for Vortex files.
//!
//! Layout of an encrypted segment buffer:
//! `[12-byte nonce][ciphertext || 16-byte tag]`
//!
//! Associated data authenticates `(offset_le, plaintext_len_le, segment_id_le)`.
//! Keys are never stored in the file; DuckLake (or another catalog) owns them.

use aes_gcm::Aes128Gcm;
use aes_gcm::Aes256Gcm;
use aes_gcm::KeyInit;
use aes_gcm::aead::Aead;
use aes_gcm::aead::Payload;
use vortex_buffer::ByteBuffer;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;

pub const NONCE_LEN: usize = 12;
/// Index written into `SegmentSpec._encryption` when AES-GCM is active (1-based; 0 = none).
pub const AES_GCM_SPEC_INDEX: u16 = 1;

#[derive(Clone)]
pub struct SegmentEncryptionKey {
    bytes: Vec<u8>,
}

impl SegmentEncryptionKey {
    pub fn try_new(bytes: impl Into<Vec<u8>>) -> VortexResult<Self> {
        let bytes = bytes.into();
        if bytes.len() != 16 && bytes.len() != 32 {
            vortex_bail!(
                "Vortex segment encryption key must be 16 or 32 bytes, got {}",
                bytes.len()
            );
        }
        Ok(Self { bytes })
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

fn aad(offset: u64, plaintext_len: u32, segment_id: u32) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&offset.to_le_bytes());
    out[8..12].copy_from_slice(&plaintext_len.to_le_bytes());
    out[12..16].copy_from_slice(&segment_id.to_le_bytes());
    out
}

/// Encrypt `plaintext` for storage at `offset` as segment `segment_id`.
pub fn encrypt_segment(
    key: &SegmentEncryptionKey,
    plaintext: &[u8],
    offset: u64,
    segment_id: u32,
) -> VortexResult<ByteBuffer> {
    let plaintext_len = u32::try_from(plaintext.len())
        .map_err(|_| vortex_err!("segment plaintext length exceeds u32"))?;
    let mut nonce = [0u8; NONCE_LEN];
    getrandom::fill(&mut nonce).map_err(|e| vortex_err!("failed to sample AES-GCM nonce: {e}"))?;
    let aad = aad(offset, plaintext_len, segment_id);
    let payload = Payload {
        msg: plaintext,
        aad: &aad,
    };
    let ciphertext = match key.bytes.len() {
        16 => {
            let cipher = Aes128Gcm::new_from_slice(&key.bytes)
                .map_err(|e| vortex_err!("invalid AES-128 key: {e}"))?;
            cipher
                .encrypt(nonce.as_ref().into(), payload)
                .map_err(|e| vortex_err!("AES-GCM encrypt failed: {e}"))?
        }
        32 => {
            let cipher = Aes256Gcm::new_from_slice(&key.bytes)
                .map_err(|e| vortex_err!("invalid AES-256 key: {e}"))?;
            cipher
                .encrypt(nonce.as_ref().into(), payload)
                .map_err(|e| vortex_err!("AES-GCM encrypt failed: {e}"))?
        }
        _ => unreachable!("validated in try_new"),
    };
    let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ciphertext);
    Ok(ByteBuffer::from(out))
}

/// Decrypt an encrypted segment buffer. `offset`/`segment_id` must match the writer.
pub fn decrypt_segment(
    key: &SegmentEncryptionKey,
    encrypted: &[u8],
    offset: u64,
    segment_id: u32,
) -> VortexResult<ByteBuffer> {
    if encrypted.len() < NONCE_LEN + 16 {
        vortex_bail!("encrypted segment too short");
    }
    let (nonce, ciphertext) = encrypted.split_at(NONCE_LEN);
    // Ciphertext length includes the GCM tag; recover plaintext length for AAD.
    let plaintext_len = u32::try_from(ciphertext.len().saturating_sub(16))
        .map_err(|_| vortex_err!("segment ciphertext length exceeds u32"))?;
    let aad = aad(offset, plaintext_len, segment_id);
    let payload = Payload {
        msg: ciphertext,
        aad: &aad,
    };
    let plaintext = match key.bytes.len() {
        16 => {
            let cipher = Aes128Gcm::new_from_slice(&key.bytes)
                .map_err(|e| vortex_err!("invalid AES-128 key: {e}"))?;
            cipher
                .decrypt(nonce.into(), payload)
                .map_err(|_| vortex_err!("AES-GCM decrypt failed (wrong key or tampered data)"))?
        }
        32 => {
            let cipher = Aes256Gcm::new_from_slice(&key.bytes)
                .map_err(|e| vortex_err!("invalid AES-256 key: {e}"))?;
            cipher
                .decrypt(nonce.into(), payload)
                .map_err(|_| vortex_err!("AES-GCM decrypt failed (wrong key or tampered data)"))?
        }
        _ => unreachable!("validated in try_new"),
    };
    Ok(ByteBuffer::from(plaintext))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_aes128() {
        let key = SegmentEncryptionKey::try_new(vec![7u8; 16]).unwrap();
        let plain = b"vortex segment bytes";
        let enc = encrypt_segment(&key, plain, 64, 3).unwrap();
        let dec = decrypt_segment(&key, enc.as_slice(), 64, 3).unwrap();
        assert_eq!(dec.as_slice(), plain);
    }

    #[test]
    fn wrong_key_fails() {
        let key = SegmentEncryptionKey::try_new(vec![7u8; 16]).unwrap();
        let other = SegmentEncryptionKey::try_new(vec![8u8; 16]).unwrap();
        let enc = encrypt_segment(&key, b"secret", 0, 0).unwrap();
        assert!(decrypt_segment(&other, enc.as_slice(), 0, 0).is_err());
    }
}
