// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;

use futures::FutureExt;
use vortex_array::buffer::BufferHandle;
use vortex_error::vortex_err;
use vortex_layout::segments::SegmentFuture;
use vortex_layout::segments::SegmentId;
use vortex_layout::segments::SegmentSource;

use crate::encryption::SegmentEncryptionKey;
use crate::encryption::decrypt_segment;
use crate::footer::SegmentSpec;

/// Wraps a [`SegmentSource`] and decrypts AES-GCM segments when a key is present.
pub struct DecryptingSegmentSource {
    inner: Arc<dyn SegmentSource>,
    segments: Arc<[SegmentSpec]>,
    key: SegmentEncryptionKey,
}

impl DecryptingSegmentSource {
    pub fn new(
        inner: Arc<dyn SegmentSource>,
        segments: Arc<[SegmentSpec]>,
        key: SegmentEncryptionKey,
    ) -> Self {
        Self {
            inner,
            segments,
            key,
        }
    }
}

impl SegmentSource for DecryptingSegmentSource {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        let Some(spec) = self.segments.get(*id as usize).copied() else {
            return futures::future::ready(Err(vortex_err!("Missing segment: {id}"))).boxed();
        };
        if !spec.is_encrypted() {
            return self.inner.request(id);
        }
        let key = self.key.clone();
        let fut = self.inner.request(id);
        async move {
            let handle = fut.await?;
            let host = handle.unwrap_host();
            let plain = decrypt_segment(&key, host.as_slice(), spec.offset, *id)?;
            Ok(BufferHandle::new_host(plain.aligned(spec.alignment)))
        }
        .boxed()
    }
}
