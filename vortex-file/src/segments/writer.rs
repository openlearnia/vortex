// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use parking_lot::Mutex;
use vortex_buffer::Alignment;
use vortex_buffer::ByteBuffer;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_layout::segments::SegmentId;
use vortex_layout::segments::SegmentSink;
use vortex_layout::sequence::SequenceId;

use crate::encryption::AES_GCM_SPEC_INDEX;
use crate::encryption::SegmentEncryptionKey;
use crate::encryption::encrypt_segment;
use crate::footer::SegmentSpec;

pub struct BufferedSegmentSink {
    buffers: kanal::AsyncSender<ByteBuffer>,
    byte_offset: AtomicU64,
    segment_specs: Mutex<Vec<SegmentSpec>>,
    encryption_key: Option<SegmentEncryptionKey>,
}

impl BufferedSegmentSink {
    pub fn with_encryption(
        send: kanal::AsyncSender<ByteBuffer>,
        byte_offset: u64,
        encryption_key: Option<SegmentEncryptionKey>,
    ) -> Self {
        Self {
            buffers: send,
            byte_offset: AtomicU64::new(byte_offset),
            segment_specs: Default::default(),
            encryption_key,
        }
    }

    /// Close the sink, returning the segment specs and the final byte offset.
    pub fn segment_specs(&self) -> Arc<[SegmentSpec]> {
        let specs = self.segment_specs.lock();
        specs.clone().into()
    }
}

#[async_trait]
impl SegmentSink for BufferedSegmentSink {
    async fn write(
        &self,
        mut sequence_id: SequenceId,
        buffers: Vec<ByteBuffer>,
    ) -> VortexResult<SegmentId> {
        // We wait for all segment IDs before this one to be dropped. Then while we hold a strong
        // reference to this one, we essentially have an exclusive lock on the segment writer.
        sequence_id.collapse().await;

        let (segment_id, padding_buffer, out_buffers) = {
            let mut specs = self.segment_specs.lock();
            let segment_id_u32 = u32::try_from(specs.len())
                .map_err(|_| vortex_err!("Too mant segments, u32 overflow"))?;
            let segment_id = SegmentId::from(segment_id_u32);

            // The API requires us to write these buffers contiguously. Therefore, we can only
            // respect the alignment of the first one.
            let alignment = buffers
                .first()
                .map(|buffer| buffer.alignment())
                .unwrap_or_else(Alignment::none);

            let byte_offset = self.byte_offset.load(Ordering::Relaxed);
            let padding = byte_offset.next_multiple_of(alignment.as_usize() as u64) - byte_offset;
            let offset = byte_offset + padding;

            let (out_buffers, length, encryption) = if let Some(key) = &self.encryption_key {
                let mut plain = Vec::new();
                for buffer in &buffers {
                    plain.extend_from_slice(buffer.as_slice());
                }
                let encrypted = encrypt_segment(key, &plain, offset, segment_id_u32)?;
                let length = u32::try_from(encrypted.len())
                    .map_err(|_| vortex_err!("segment buffer length exceeds maximum u32"))?;
                (vec![encrypted], length, AES_GCM_SPEC_INDEX)
            } else {
                let length =
                    u32::try_from(buffers.iter().map(|buffer| buffer.len()).sum::<usize>())
                        .map_err(|_| vortex_err!("segment buffer length exceeds maximum u32"))?;
                (buffers, length, 0)
            };

            specs.push(SegmentSpec {
                offset,
                length,
                alignment,
                encryption,
            });

            self.byte_offset
                .store(byte_offset + padding + u64::from(length), Ordering::Relaxed);

            if padding > 0 {
                (
                    segment_id,
                    Some(ByteBuffer::zeroed(padding as usize)),
                    out_buffers,
                )
            } else {
                (segment_id, None, out_buffers)
            }
        };

        if let Some(padding) = padding_buffer {
            let _ = self.buffers.send(padding).await;
        }
        for buffer in out_buffers {
            let _ = self.buffers.send(buffer).await;
        }

        Ok(segment_id)
    }
}
