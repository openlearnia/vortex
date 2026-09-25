// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#pragma once

#include "data.hpp"
#include "duckdb/common/types/vector_buffer.hpp"

// Owns externally-managed data. Stored as a shared_ptr so it can either be
// attached to a VectorBuffer's auxiliary data or aliased into a ValidityMask's
// validity_data keep-alive.
class ExternalVectorBuffer {
    duckdb::unique_ptr<CData> data;

public:
    explicit inline ExternalVectorBuffer(duckdb::unique_ptr<CData> data) : data(std::move(data)) {
    }
};

// Attaches an ExternalVectorBuffer to a VectorBuffer's auxiliary data set,
// keeping the external data alive for the buffer's lifetime.
struct ExternalVectorBufferHolder final : duckdb::AuxiliaryDataHolder {
    duckdb::shared_ptr<ExternalVectorBuffer> buffer;

    explicit inline ExternalVectorBufferHolder(duckdb::shared_ptr<ExternalVectorBuffer> buffer)
        : buffer(std::move(buffer)) {
    }
};
