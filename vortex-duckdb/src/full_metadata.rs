// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Footer-only Vortex file metadata for DuckLake `ducklake_add_data_files`.

use vortex::dtype::DType;
use vortex::dtype::FieldPath;
use vortex::dtype::Nullability;
use vortex::dtype::StructFields;
use vortex::error::VortexResult;
use vortex::error::vortex_bail;
use vortex::expr::stats::Precision;
use vortex::expr::stats::Stat;
use vortex::file::OpenOptionsSessionExt;
use vortex::file::VortexFile;
use vortex::io::runtime::BlockingRuntime;

use crate::copy::scalar_value_to_stats_string;
use crate::duckdb::LogicalType;
use crate::RUNTIME;
use crate::SESSION;

#[derive(Debug, Clone)]
pub struct SchemaNode {
    pub name: String,
    /// Empty for the synthetic root; otherwise a DuckDB type string (e.g. `INTEGER`).
    pub duckdb_type: String,
    pub num_children: u64,
}

#[derive(Debug, Clone)]
pub struct ColumnStat {
    pub column_id: u64,
    pub stats_min: Option<String>,
    pub stats_max: Option<String>,
    pub stats_null_count: Option<u64>,
    pub stats_num_values: Option<u64>,
    pub total_compressed_size: Option<u64>,
    pub contains_nan: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct FullMetadata {
    #[allow(dead_code)] // retained for FFI/debugging; C++ uses MultiFile OpenFileInfo path
    pub file_name: String,
    pub num_rows: u64,
    pub file_size_bytes: u64,
    pub schema: Vec<SchemaNode>,
    pub stats: Vec<ColumnStat>,
}

fn duckdb_type_name(dtype: &DType) -> VortexResult<String> {
    Ok(format!("{:?}", LogicalType::try_from(dtype)?))
}

fn is_nested_container(dtype: &DType) -> bool {
    matches!(
        dtype,
        DType::Struct(_, _)
            | DType::List(_, _)
            | DType::FixedSizeList(_, _, _)
            | DType::Map(_, _)
    )
}

fn push_schema_node(out: &mut Vec<SchemaNode>, name: &str, dtype: &DType) -> VortexResult<()> {
    match dtype {
        DType::Struct(fields, _) => {
            out.push(SchemaNode {
                name: name.to_owned(),
                duckdb_type: duckdb_type_name(dtype)?,
                num_children: fields.nfields() as u64,
            });
            for (child_name, child_dtype) in fields.names().iter().zip(fields.fields()) {
                push_schema_node(out, child_name.as_ref(), &child_dtype)?;
            }
        }
        DType::List(elem, _) | DType::FixedSizeList(elem, _, _) => {
            out.push(SchemaNode {
                name: name.to_owned(),
                duckdb_type: duckdb_type_name(dtype)?,
                num_children: 1,
            });
            push_schema_node(out, "element", elem.as_ref())?;
        }
        DType::Map(map_dtype, _) => {
            out.push(SchemaNode {
                name: name.to_owned(),
                duckdb_type: duckdb_type_name(dtype)?,
                num_children: 1,
            });
            let key = map_dtype.key_dtype();
            let value = map_dtype.value_dtype();
            let kv_dtype = DType::Struct(
                StructFields::from_iter([("key", key.clone()), ("value", value.clone())]),
                Nullability::NonNullable,
            );
            out.push(SchemaNode {
                name: "key_value".to_owned(),
                duckdb_type: duckdb_type_name(&kv_dtype)?,
                num_children: 2,
            });
            push_schema_node(out, "key", &key)?;
            push_schema_node(out, "value", &value)?;
        }
        _ => {
            out.push(SchemaNode {
                name: name.to_owned(),
                duckdb_type: duckdb_type_name(dtype)?,
                num_children: 0,
            });
        }
    }
    Ok(())
}

fn build_schema(dtype: &DType) -> VortexResult<Vec<SchemaNode>> {
    let mut out = Vec::new();
    match dtype {
        DType::Struct(fields, _) => {
            out.push(SchemaNode {
                name: "vortex_schema".to_owned(),
                duckdb_type: String::new(),
                num_children: fields.nfields() as u64,
            });
            for (name, field_dtype) in fields.names().iter().zip(fields.fields()) {
                push_schema_node(&mut out, name.as_ref(), &field_dtype)?;
            }
        }
        other => {
            out.push(SchemaNode {
                name: "vortex_schema".to_owned(),
                duckdb_type: String::new(),
                num_children: 1,
            });
            push_schema_node(&mut out, "value", other)?;
        }
    }
    Ok(out)
}

fn leaf_column_ids(schema: &[SchemaNode]) -> Vec<u64> {
    let mut next = 0u64;
    let mut ids = Vec::new();
    for node in schema.iter().skip(1) {
        if node.num_children == 0 {
            ids.push(next);
            next += 1;
        }
    }
    ids
}

fn build_column_stats(file: &VortexFile) -> VortexResult<Vec<ColumnStat>> {
    let schema = build_schema(file.dtype())?;
    let leaf_ids = leaf_column_ids(&schema);
    let row_count = file.row_count();

    let Some(file_stats) = file.file_stats() else {
        return Ok(Vec::new());
    };
    let sizes = file.footer().compressed_field_sizes().ok();

    let (names, top_dtypes): (Vec<String>, Vec<DType>) = match file.dtype() {
        DType::Struct(fields, _) => (
            fields.names().iter().map(|n| n.to_string()).collect(),
            fields.fields().collect(),
        ),
        other => (vec!["value".to_string()], vec![other.clone()]),
    };

    if file_stats.stats_sets().len() != top_dtypes.len() {
        vortex_bail!(
            "vortex footer stats length {} != top-level field count {}",
            file_stats.stats_sets().len(),
            top_dtypes.len()
        );
    }

    // Map each top-level field to its first DFS leaf id (None for nested containers).
    let mut top_level_leaf_id: Vec<Option<u64>> = Vec::with_capacity(top_dtypes.len());
    {
        let mut schema_idx = 1usize;
        let mut leaf_iter = leaf_ids.iter().copied();
        for _ in 0..top_dtypes.len() {
            let node = &schema[schema_idx];
            if node.num_children == 0 {
                top_level_leaf_id.push(leaf_iter.next());
                schema_idx += 1;
            } else {
                top_level_leaf_id.push(None);
                let mut remaining = node.num_children;
                schema_idx += 1;
                while remaining > 0 {
                    let child = &schema[schema_idx];
                    if child.num_children == 0 {
                        let _ = leaf_iter.next();
                    }
                    remaining = remaining - 1 + child.num_children;
                    schema_idx += 1;
                }
            }
        }
    }

    let mut out = Vec::new();
    for (idx, dtype) in top_dtypes.iter().enumerate() {
        if is_nested_container(dtype) {
            continue;
        }
        let Some(column_id) = top_level_leaf_id[idx] else {
            continue;
        };
        let stats = &file_stats.stats_sets()[idx];
        let name = names.get(idx).map(String::as_str).unwrap_or("value");

        let null_count = match stats.get(Stat::NullCount) {
            Precision::Exact(v) => v.as_primitive().as_u64(),
            _ => None,
        };
        let has_nan = match stats.get(Stat::NaNCount) {
            Precision::Exact(v) => v.as_primitive().as_u64().map(|c| c > 0),
            _ => None,
        };
        let min = match stats.get(Stat::Min) {
            Precision::Exact(v) => scalar_value_to_stats_string(dtype, v),
            _ => None,
        };
        let max = match stats.get(Stat::Max) {
            Precision::Exact(v) => scalar_value_to_stats_string(dtype, v),
            _ => None,
        };
        let compressed = sizes
            .as_ref()
            .and_then(|s| s.get(&FieldPath::from_name(name)));

        out.push(ColumnStat {
            column_id,
            stats_min: min,
            stats_max: max,
            stats_null_count: null_count,
            stats_num_values: Some(row_count),
            total_compressed_size: compressed,
            contains_nan: has_nan,
        });
    }
    Ok(out)
}

pub fn open_full_metadata(path: &str) -> VortexResult<FullMetadata> {
    RUNTIME.block_on(async {
        let file_size_bytes = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        let mut options = SESSION.open_options().include_metadata();
        if file_size_bytes > 0 {
            options = options.with_file_size(file_size_bytes);
        }
        let file = options.open_path(path).await?;
        Ok(FullMetadata {
            file_name: path.to_owned(),
            num_rows: file.row_count(),
            file_size_bytes,
            schema: build_schema(file.dtype())?,
            stats: build_column_stats(&file)?,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::build_schema;
    use vortex::dtype::DType;
    use vortex::dtype::Nullability;
    use vortex::dtype::PType;
    use vortex::dtype::StructFields;

    #[test]
    fn flat_schema_has_synthetic_root() {
        let dtype = DType::Struct(
            StructFields::from_iter([
                ("col1", DType::Primitive(PType::I32, Nullability::Nullable)),
                ("col2", DType::Utf8(Nullability::Nullable)),
            ]),
            Nullability::NonNullable,
        );
        let schema = build_schema(&dtype).unwrap();
        assert_eq!(schema[0].name, "vortex_schema");
        assert_eq!(schema[0].num_children, 2);
        assert_eq!(schema[1].name, "col1");
        assert_eq!(schema[1].num_children, 0);
        assert_eq!(schema[2].name, "col2");
        assert_eq!(schema[2].num_children, 0);
    }
}
