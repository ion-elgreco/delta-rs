//! Provide schema merging for delta schemas
//!
use crate::{
    kernel::{ArrayType, DataType as DeltaDataType, MapType, StructField, StructType},
    table,
};
use arrow::datatypes::DataType::Dictionary;
use arrow_schema::{
    ArrowError, DataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use std::collections::HashMap;

fn try_merge_metadata<T: std::cmp::PartialEq + Clone>(
    left: &mut HashMap<String, T>,
    right: &HashMap<String, T>,
) -> Result<(), ArrowError> {
    for (k, v) in right {
        if let Some(vl) = left.get(k) {
            if vl != v {
                return Err(ArrowError::SchemaError(format!(
                    "Cannot merge metadata with different values for key {}",
                    k
                )));
            }
        } else {
            left.insert(k.clone(), v.clone());
        }
    }
    Ok(())
}

pub(crate) fn merge_delta_type(
    left: &DeltaDataType,
    right: &DeltaDataType,
) -> Result<DeltaDataType, ArrowError> {
    if left == right {
        return Ok(left.clone());
    }
    match (left, right) {
        (DeltaDataType::Array(a), DeltaDataType::Array(b)) => {
            let merged = merge_delta_type(&a.element_type, &b.element_type)?;
            Ok(DeltaDataType::Array(Box::new(ArrayType::new(
                merged,
                a.contains_null() || b.contains_null(),
            ))))
        }
        (DeltaDataType::Map(a), DeltaDataType::Map(b)) => {
            let merged_key = merge_delta_type(&a.key_type, &b.key_type)?;
            let merged_value = merge_delta_type(&a.value_type, &b.value_type)?;
            Ok(DeltaDataType::Map(Box::new(MapType::new(
                merged_key,
                merged_value,
                a.value_contains_null() || b.value_contains_null(),
            ))))
        }
        (DeltaDataType::Struct(a), DeltaDataType::Struct(b)) => {
            let merged = merge_delta_struct(a, b)?;
            Ok(DeltaDataType::Struct(Box::new(merged)))
        }
        (a, b) => Err(ArrowError::SchemaError(format!(
            "Cannot merge types {} and {}",
            a, b
        ))),
    }
}

pub(crate) fn merge_delta_struct(
    left: &StructType,
    right: &StructType,
) -> Result<StructType, ArrowError> {
    let mut errors = Vec::new();
    let merged_fields: Result<Vec<StructField>, ArrowError> = left
        .fields()
        .map(|field| {
            let right_field = right.field(field.name());
            if let Some(right_field) = right_field {
                let type_or_not = merge_delta_type(field.data_type(), right_field.data_type());
                match type_or_not {
                    Err(e) => {
                        errors.push(e.to_string());
                        Err(e)
                    }
                    Ok(f) => {
                        let mut new_field = StructField::new(
                            field.name(),
                            f,
                            field.is_nullable() || right_field.is_nullable(),
                        );

                        new_field.metadata.clone_from(&field.metadata);
                        try_merge_metadata(&mut new_field.metadata, &right_field.metadata)?;
                        Ok(new_field)
                    }
                }
            } else {
                Ok(field.clone())
            }
        })
        .collect();
    match merged_fields {
        Ok(mut fields) => {
            for field in right.fields() {
                if !left.field(field.name()).is_some() {
                    fields.push(field.clone());
                }
            }

            Ok(StructType::new(fields))
        }
        Err(e) => {
            errors.push(e.to_string());
            Err(ArrowError::SchemaError(errors.join("\n")))
        }
    }
}

pub(crate) fn merge_arrow_field(
    left: &ArrowField,
    right: &ArrowField,
) -> Result<ArrowField, ArrowError> {
    if left == right {
        return Ok(left.clone());
    }

    let (table_type, batch_type) = (left.data_type(), right.data_type());

    match (table_type, batch_type) {
        (Dictionary(key_type, value_type), _)
            if matches!(
                value_type.as_ref(),
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
            ) && matches!(
                batch_type,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) =>
        {
            Ok(ArrowField::new(
                right.name(),
                Dictionary(key_type.clone(), Box::new(batch_type.clone())),
                left.is_nullable() || right.is_nullable(),
            ))
        }
        (Dictionary(key_type, value_type), _)
            if matches!(
                value_type.as_ref(),
                DataType::Binary | DataType::BinaryView | DataType::LargeBinary
            ) && matches!(
                batch_type,
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView
            ) =>
        {
            Ok(ArrowField::new(
                right.name(),
                Dictionary(key_type.clone(), Box::new(batch_type.clone())),
                left.is_nullable() || right.is_nullable(),
            ))
        }
        (Dictionary(_, value_type), _) if value_type.equals_datatype(batch_type) => Ok(left
            .clone()
            .with_nullable(left.is_nullable() || right.is_nullable())),

        (_, Dictionary(_, value_type))
            if matches!(
                table_type,
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
            ) && matches!(
                value_type.as_ref(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) =>
        {
            Ok(right
                .clone()
                .with_nullable(left.is_nullable() || right.is_nullable()))
        }
        (_, Dictionary(_, value_type))
            if matches!(
                table_type,
                DataType::Binary | DataType::BinaryView | DataType::LargeBinary
            ) && matches!(
                value_type.as_ref(),
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView
            ) =>
        {
            Ok(right
                .clone()
                .with_nullable(left.is_nullable() || right.is_nullable()))
        }
        (_, Dictionary(_, value_type)) if value_type.equals_datatype(table_type) => Ok(right
            .clone()
            .with_nullable(left.is_nullable() || right.is_nullable())),
        // With Utf8/binary we always take  the right type since that is coming from the incoming data
        // by doing that we allow passthrough of any string flavor
        (
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
        )
        | (
            DataType::Binary | DataType::BinaryView | DataType::LargeBinary,
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
        ) => Ok(ArrowField::new(
            right.name(),
            batch_type.clone(),
            right.is_nullable() || left.is_nullable(),
        )),
        (
            DataType::List(left_child_fields) | DataType::LargeList(left_child_fields),
            DataType::LargeList(right_child_fields),
        ) => {
            let merged = merge_arrow_field(left_child_fields, right_child_fields)?;
            Ok(ArrowField::new(
                right.name(),
                DataType::LargeList(merged.into()),
                right.is_nullable() || left.is_nullable(),
            ))
        }
        (
            DataType::List(left_child_fields) | DataType::LargeList(left_child_fields),
            DataType::List(right_child_fields),
        ) => {
            let merged = merge_arrow_field(left_child_fields, right_child_fields)?;
            Ok(ArrowField::new(
                right.name(),
                DataType::List(merged.into()),
                right.is_nullable() || left.is_nullable(),
            ))
        }
        _ => {
            let mut new_field = left.clone();
            match new_field.try_merge(right) {
                Ok(()) => (),
                Err(_) => new_field = left.clone(),
                // Sometimes fields can't be merged because they are not the same types
                // So table has int32, but batch int64. We want the preserve the table type
                // At later stage we will call cast_record_batch which will cast the batch int64->int32
                // This is desired behaviour so we can have flexibility in the batch data types
                // But preserve the correct table and parquet types
            };
            Ok(new_field)
        }
    }
}

/// Merges Arrow Table schema and Arrow Batch Schema, by allowing Large/View Types to passthrough
pub(crate) fn merge_arrow_schema(
    table_schema: ArrowSchemaRef,
    batch_schema: ArrowSchemaRef,
) -> Result<ArrowSchemaRef, ArrowError> {
    let mut errors = Vec::with_capacity(table_schema.fields().len());
    let merged_fields: Result<Vec<ArrowField>, ArrowError> = table_schema
        .fields()
        .iter()
        .map(|field| {
            let right_field = batch_schema.field_with_name(field.name());
            if let Ok(right_field) = right_field {
                let field_or_not = merge_arrow_field(field.as_ref(), right_field);
                match field_or_not {
                    Err(e) => {
                        errors.push(e.to_string());
                        Err(e)
                    }
                    Ok(mut f) => {
                        let mut field_matadata = f.metadata().clone();
                        try_merge_metadata(&mut field_matadata, right_field.metadata())?;
                        f.set_metadata(field_matadata);
                        Ok(f)
                    }
                }
            } else {
                Ok(field.as_ref().clone())
            }
        })
        .collect();
    match merged_fields {
        Ok(mut fields) => {
            for field in batch_schema.fields() {
                if !table_schema.field_with_name(field.name()).is_ok() {
                    fields.push(field.as_ref().clone());
                }
            }
            let merged_schema = ArrowSchema::new(fields).into();
            Ok(merged_schema)
        }
        Err(e) => {
            errors.push(e.to_string());
            Err(ArrowError::SchemaError(errors.join("\n")))
        }
    }
}
