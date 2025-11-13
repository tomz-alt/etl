use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use etl::types::{ArrayCell, Cell};
use serde_json::{Value, json};

/// Converts a [`Cell`] to a JSON [`Value`].
///
/// Handles all cell types with appropriate JSON representations:
/// - Null values become JSON null
/// - Primitive types map directly
/// - Temporal types are serialized as strings
/// - Complex types (arrays) are recursively converted
/// - Binary data is base64 encoded
pub fn cell_to_json(cell: &Cell) -> Value {
    match cell {
        Cell::Null => Value::Null,
        Cell::Bool(b) => json!(b),
        Cell::I16(i) => json!(i),
        Cell::I32(i) => json!(i),
        Cell::U32(u) => json!(u),
        Cell::I64(i) => json!(i),
        Cell::F32(f) => {
            if f.is_nan() || f.is_infinite() {
                Value::Null
            } else {
                json!(f)
            }
        }
        Cell::F64(f) => {
            if f.is_nan() || f.is_infinite() {
                Value::Null
            } else {
                json!(f)
            }
        }
        Cell::String(s) => json!(s),
        Cell::Bytes(b) => {
            // Encode binary data as base64
            json!(BASE64.encode(b))
        }
        Cell::Date(d) => json!(d.to_string()),
        Cell::Time(t) => json!(t.to_string()),
        Cell::Timestamp(ts) => json!(ts.format("%Y-%m-%d %H:%M:%S%.f").to_string()),
        Cell::TimestampTz(ts) => json!(ts.to_rfc3339()),
        Cell::Json(j) => j.clone(),
        Cell::Uuid(u) => json!(u.to_string()),
        Cell::Array(arr) => array_cell_to_json(arr),
        Cell::Numeric(n) => {
            // Convert numeric to string to preserve precision
            json!(n.to_string())
        }
    }
}

/// Converts an [`ArrayCell`] to a JSON array.
fn array_cell_to_json(arr: &ArrayCell) -> Value {
    match arr {
        ArrayCell::Bool(v) => json!(
            v.iter()
                .map(|opt| opt.map(|b| json!(b)).unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        ),
        ArrayCell::String(v) => json!(
            v.iter()
                .map(|opt| opt.as_ref().map(|s| json!(s)).unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        ),
        ArrayCell::I16(v) => json!(
            v.iter()
                .map(|opt| opt.map(|i| json!(i)).unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        ),
        ArrayCell::I32(v) => json!(
            v.iter()
                .map(|opt| opt.map(|i| json!(i)).unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        ),
        ArrayCell::U32(v) => json!(
            v.iter()
                .map(|opt| opt.map(|u| json!(u)).unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        ),
        ArrayCell::I64(v) => json!(
            v.iter()
                .map(|opt| opt.map(|i| json!(i)).unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        ),
        ArrayCell::F32(v) => json!(
            v.iter()
                .map(|opt| opt
                    .map(|f| if f.is_nan() || f.is_infinite() {
                        Value::Null
                    } else {
                        json!(f)
                    })
                    .unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        ),
        ArrayCell::F64(v) => json!(
            v.iter()
                .map(|opt| opt
                    .map(|f| if f.is_nan() || f.is_infinite() {
                        Value::Null
                    } else {
                        json!(f)
                    })
                    .unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        ),
        ArrayCell::Numeric(v) => json!(
            v.iter()
                .map(|opt| opt
                    .as_ref()
                    .map(|n| json!(n.to_string()))
                    .unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        ),
        ArrayCell::Date(v) => json!(
            v.iter()
                .map(|opt| opt.map(|d| json!(d.to_string())).unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        ),
        ArrayCell::Time(v) => json!(
            v.iter()
                .map(|opt| opt.map(|t| json!(t.to_string())).unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        ),
        ArrayCell::Timestamp(v) => json!(
            v.iter()
                .map(|opt| opt
                    .map(|ts| json!(ts.format("%Y-%m-%d %H:%M:%S%.f").to_string()))
                    .unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        ),
        ArrayCell::TimestampTz(v) => json!(
            v.iter()
                .map(|opt| opt.map(|ts| json!(ts.to_rfc3339())).unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        ),
        ArrayCell::Uuid(v) => json!(
            v.iter()
                .map(|opt| opt.map(|u| json!(u.to_string())).unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        ),
        ArrayCell::Json(v) => json!(
            v.iter()
                .map(|opt| opt.clone().unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        ),
        ArrayCell::Bytes(v) => json!(
            v.iter()
                .map(|opt| opt
                    .as_ref()
                    .map(|b| json!(BASE64.encode(b)))
                    .unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_cell() {
        let cell = Cell::Null;
        let json = cell_to_json(&cell);
        assert_eq!(json, Value::Null);
    }

    #[test]
    fn test_primitive_cells() {
        assert_eq!(cell_to_json(&Cell::Bool(true)), json!(true));
        assert_eq!(cell_to_json(&Cell::I32(42)), json!(42));
        assert_eq!(cell_to_json(&Cell::F64(3.14)), json!(3.14));
        assert_eq!(
            cell_to_json(&Cell::String("test".to_string())),
            json!("test")
        );
    }

    #[test]
    fn test_array_cell() {
        let arr = ArrayCell::I32(vec![Some(1), Some(2), None, Some(3)]);
        let cell = Cell::Array(arr);
        let json = cell_to_json(&cell);
        assert_eq!(json, json!([1, 2, null, 3]));
    }

    #[test]
    fn test_nan_and_infinity() {
        assert_eq!(cell_to_json(&Cell::F64(f64::NAN)), Value::Null);
        assert_eq!(cell_to_json(&Cell::F64(f64::INFINITY)), Value::Null);
        assert_eq!(cell_to_json(&Cell::F64(f64::NEG_INFINITY)), Value::Null);
    }
}
