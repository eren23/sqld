use std::fmt;

/// The SQL data types supported by the engine.
///
/// `Varchar` carries a maximum byte-length, `Decimal` carries precision and
/// scale.  All other variants are fixed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    Integer,
    BigInt,
    Float,
    Boolean,
    Varchar(u32),
    Text,
    Timestamp,
    Date,
    Decimal(u8, u8), // (precision, scale)
    Blob,
}

impl DataType {
    // -----------------------------------------------------------------------
    // Classification helpers
    // -----------------------------------------------------------------------

    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataType::Integer | DataType::BigInt | DataType::Float | DataType::Decimal(_, _)
        )
    }

    pub fn is_string(&self) -> bool {
        matches!(self, DataType::Varchar(_) | DataType::Text)
    }

    pub fn is_temporal(&self) -> bool {
        matches!(self, DataType::Timestamp | DataType::Date)
    }

    /// Returns the fixed on-disk size in bytes, or `None` for variable-length
    /// types (Varchar, Text, Blob).
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            DataType::Integer => Some(4),
            DataType::BigInt => Some(8),
            DataType::Float => Some(8),
            DataType::Boolean => Some(1),
            DataType::Timestamp => Some(8),
            DataType::Date => Some(4),
            DataType::Decimal(_, _) => Some(17), // i128 (16) + scale (1)
            DataType::Varchar(_) | DataType::Text | DataType::Blob => None,
        }
    }

    // -----------------------------------------------------------------------
    // Coercion lattice
    // -----------------------------------------------------------------------

    /// Numeric rank used to determine the common super-type.
    /// Higher rank wins.  Returns `None` for non-numeric types.
    fn numeric_rank(&self) -> Option<u8> {
        match self {
            DataType::Integer => Some(0),
            DataType::BigInt => Some(1),
            DataType::Decimal(_, _) => Some(2),
            DataType::Float => Some(3),
            _ => None,
        }
    }

    /// Compute the *least common super-type* of two types following the
    /// implicit coercion lattice:
    ///
    /// ```text
    /// INTEGER → BIGINT → DECIMAL → FLOAT
    /// VARCHAR(n) → TEXT
    /// DATE → TIMESTAMP
    /// ```
    ///
    /// Returns `None` when the types are incompatible (no implicit coercion).
    pub fn common_type(a: DataType, b: DataType) -> Option<DataType> {
        if a == b {
            return Some(a);
        }

        // Numeric coercion
        if let (Some(ra), Some(rb)) = (a.numeric_rank(), b.numeric_rank()) {
            let winner = if ra >= rb { a } else { b };
            // If either is Decimal we need to merge precision/scale
            return match winner {
                DataType::Decimal(p, s) => {
                    // Keep the widest Decimal; if promoting from int types,
                    // use the existing precision/scale.
                    let (p2, s2) = match (a, b) {
                        (DataType::Decimal(p1, s1), DataType::Decimal(p2, s2)) => {
                            (p1.max(p2), s1.max(s2))
                        }
                        (DataType::Decimal(p1, s1), _) => (p1, s1),
                        (_, DataType::Decimal(p2, s2)) => (p2, s2),
                        _ => (p, s),
                    };
                    Some(DataType::Decimal(p2, s2))
                }
                other => Some(other),
            };
        }

        // String coercion: VARCHAR(n) → TEXT
        if a.is_string() && b.is_string() {
            return Some(DataType::Text);
        }

        // Temporal coercion: DATE → TIMESTAMP
        if a.is_temporal() && b.is_temporal() {
            return Some(DataType::Timestamp);
        }

        None // incompatible
    }

    /// Whether `self` can be implicitly coerced to `target`.
    pub fn can_coerce_to(&self, target: &DataType) -> bool {
        if self == target {
            return true;
        }
        DataType::common_type(*self, *target)
            .map(|ct| ct == *target)
            .unwrap_or(false)
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Integer => write!(f, "INTEGER"),
            DataType::BigInt => write!(f, "BIGINT"),
            DataType::Float => write!(f, "FLOAT"),
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Varchar(n) => write!(f, "VARCHAR({n})"),
            DataType::Text => write!(f, "TEXT"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
            DataType::Date => write!(f, "DATE"),
            DataType::Decimal(p, s) => write!(f, "DECIMAL({p},{s})"),
            DataType::Blob => write!(f, "BLOB"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_coercion_lattice() {
        assert_eq!(
            DataType::common_type(DataType::Integer, DataType::BigInt),
            Some(DataType::BigInt)
        );
        assert_eq!(
            DataType::common_type(DataType::BigInt, DataType::Decimal(10, 2)),
            Some(DataType::Decimal(10, 2))
        );
        assert_eq!(
            DataType::common_type(DataType::Decimal(10, 2), DataType::Float),
            Some(DataType::Float)
        );
        assert_eq!(
            DataType::common_type(DataType::Integer, DataType::Float),
            Some(DataType::Float)
        );
    }

    #[test]
    fn string_coercion() {
        assert_eq!(
            DataType::common_type(DataType::Varchar(100), DataType::Text),
            Some(DataType::Text)
        );
        assert_eq!(
            DataType::common_type(DataType::Varchar(50), DataType::Varchar(200)),
            Some(DataType::Text)
        );
    }

    #[test]
    fn temporal_coercion() {
        assert_eq!(
            DataType::common_type(DataType::Date, DataType::Timestamp),
            Some(DataType::Timestamp)
        );
    }

    #[test]
    fn incompatible_types() {
        assert_eq!(
            DataType::common_type(DataType::Integer, DataType::Boolean),
            None
        );
        assert_eq!(
            DataType::common_type(DataType::Varchar(10), DataType::Float),
            None
        );
        assert_eq!(
            DataType::common_type(DataType::Blob, DataType::Integer),
            None
        );
    }

    #[test]
    fn can_coerce_to() {
        assert!(DataType::Integer.can_coerce_to(&DataType::BigInt));
        assert!(DataType::Integer.can_coerce_to(&DataType::Float));
        assert!(!DataType::Float.can_coerce_to(&DataType::Integer)); // no narrowing
        assert!(DataType::Date.can_coerce_to(&DataType::Timestamp));
        assert!(!DataType::Timestamp.can_coerce_to(&DataType::Date));
    }

    #[test]
    fn classification() {
        assert!(DataType::Integer.is_numeric());
        assert!(DataType::Decimal(10, 2).is_numeric());
        assert!(!DataType::Boolean.is_numeric());
        assert!(DataType::Varchar(255).is_string());
        assert!(DataType::Text.is_string());
        assert!(DataType::Date.is_temporal());
    }
}
