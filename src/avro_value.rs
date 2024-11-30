use apache_avro::types::Value;
use jiff::{tz::TimeZone, Span};
use miette::{IntoDiagnostic, Result};
use num_bigint::BigInt;
use std::fmt;

pub(crate) const NULL: &'static str = "null";
pub(crate) const NA: &'static str = "N/A";

#[derive(Debug, Clone)]
pub(crate) enum AvroValue {
    Value(Value),
    Na,
}

impl<'a> AvroValue {
    pub fn from(value: Value) -> Self {
        AvroValue::Value(value)
    }

    pub fn na() -> Self {
        AvroValue::Na
    }

    pub fn to_string(&self) -> String {
        format!("{}", self)
    }
    
    pub fn to_json(&self) -> Result<serde_json::Value> {
        match self {
            Self::Na => Ok(serde_json::Value::Null),
            Self::Value(v) => to_json(v),
        }
    }
}

impl<'a> fmt::Display for AvroValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AvroValue::Value(v) => write!(f, "{}", format_avro_value(v).map_err(|_| fmt::Error)?),
            AvroValue::Na => write!(f, "{}", NA),
        }
    }
}

fn format_avro_value(value: &Value) -> Result<String> {
    Ok(match value {
        Value::Array(a) => format!(
            "{}",
            a.iter()
                .map(|v| format_avro_value(v))
                .collect::<Result<Vec<String>>>()?
                .join(", ")
        ),
        Value::Bytes(b) => format!(
            "{}",
            b.iter()
                .map(|n| format!("{}", n))
                .collect::<Vec<String>>()
                .join(", ")
        ),
        Value::Boolean(b) => format!("{}", b),
        Value::Double(d) => format!("{}", d),
        Value::Enum(id, desc) => format!("{} ({})", id, desc),
        Value::Fixed(_, f) => format!(
            "{}",
            f.iter()
                .map(|n| format!("{}", n))
                .collect::<Vec<String>>()
                .join(", ")
        ),
        Value::Float(f) => format!("{}", f),
        Value::Int(i) => format!("{}", i),
        Value::Long(l) => format!("{}", l),
        Value::Map(m) => format!(
            "{}",
            m.iter()
                .map(|(k, v)| format_avro_value(v).map(|v| format!("{}: {}", k, v)))
                .collect::<Result<Vec<String>>>()?
                .join(", ")
        ),
        Value::Null => NULL.to_owned(),
        Value::Record(m) => format!(
            "{}",
            m.iter()
                .map(|(k, v)| format_avro_value(v).map(|v| format!("{}: {}", k, v)))
                .collect::<Result<Vec<String>>>()?
                .join(", ")
        ),
        Value::String(s) => s.clone(),

        Value::Date(s) => jiff::Timestamp::from_second((*s).into())
            .into_diagnostic()?
            .to_string(),
        Value::Decimal(decimal) => BigInt::from(decimal.clone()).to_string(),
        Value::BigDecimal(big_decimal) => big_decimal.as_bigint_and_exponent().0.to_string(),
        Value::TimeMillis(ms) => jiff::civil::Time::MIN
            .saturating_add(Span::new().milliseconds(*ms))
            .to_string(),
        Value::TimeMicros(us) => jiff::civil::Time::MIN
            .saturating_add(Span::new().microseconds(*us))
            .to_string(),
        Value::TimestampMillis(ms) => jiff::Timestamp::from_millisecond(*ms)
            .into_diagnostic()?
            .to_string(),
        Value::TimestampMicros(us) => jiff::Timestamp::from_millisecond(*us)
            .into_diagnostic()?
            .to_string(),
        Value::TimestampNanos(ns) => jiff::Timestamp::from_millisecond(*ns)
            .into_diagnostic()?
            .to_string(),
        Value::LocalTimestampMillis(ms) => jiff::Timestamp::from_millisecond(*ms)
            .into_diagnostic()?
            .to_zoned(TimeZone::try_system().unwrap_or(TimeZone::UTC))
            .to_string(),
        Value::LocalTimestampMicros(us) => jiff::Timestamp::from_microsecond(*us)
            .into_diagnostic()?
            .to_zoned(TimeZone::try_system().unwrap_or(TimeZone::UTC))
            .to_string(),
        Value::LocalTimestampNanos(ns) => jiff::Timestamp::from_nanosecond((*ns).into())
            .into_diagnostic()?
            .to_zoned(TimeZone::try_system().unwrap_or(TimeZone::UTC))
            .to_string(),
        Value::Duration(duration) => (jiff::Span::new()
            .months(u32::from(duration.months()))
            .checked_add(jiff::Span::new().days(u32::from(duration.days())))
            .into_diagnostic()?
            .checked_add(jiff::Span::new().milliseconds(u32::from(duration.millis())))
            .into_diagnostic()?)
        .to_string(),
        Value::Uuid(uuid) => uuid.to_string(),

        Value::Union(_, value) => format_avro_value(&*value)?,
    })
}

pub fn to_json(value: &Value) -> Result<serde_json::Value> {
    Ok(match value {
        Value::Array(a) => serde_json::Value::Array(
            a.iter()
                .map(|v| to_json(v))
                .collect::<Result<Vec<serde_json::Value>>>()?,
        ),
        Value::Map(m) => serde_json::Value::Object(
            m.iter()
                .map(|(k, v)| to_json(v).map(|v| (k.to_owned(), v)))
                .collect::<Result<_>>()?,
        ),
        Value::Record(m) => serde_json::Value::Object(
            m.iter()
                .map(|(k, v)| to_json(v).map(|v| (k.to_owned(), v)))
                .collect::<Result<_>>()?,
        ),
        Value::Union(_, value) => to_json(&*value)?,
        Value::Null => serde_json::Value::Null,

        Value::Bytes(b) => serde_json::Value::Array(
            b.iter()
                .map(|b| serde_json::Value::Number((*b).into()))
                .collect(),
        ),
        Value::Boolean(b) => serde_json::Value::Bool(*b),
        Value::Double(d) => serde_json::Value::Number(
            serde_json::Number::from_f64(*d).unwrap_or(serde_json::Number::from_f64(0.0).unwrap()),
        ),
        Value::Float(f) => serde_json::Value::Number(
            serde_json::Number::from_f64((*f).into())
                .unwrap_or(serde_json::Number::from_f64(0.0).unwrap()),
        ),
        Value::Enum(_id, desc) => serde_json::Value::String(desc.into()),
        Value::Fixed(_, f) => serde_json::Value::Array(
            f.iter()
                .map(|b| serde_json::Value::Number((*b).into()))
                .collect(),
        ),
        Value::Int(i) => serde_json::Value::Number((*i).into()),
        Value::Long(l) => serde_json::Value::Number((*l).into()),
        Value::String(s) => serde_json::Value::String(s.into()),
        Value::Uuid(uuid) => serde_json::Value::String(uuid.to_string()),
        Value::Date(s) => serde_json::Value::String(
            jiff::Timestamp::from_second((*s).into())
                .into_diagnostic()?
                .to_string(),
        ),
        Value::Decimal(decimal) => {
            serde_json::Value::String(BigInt::from(decimal.clone()).to_string())
        }
        Value::BigDecimal(big_decimal) => {
            serde_json::Value::String(big_decimal.as_bigint_and_exponent().0.to_string())
        }
        Value::TimeMillis(ms) => serde_json::Value::String(
            jiff::civil::Time::MIN
                .saturating_add(Span::new().milliseconds(*ms))
                .to_string(),
        ),
        Value::TimeMicros(us) => serde_json::Value::String(
            jiff::civil::Time::MIN
                .saturating_add(Span::new().microseconds(*us))
                .to_string(),
        ),
        Value::TimestampMillis(ms) => serde_json::Value::String(
            jiff::Timestamp::from_millisecond(*ms)
                .into_diagnostic()?
                .to_string(),
        ),
        Value::TimestampMicros(us) => serde_json::Value::String(
            jiff::Timestamp::from_millisecond(*us)
                .into_diagnostic()?
                .to_string(),
        ),
        Value::TimestampNanos(ns) => serde_json::Value::String(
            jiff::Timestamp::from_millisecond(*ns)
                .into_diagnostic()?
                .to_string(),
        ),
        Value::LocalTimestampMillis(ms) => serde_json::Value::String(
            jiff::Timestamp::from_millisecond(*ms)
                .into_diagnostic()?
                .to_zoned(TimeZone::try_system().unwrap_or(TimeZone::UTC))
                .to_string(),
        ),
        Value::LocalTimestampMicros(us) => serde_json::Value::String(
            jiff::Timestamp::from_microsecond(*us)
                .into_diagnostic()?
                .to_zoned(TimeZone::try_system().unwrap_or(TimeZone::UTC))
                .to_string(),
        ),
        Value::LocalTimestampNanos(ns) => serde_json::Value::String(
            jiff::Timestamp::from_nanosecond((*ns).into())
                .into_diagnostic()?
                .to_zoned(TimeZone::try_system().unwrap_or(TimeZone::UTC))
                .to_string(),
        ),
        Value::Duration(duration) => serde_json::Value::String(
            (jiff::Span::new()
                .months(u32::from(duration.months()))
                .checked_add(jiff::Span::new().days(u32::from(duration.days())))
                .into_diagnostic()?
                .checked_add(jiff::Span::new().milliseconds(u32::from(duration.millis())))
                .into_diagnostic()?)
            .to_string(),
        ),
    })
}
