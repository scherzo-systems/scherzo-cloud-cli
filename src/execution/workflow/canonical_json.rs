use std::io::{self, Write};
use std::sync::Arc;

use serde::Serialize;
use serde::ser::{SerializeMap, SerializeSeq, Serializer};
use serde_json::Value;

pub(crate) struct CanonicalJson<'a>(&'a Value);

impl Serialize for CanonicalJson<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.0 {
            Value::Null => serializer.serialize_unit(),
            Value::Bool(value) => serializer.serialize_bool(*value),
            Value::Number(value) => value.serialize(serializer),
            Value::String(value) => serializer.serialize_str(value),
            Value::Array(values) => {
                let mut sequence = serializer.serialize_seq(Some(values.len()))?;
                for value in values {
                    sequence.serialize_element(&CanonicalJson(value))?;
                }
                sequence.end()
            }
            Value::Object(values) => {
                let mut entries = values.iter().collect::<Vec<_>>();
                entries.sort_unstable_by(|(left, _), (right, _)| {
                    left.as_bytes().cmp(right.as_bytes())
                });
                let mut map = serializer.serialize_map(Some(entries.len()))?;
                for (key, value) in entries {
                    map.serialize_entry(key, &CanonicalJson(value))?;
                }
                map.end()
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CanonicalJsonError {
    SizeLimitExceeded,
    SerializationFailed,
}

pub(crate) fn to_writer(writer: impl Write, value: &Value) -> Result<(), serde_json::Error> {
    serde_json::to_writer(writer, &CanonicalJson(value))
}

pub(crate) fn to_bounded_bytes(
    value: &Value,
    maximum_bytes: u64,
) -> Result<Arc<[u8]>, CanonicalJsonError> {
    serialize_bounded(value, Vec::new(), maximum_bytes).map(|writer| Arc::from(writer.inner))
}

fn serialize_bounded<Writer>(
    value: &Value,
    inner: Writer,
    maximum_bytes: u64,
) -> Result<BoundedWriter<Writer>, CanonicalJsonError>
where
    Writer: Write,
{
    let mut writer = BoundedWriter {
        inner,
        bytes: 0,
        maximum_bytes,
        exceeded: false,
    };
    if to_writer(&mut writer, value).is_err() {
        return Err(if writer.exceeded {
            CanonicalJsonError::SizeLimitExceeded
        } else {
            CanonicalJsonError::SerializationFailed
        });
    }
    Ok(writer)
}

struct BoundedWriter<Writer> {
    inner: Writer,
    bytes: u64,
    maximum_bytes: u64,
    exceeded: bool,
}

impl<Writer> Write for BoundedWriter<Writer>
where
    Writer: Write,
{
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let length = u64::try_from(bytes.len())
            .map_err(|_| io::Error::other("canonical JSON size is unavailable"))?;
        let Some(updated) = self.bytes.checked_add(length) else {
            self.exceeded = true;
            return Err(io::Error::other("canonical JSON exceeds its limit"));
        };
        if updated > self.maximum_bytes {
            self.exceeded = true;
            return Err(io::Error::other("canonical JSON exceeds its limit"));
        }
        self.inner.write_all(bytes)?;
        self.bytes = updated;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
