use std::io::{self, Read, Seek, SeekFrom};

use serde::de::{Error as _, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

const MAXIMUM_JSON_CONTAINERS: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ArtifactJsonFailure {
    Invalid,
    Noncanonical,
    Unavailable,
}

pub(super) fn validate(reader: &mut (impl Read + Seek)) -> Result<(), ArtifactJsonFailure> {
    reader
        .seek(SeekFrom::Start(0))
        .map_err(|_| ArtifactJsonFailure::Unavailable)?;
    let mut prefix = [0_u8; 3];
    let prefix_read = reader
        .read(&mut prefix)
        .map_err(|_| ArtifactJsonFailure::Unavailable)?;
    if prefix_read == prefix.len() && prefix == [0xef, 0xbb, 0xbf] {
        return Err(ArtifactJsonFailure::Noncanonical);
    }

    reader
        .seek(SeekFrom::Start(0))
        .map_err(|_| ArtifactJsonFailure::Unavailable)?;
    let mut compact = CompactJsonReader::new(reader);
    let syntax_result = {
        let mut deserializer = serde_json::Deserializer::from_reader(&mut compact);
        deserializer.disable_recursion_limit();
        match IgnoredAny::deserialize(&mut deserializer) {
            Ok(_) => deserializer.end(),
            Err(error) => Err(error),
        }
    };
    if let Err(error) = syntax_result {
        return Err(if compact.depth_exceeded {
            ArtifactJsonFailure::Invalid
        } else if error.is_io() {
            ArtifactJsonFailure::Unavailable
        } else {
            ArtifactJsonFailure::Invalid
        });
    }
    if compact.noncanonical {
        return Err(ArtifactJsonFailure::Noncanonical);
    }

    compact
        .inner
        .seek(SeekFrom::Start(0))
        .map_err(|_| ArtifactJsonFailure::Unavailable)?;
    let mut deserializer = serde_json::Deserializer::from_reader(compact.inner);
    deserializer.disable_recursion_limit();
    OrderedUniqueValue::deserialize(&mut deserializer)
        .and_then(|_| deserializer.end())
        .map_err(|error| {
            if error.is_io() {
                ArtifactJsonFailure::Unavailable
            } else {
                ArtifactJsonFailure::Noncanonical
            }
        })
}

struct CompactJsonReader<Reader> {
    inner: Reader,
    in_string: bool,
    escaped: bool,
    depth: usize,
    depth_exceeded: bool,
    noncanonical: bool,
}

impl<Reader> CompactJsonReader<Reader> {
    fn new(inner: Reader) -> Self {
        Self {
            inner,
            in_string: false,
            escaped: false,
            depth: 0,
            depth_exceeded: false,
            noncanonical: false,
        }
    }
}

impl<Reader: Read> Read for CompactJsonReader<Reader> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(bytes)?;
        for byte in &bytes[..read] {
            if self.in_string {
                if self.escaped {
                    self.escaped = false;
                } else if *byte == b'\\' {
                    self.escaped = true;
                } else if *byte == b'"' {
                    self.in_string = false;
                }
            } else if *byte == b'"' {
                self.in_string = true;
            } else if matches!(*byte, b'[' | b'{') {
                self.depth = self.depth.saturating_add(1);
                if self.depth > MAXIMUM_JSON_CONTAINERS {
                    self.depth_exceeded = true;
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "JSON container limit exceeded",
                    ));
                }
            } else if matches!(*byte, b']' | b'}') {
                self.depth = self.depth.saturating_sub(1);
            } else if byte.is_ascii_whitespace() {
                self.noncanonical = true;
            }
        }
        Ok(read)
    }
}

struct OrderedUniqueValue;

impl<'de> Deserialize<'de> for OrderedUniqueValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(OrderedUniqueValueVisitor)
    }
}

struct OrderedUniqueValueVisitor;

impl<'de> Visitor<'de> for OrderedUniqueValueVisitor {
    type Value = OrderedUniqueValue;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("compact JSON with ordered unique object members")
    }

    fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E> {
        Ok(OrderedUniqueValue)
    }

    fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E> {
        Ok(OrderedUniqueValue)
    }

    fn visit_u64<E>(self, _value: u64) -> Result<Self::Value, E> {
        Ok(OrderedUniqueValue)
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E> {
        Ok(OrderedUniqueValue)
    }

    fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E> {
        Ok(OrderedUniqueValue)
    }

    fn visit_string<E>(self, _value: String) -> Result<Self::Value, E> {
        Ok(OrderedUniqueValue)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(OrderedUniqueValue)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(OrderedUniqueValue)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while sequence.next_element::<OrderedUniqueValue>()?.is_some() {}
        Ok(OrderedUniqueValue)
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut previous: Option<String> = None;
        while let Some(name) = map.next_key::<String>()? {
            if previous
                .as_ref()
                .is_some_and(|previous| previous.as_bytes() >= name.as_bytes())
            {
                return Err(A::Error::custom(
                    "unordered or duplicate JSON object member",
                ));
            }
            previous = Some(name);
            map.next_value::<OrderedUniqueValue>()?;
        }
        Ok(OrderedUniqueValue)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn distinguishes_invalid_json_from_noncanonical_valid_json() {
        let nested = format!("{}0{}", "[".repeat(128), "]".repeat(128));
        for valid in [
            br#"null"#.as_slice(),
            br#"{"a":[true,false],"z":1}"#.as_slice(),
            b"1e400".as_slice(),
            nested.as_bytes(),
        ] {
            assert_eq!(validate(&mut Cursor::new(valid)), Ok(()));
        }
        for noncanonical in [
            br#"{ "a":1}"#.as_slice(),
            br#"{"z":1,"a":2}"#.as_slice(),
            br#"{"a":1,"a":2}"#.as_slice(),
            b"\xef\xbb\xbfnull".as_slice(),
        ] {
            assert_eq!(
                validate(&mut Cursor::new(noncanonical)),
                Err(ArtifactJsonFailure::Noncanonical)
            );
        }
        let too_deep = format!("{}0{}", "[".repeat(129), "]".repeat(129));
        for invalid in [br#"{"a":}"#.as_slice(), too_deep.as_bytes()] {
            assert_eq!(
                validate(&mut Cursor::new(invalid)),
                Err(ArtifactJsonFailure::Invalid)
            );
        }
    }
}
