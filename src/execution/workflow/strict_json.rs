use std::collections::BTreeSet;
use std::fmt;

use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::Value;

/// Decodes JSON only after proving that every object has unique decoded keys.
///
/// `serde_json::Value` keeps only one member when an object repeats a key. Authority-bearing
/// protocol inputs must reject that ambiguity before constructing a `Value`.
pub(crate) fn from_slice(bytes: &[u8]) -> Result<Value, serde_json::Error> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    RejectDuplicateKeys::deserialize(&mut deserializer)?;
    deserializer.end()?;
    serde_json::from_slice(bytes)
}

pub(crate) fn from_str(source: &str) -> Result<Value, serde_json::Error> {
    from_slice(source.as_bytes())
}

struct RejectDuplicateKeys;

impl<'de> Deserialize<'de> for RejectDuplicateKeys {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(RejectDuplicateKeysVisitor)
    }
}

struct RejectDuplicateKeysVisitor;

impl<'de> Visitor<'de> for RejectDuplicateKeysVisitor {
    type Value = RejectDuplicateKeys;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("JSON with unique object keys")
    }

    fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E> {
        Ok(RejectDuplicateKeys)
    }

    fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E> {
        Ok(RejectDuplicateKeys)
    }

    fn visit_i128<E>(self, _value: i128) -> Result<Self::Value, E> {
        Ok(RejectDuplicateKeys)
    }

    fn visit_u64<E>(self, _value: u64) -> Result<Self::Value, E> {
        Ok(RejectDuplicateKeys)
    }

    fn visit_u128<E>(self, _value: u128) -> Result<Self::Value, E> {
        Ok(RejectDuplicateKeys)
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E> {
        Ok(RejectDuplicateKeys)
    }

    fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E> {
        Ok(RejectDuplicateKeys)
    }

    fn visit_string<E>(self, _value: String) -> Result<Self::Value, E> {
        Ok(RejectDuplicateKeys)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(RejectDuplicateKeys)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        RejectDuplicateKeys::deserialize(deserializer)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(RejectDuplicateKeys)
    }

    fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        RejectDuplicateKeys::deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while sequence.next_element::<RejectDuplicateKeys>()?.is_some() {}
        Ok(RejectDuplicateKeys)
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut keys = BTreeSet::new();
        while let Some(key) = map.next_key::<String>()? {
            if !keys.insert(key) {
                return Err(de::Error::custom("duplicate JSON object key"));
            }
            map.next_value::<RejectDuplicateKeys>()?;
        }
        Ok(RejectDuplicateKeys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_duplicate_keys_before_value_normalization() {
        for bytes in [
            br#"{"decision":"recheck","decision":"gave_up"}"#.as_slice(),
            br#"{"result":{"summary":"first","summary":"second"}}"#.as_slice(),
            br#"{"\u0061":1,"a":2}"#.as_slice(),
        ] {
            assert!(from_slice(bytes).is_err());
        }
    }

    #[test]
    fn retains_ordinary_json_value_semantics() {
        let source = br#"{"array":[null,true,-1,2.5],"large":1234567890123456789012345678901234567890,"nested":{"value":"ok"}}"#;
        assert_eq!(
            from_slice(source).unwrap(),
            serde_json::from_slice::<Value>(source).unwrap()
        );
    }
}
