use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use zeroize::Zeroizing;

#[derive(Clone)]
pub(crate) struct SecretToken(Zeroizing<String>);

pub(crate) trait TokenSource {
    fn expose(&self) -> &str;
    fn to_secret(&self) -> SecretToken;
}

impl SecretToken {
    pub(crate) fn new(value: String) -> Self {
        Self(Zeroizing::new(value))
    }

    pub(crate) fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl TokenSource for SecretToken {
    fn expose(&self) -> &str {
        self.expose()
    }

    fn to_secret(&self) -> SecretToken {
        self.clone()
    }
}

#[cfg(test)]
impl TokenSource for str {
    fn expose(&self) -> &str {
        self
    }

    fn to_secret(&self) -> SecretToken {
        SecretToken::new(self.to_owned())
    }
}

#[cfg(test)]
impl TokenSource for String {
    fn expose(&self) -> &str {
        self.as_str()
    }

    fn to_secret(&self) -> SecretToken {
        SecretToken::new(self.clone())
    }
}

#[cfg(test)]
impl PartialEq<str> for SecretToken {
    fn eq(&self, other: &str) -> bool {
        self.expose() == other
    }
}

impl fmt::Debug for SecretToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretToken([REDACTED])")
    }
}

impl Serialize for SecretToken {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.expose())
    }
}

impl<'de> Deserialize<'de> for SecretToken {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Self::new)
    }
}
