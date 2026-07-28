use serde_json::{Map, Number, Value};
use yaml_rust2::parser::{Event, MarkedEventReceiver, Parser, Tag};
use yaml_rust2::scanner::{Marker, Scanner, TScalarStyle, TokenType};

use super::DecodeFailure;

pub(super) fn parse(bytes: &[u8]) -> Result<Value, DecodeFailure> {
    let source = std::str::from_utf8(bytes).map_err(|_| DecodeFailure::malformed_yaml())?;
    reject_forbidden_tokens(source)?;

    let mut receiver = JsonEventReceiver::default();
    let mut parser = Parser::new_from_str(source);
    if parser.load(&mut receiver, true).is_err() {
        return Err(DecodeFailure::malformed_yaml());
    }
    if let Some(error) = receiver.error {
        return Err(error.into_decode_failure());
    }
    if receiver.document_count != 1 {
        return Err(DecodeFailure::forbidden_yaml());
    }

    receiver.document.ok_or_else(DecodeFailure::malformed_yaml)
}

fn reject_forbidden_tokens(source: &str) -> Result<(), DecodeFailure> {
    let mut scanner = Scanner::new(source.chars());
    loop {
        let token = scanner
            .next_token()
            .map_err(|_| DecodeFailure::malformed_yaml())?;
        let Some(token) = token else {
            return Ok(());
        };

        match token.1 {
            TokenType::Anchor(_) | TokenType::Alias(_) => {
                return Err(DecodeFailure::forbidden_yaml());
            }
            TokenType::VersionDirective(1, 2) => {}
            TokenType::VersionDirective(_, _) => return Err(DecodeFailure::forbidden_yaml()),
            _ => {}
        }
    }
}

#[derive(Default)]
struct JsonEventReceiver {
    stack: Vec<Container>,
    current_root: Option<Value>,
    document: Option<Value>,
    document_count: usize,
    error: Option<BuildError>,
}

enum Container {
    Sequence(Vec<Value>),
    Mapping {
        entries: Map<String, Value>,
        pending_key: Option<String>,
    },
}

#[derive(Clone, Copy)]
enum BuildError {
    Malformed,
    Forbidden,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CoreTag {
    NonSpecific,
    Null,
    Boolean,
    Integer,
    Float,
    String,
    Sequence,
    Mapping,
}

impl BuildError {
    fn into_decode_failure(self) -> DecodeFailure {
        match self {
            Self::Malformed => DecodeFailure::malformed_yaml(),
            Self::Forbidden => DecodeFailure::forbidden_yaml(),
        }
    }
}

impl MarkedEventReceiver for JsonEventReceiver {
    fn on_event(&mut self, event: Event, _mark: Marker) {
        if self.error.is_some() {
            return;
        }

        let result = match event {
            Event::Nothing | Event::StreamStart | Event::StreamEnd | Event::DocumentStart => Ok(()),
            Event::DocumentEnd => self.finish_document(),
            Event::Alias(_) => Err(BuildError::Forbidden),
            Event::Scalar(value, style, anchor, tag) => {
                if anchor != 0 {
                    Err(BuildError::Forbidden)
                } else {
                    resolve_scalar(value, style, tag.as_ref())
                        .and_then(|value| self.insert_node(value))
                }
            }
            Event::SequenceStart(anchor, tag) => {
                if anchor != 0 {
                    Err(BuildError::Forbidden)
                } else {
                    validate_collection_tag(tag.as_ref(), CoreTag::Sequence).map(|()| {
                        self.stack.push(Container::Sequence(Vec::new()));
                    })
                }
            }
            Event::SequenceEnd => self.finish_sequence(),
            Event::MappingStart(anchor, tag) => {
                if anchor != 0 {
                    Err(BuildError::Forbidden)
                } else {
                    validate_collection_tag(tag.as_ref(), CoreTag::Mapping).map(|()| {
                        self.stack.push(Container::Mapping {
                            entries: Map::new(),
                            pending_key: None,
                        });
                    })
                }
            }
            Event::MappingEnd => self.finish_mapping(),
        };

        if let Err(error) = result {
            self.error = Some(error);
        }
    }
}

impl JsonEventReceiver {
    fn finish_document(&mut self) -> Result<(), BuildError> {
        if !self.stack.is_empty() {
            return Err(BuildError::Malformed);
        }

        self.document_count = self.document_count.saturating_add(1);
        if self.document_count != 1 {
            return Err(BuildError::Forbidden);
        }
        self.document = Some(self.current_root.take().unwrap_or(Value::Null));
        Ok(())
    }

    fn finish_sequence(&mut self) -> Result<(), BuildError> {
        let Some(Container::Sequence(values)) = self.stack.pop() else {
            return Err(BuildError::Malformed);
        };
        self.insert_node(Value::Array(values))
    }

    fn finish_mapping(&mut self) -> Result<(), BuildError> {
        let Some(Container::Mapping {
            entries,
            pending_key,
        }) = self.stack.pop()
        else {
            return Err(BuildError::Malformed);
        };
        if pending_key.is_some() {
            return Err(BuildError::Malformed);
        }
        self.insert_node(Value::Object(entries))
    }

    fn insert_node(&mut self, value: Value) -> Result<(), BuildError> {
        match self.stack.last_mut() {
            Some(Container::Sequence(values)) => {
                values.push(value);
                Ok(())
            }
            Some(Container::Mapping {
                entries,
                pending_key,
            }) => {
                if let Some(key) = pending_key.take() {
                    if entries.contains_key(&key) {
                        return Err(BuildError::Forbidden);
                    }
                    entries.insert(key, value);
                    return Ok(());
                }

                let Value::String(key) = value else {
                    return Err(BuildError::Forbidden);
                };
                if key == "<<" {
                    return Err(BuildError::Forbidden);
                }
                *pending_key = Some(key);
                Ok(())
            }
            None => {
                if self.current_root.is_some() {
                    return Err(BuildError::Malformed);
                }
                self.current_root = Some(value);
                Ok(())
            }
        }
    }
}

const CORE_TAG_PREFIX: &str = "tag:yaml.org,2002:";

fn validate_collection_tag(tag: Option<&Tag>, expected: CoreTag) -> Result<(), BuildError> {
    let Some(tag) = tag else {
        return Ok(());
    };
    let tag = parse_core_tag(tag)?;
    if tag == CoreTag::NonSpecific || tag == expected {
        Ok(())
    } else {
        Err(BuildError::Forbidden)
    }
}

fn parse_core_tag(tag: &Tag) -> Result<CoreTag, BuildError> {
    if tag.handle.is_empty() && tag.suffix == "!" {
        return Ok(CoreTag::NonSpecific);
    }

    let suffix = if tag.handle == CORE_TAG_PREFIX {
        tag.suffix.as_str()
    } else if tag.handle.is_empty() {
        tag.suffix
            .strip_prefix(CORE_TAG_PREFIX)
            .ok_or(BuildError::Forbidden)?
    } else {
        return Err(BuildError::Forbidden);
    };

    match suffix {
        "null" => Ok(CoreTag::Null),
        "bool" => Ok(CoreTag::Boolean),
        "int" => Ok(CoreTag::Integer),
        "float" => Ok(CoreTag::Float),
        "str" => Ok(CoreTag::String),
        "seq" => Ok(CoreTag::Sequence),
        "map" => Ok(CoreTag::Mapping),
        _ => Err(BuildError::Forbidden),
    }
}

fn resolve_scalar(
    value: String,
    style: TScalarStyle,
    tag: Option<&Tag>,
) -> Result<Value, BuildError> {
    if let Some(tag) = tag {
        return resolve_tagged_scalar(value, parse_core_tag(tag)?);
    }
    if style != TScalarStyle::Plain {
        return Ok(Value::String(value));
    }

    if is_core_null(&value) {
        return Ok(Value::Null);
    }
    if let Some(boolean) = parse_core_boolean(&value) {
        return Ok(Value::Bool(boolean));
    }
    if let Some(integer) = parse_core_integer(&value)? {
        return Ok(Value::Number(integer));
    }
    if let Some(float) = parse_core_float(&value, false)? {
        return Ok(Value::Number(float));
    }

    Ok(Value::String(value))
}

fn resolve_tagged_scalar(value: String, tag: CoreTag) -> Result<Value, BuildError> {
    match tag {
        CoreTag::NonSpecific | CoreTag::String => Ok(Value::String(value)),
        CoreTag::Null if is_core_null(&value) => Ok(Value::Null),
        CoreTag::Boolean => parse_core_boolean(&value)
            .map(Value::Bool)
            .ok_or(BuildError::Forbidden),
        CoreTag::Integer => parse_core_integer(&value)?
            .map(Value::Number)
            .ok_or(BuildError::Forbidden),
        CoreTag::Float => parse_core_float(&value, true)?
            .map(Value::Number)
            .ok_or(BuildError::Forbidden),
        CoreTag::Null | CoreTag::Sequence | CoreTag::Mapping => Err(BuildError::Forbidden),
    }
}

fn is_core_null(value: &str) -> bool {
    matches!(value, "" | "~" | "null" | "Null" | "NULL")
}

fn parse_core_boolean(value: &str) -> Option<bool> {
    match value {
        "true" | "True" | "TRUE" => Some(true),
        "false" | "False" | "FALSE" => Some(false),
        _ => None,
    }
}

fn parse_core_integer(value: &str) -> Result<Option<Number>, BuildError> {
    let (negative, unsigned) = without_sign(value);
    let has_sign = unsigned.len() != value.len();
    let (radix, digits) = if let Some(digits) = unsigned.strip_prefix("0o") {
        if has_sign {
            return Ok(None);
        }
        (8, digits)
    } else if let Some(digits) = unsigned.strip_prefix("0x") {
        if has_sign {
            return Ok(None);
        }
        (16, digits)
    } else {
        (10, unsigned)
    };

    let valid_digits = match radix {
        8 => digits.bytes().all(|byte| matches!(byte, b'0'..=b'7')),
        10 => digits.bytes().all(|byte| byte.is_ascii_digit()),
        16 => digits.bytes().all(|byte| byte.is_ascii_hexdigit()),
        _ => false,
    };
    if digits.is_empty() || !valid_digits {
        return Ok(None);
    }

    let magnitude = u64::from_str_radix(digits, radix).map_err(|_| BuildError::Forbidden)?;
    if negative {
        const MIN_MAGNITUDE: u64 = 9_223_372_036_854_775_808;
        let signed = if magnitude == MIN_MAGNITUDE {
            i64::MIN
        } else {
            -i64::try_from(magnitude).map_err(|_| BuildError::Forbidden)?
        };
        Ok(Some(Number::from(signed)))
    } else {
        Ok(Some(Number::from(magnitude)))
    }
}

fn parse_core_float(value: &str, allow_integer: bool) -> Result<Option<Number>, BuildError> {
    if is_non_json_core_float(value) {
        return Err(BuildError::Forbidden);
    }
    if !(is_core_float(value) || allow_integer && is_core_decimal_integer(value)) {
        return Ok(None);
    }

    value
        .parse::<f64>()
        .ok()
        .filter(|number| number.is_finite())
        .and_then(|number| number_from_finite_float(value, number))
        .map(Some)
        .ok_or(BuildError::Forbidden)
}

fn number_from_finite_float(source: &str, value: f64) -> Option<Number> {
    if value.fract() == 0.0 && core_float_is_integral(source) {
        let decimal = value.to_string();
        if let Ok(integer) = decimal.parse::<i64>() {
            return Some(Number::from(integer));
        }
        if let Ok(integer) = decimal.parse::<u64>() {
            return Some(Number::from(integer));
        }
    }
    Number::from_f64(value)
}

fn core_float_is_integral(value: &str) -> bool {
    let (_, unsigned) = without_sign(value);
    let mut parts = unsigned.split(['e', 'E']);
    let base = parts.next().unwrap_or_default();
    let exponent = parts.next().unwrap_or("0");
    let fractional_digits = base
        .split_once('.')
        .map_or(0, |(_, fraction)| fraction.len());
    let (negative_exponent, magnitude) = without_sign(exponent);
    let Ok(magnitude) = magnitude.parse::<usize>() else {
        return if negative_exponent {
            coefficient_is_zero(base)
        } else {
            true
        };
    };

    let trailing_fractional_digits = if negative_exponent {
        let Some(digits) = fractional_digits.checked_add(magnitude) else {
            return coefficient_is_zero(base);
        };
        digits
    } else {
        fractional_digits.saturating_sub(magnitude)
    };
    coefficient_has_trailing_zeroes(base, trailing_fractional_digits)
}

fn coefficient_has_trailing_zeroes(base: &str, count: usize) -> bool {
    let digit_count = base.len() - usize::from(base.contains('.'));
    if count >= digit_count {
        return coefficient_is_zero(base);
    }
    base.bytes()
        .filter(|byte| *byte != b'.')
        .rev()
        .take(count)
        .all(|byte| byte == b'0')
}

fn coefficient_is_zero(base: &str) -> bool {
    base.bytes().all(|byte| matches!(byte, b'.' | b'0'))
}

fn is_core_decimal_integer(value: &str) -> bool {
    let (_, unsigned) = without_sign(value);
    !unsigned.is_empty() && unsigned.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_non_json_core_float(value: &str) -> bool {
    let (_, unsigned) = without_sign(value);
    matches!(unsigned, ".inf" | ".Inf" | ".INF") || matches!(value, ".nan" | ".NaN" | ".NAN")
}

fn is_core_float(value: &str) -> bool {
    let (_, unsigned) = without_sign(value);
    let mut exponent_parts = unsigned.split(['e', 'E']);
    let Some(base) = exponent_parts.next() else {
        return false;
    };
    let exponent = exponent_parts.next();
    if exponent_parts.next().is_some() {
        return false;
    }

    if let Some(exponent) = exponent {
        let (_, digits) = without_sign(exponent);
        if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            return false;
        }
    }

    let valid_base = if let Some(fraction) = base.strip_prefix('.') {
        !fraction.is_empty() && fraction.bytes().all(|byte| byte.is_ascii_digit())
    } else if let Some((integer, fraction)) = base.split_once('.') {
        !integer.is_empty()
            && integer.bytes().all(|byte| byte.is_ascii_digit())
            && fraction.bytes().all(|byte| byte.is_ascii_digit())
    } else {
        !base.is_empty() && base.bytes().all(|byte| byte.is_ascii_digit())
    };

    valid_base && (base.contains('.') || exponent.is_some())
}

fn without_sign(value: &str) -> (bool, &str) {
    if let Some(unsigned) = value.strip_prefix('-') {
        (true, unsigned)
    } else if let Some(unsigned) = value.strip_prefix('+') {
        (false, unsigned)
    } else {
        (false, value)
    }
}
