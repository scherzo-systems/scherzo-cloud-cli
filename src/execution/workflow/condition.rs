#[cfg(test)]
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::{Number, Value};

use super::validated::WorkflowNode;
use super::value::{CapturedJson, CapturedText};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JsonPointer {
    authored: Arc<str>,
    tokens: Arc<[Arc<str>]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InvalidJsonPointer;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum JsonSelection<'a> {
    Selected(&'a Value),
    Missing,
}

impl JsonPointer {
    pub(crate) fn parse(authored: impl Into<Arc<str>>) -> Result<Self, InvalidJsonPointer> {
        let authored = authored.into();
        let tokens = if authored.is_empty() {
            Vec::new()
        } else {
            authored
                .strip_prefix('/')
                .ok_or(InvalidJsonPointer)?
                .split('/')
                .map(decode_pointer_token)
                .collect::<Result<Vec<_>, _>>()?
        };
        Ok(Self {
            authored,
            tokens: tokens.into(),
        })
    }

    pub(crate) fn authored(&self) -> &str {
        &self.authored
    }

    pub(crate) fn tokens(&self) -> impl ExactSizeIterator<Item = &str> {
        self.tokens.iter().map(AsRef::as_ref)
    }

    pub(crate) fn select<'a>(&self, value: &'a Value) -> JsonSelection<'a> {
        let mut selected = value;
        for token in self.tokens.iter() {
            selected = match selected {
                Value::Object(object) => {
                    let Some(value) = object.get(token.as_ref()) else {
                        return JsonSelection::Missing;
                    };
                    value
                }
                Value::Array(array) => {
                    let Some(index) = parse_array_index(token) else {
                        return JsonSelection::Missing;
                    };
                    let Some(value) = array.get(index) else {
                        return JsonSelection::Missing;
                    };
                    value
                }
                Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
                    return JsonSelection::Missing;
                }
            };
        }
        JsonSelection::Selected(selected)
    }
}

fn decode_pointer_token(token: &str) -> Result<Arc<str>, InvalidJsonPointer> {
    if !token.contains('~') {
        return Ok(Arc::from(token));
    }

    let mut decoded = String::with_capacity(token.len());
    let mut characters = token.chars();
    while let Some(character) = characters.next() {
        if character != '~' {
            decoded.push(character);
            continue;
        }
        match characters.next() {
            Some('0') => decoded.push('~'),
            Some('1') => decoded.push('/'),
            Some(_) | None => return Err(InvalidJsonPointer),
        }
    }
    Ok(Arc::from(decoded))
}

fn parse_array_index(token: &str) -> Option<usize> {
    let bytes = token.as_bytes();
    match bytes {
        [b'0'] => Some(0),
        [first, rest @ ..]
            if first.is_ascii_digit() && *first != b'0' && rest.iter().all(u8::is_ascii_digit) =>
        {
            token.parse().ok()
        }
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConditionValueKind {
    Text,
    Json,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedOperand {
    kind: ConditionValueKind,
    value: ResolvedOperandValue,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ResolvedOperandValue {
    Reference {
        canonical_ref: Arc<str>,
        pointer: Option<JsonPointer>,
    },
    TextLiteral(Arc<str>),
    JsonLiteral(Arc<Value>),
}

impl ResolvedOperand {
    pub(crate) fn text_reference(canonical_ref: impl Into<Arc<str>>) -> Self {
        Self {
            kind: ConditionValueKind::Text,
            value: ResolvedOperandValue::Reference {
                canonical_ref: canonical_ref.into(),
                pointer: None,
            },
        }
    }

    pub(crate) fn json_reference(
        canonical_ref: impl Into<Arc<str>>,
        pointer: Option<JsonPointer>,
    ) -> Self {
        Self {
            kind: ConditionValueKind::Json,
            value: ResolvedOperandValue::Reference {
                canonical_ref: canonical_ref.into(),
                pointer,
            },
        }
    }

    pub(crate) fn text_literal(value: impl Into<Arc<str>>) -> Self {
        Self {
            kind: ConditionValueKind::Text,
            value: ResolvedOperandValue::TextLiteral(value.into()),
        }
    }

    pub(crate) fn json_literal(value: Arc<Value>) -> Self {
        Self {
            kind: ConditionValueKind::Json,
            value: ResolvedOperandValue::JsonLiteral(value),
        }
    }

    pub(crate) const fn kind(&self) -> ConditionValueKind {
        self.kind
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedSelector {
    canonical_ref: Arc<str>,
    pointer: JsonPointer,
}

impl ResolvedSelector {
    pub(crate) fn new(canonical_ref: impl Into<Arc<str>>, pointer: JsonPointer) -> Self {
        Self {
            canonical_ref: canonical_ref.into(),
            pointer,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TerminalDisposition {
    Succeeded,
    Failed,
    Skipped,
    Blocked,
    NotRun,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ResolvedPredicate {
    All(Arc<[ResolvedPredicate]>),
    Any(Arc<[ResolvedPredicate]>),
    Not(Box<ResolvedPredicate>),
    Equals([ResolvedOperand; 2]),
    Exists(ResolvedSelector),
    Disposition {
        node: WorkflowNode,
        is: TerminalDisposition,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PredicatePath(Arc<str>);

impl PredicatePath {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    fn root() -> Self {
        Self(Arc::from(""))
    }

    fn child(&self, segment: &str) -> Self {
        Self(Arc::from(format!("{}/{segment}", self.as_str())))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EvaluatedPredicate {
    pub(crate) path: PredicatePath,
    pub(crate) result: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum UnavailableConditionInput {
    Value { canonical_ref: Arc<str> },
    Disposition { node: WorkflowNode },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ConditionEvaluation {
    Passed,
    False {
        evaluated_predicates: Arc<[EvaluatedPredicate]>,
    },
    Failed {
        canonical_ref: Arc<str>,
        pointer: Arc<str>,
    },
    Unavailable {
        input: UnavailableConditionInput,
    },
}

#[derive(Clone, Copy)]
enum CapturedConditionValue<'a> {
    Text(&'a str),
    Json(&'a Value),
}

#[derive(Default)]
pub(crate) struct ConditionValues<'a> {
    values: BTreeMap<Arc<str>, CapturedConditionValue<'a>>,
    #[cfg(test)]
    accesses: RefCell<Vec<Arc<str>>>,
}

impl<'a> ConditionValues<'a> {
    pub(crate) fn insert_text(
        &mut self,
        canonical_ref: impl Into<Arc<str>>,
        value: &'a CapturedText,
    ) {
        self.insert_text_value(canonical_ref, value.as_str());
    }

    pub(crate) fn insert_text_value(&mut self, canonical_ref: impl Into<Arc<str>>, value: &'a str) {
        self.values
            .insert(canonical_ref.into(), CapturedConditionValue::Text(value));
    }

    pub(crate) fn insert_json(
        &mut self,
        canonical_ref: impl Into<Arc<str>>,
        value: &'a CapturedJson,
    ) {
        self.insert_json_value(canonical_ref, value.value());
    }

    pub(crate) fn insert_json_value(
        &mut self,
        canonical_ref: impl Into<Arc<str>>,
        value: &'a Value,
    ) {
        self.values
            .insert(canonical_ref.into(), CapturedConditionValue::Json(value));
    }

    fn get(&self, canonical_ref: &Arc<str>) -> Option<CapturedConditionValue<'a>> {
        #[cfg(test)]
        self.accesses.borrow_mut().push(Arc::clone(canonical_ref));
        self.values.get(canonical_ref).copied()
    }

    #[cfg(test)]
    fn accessed_references(&self) -> Vec<Arc<str>> {
        self.accesses.borrow().clone()
    }
}

#[derive(Default)]
pub(crate) struct ConditionDispositions {
    values: BTreeMap<WorkflowNode, TerminalDisposition>,
}

impl ConditionDispositions {
    pub(crate) fn insert(&mut self, node: WorkflowNode, disposition: TerminalDisposition) {
        self.values.insert(node, disposition);
    }

    fn get(&self, node: &WorkflowNode) -> Option<TerminalDisposition> {
        self.values.get(node).copied()
    }
}

pub(crate) fn evaluate(
    predicate: &ResolvedPredicate,
    values: &ConditionValues<'_>,
    dispositions: &ConditionDispositions,
) -> ConditionEvaluation {
    let mut evaluated_predicates = Vec::new();
    match evaluate_predicate(
        predicate,
        &PredicatePath::root(),
        values,
        dispositions,
        &mut evaluated_predicates,
    ) {
        Ok(true) => ConditionEvaluation::Passed,
        Ok(false) => ConditionEvaluation::False {
            evaluated_predicates: evaluated_predicates.into(),
        },
        Err(EvaluationFailure::PointerMissing {
            canonical_ref,
            pointer,
        }) => ConditionEvaluation::Failed {
            canonical_ref,
            pointer,
        },
        Err(EvaluationFailure::Unavailable(input)) => ConditionEvaluation::Unavailable { input },
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum EvaluationFailure {
    PointerMissing {
        canonical_ref: Arc<str>,
        pointer: Arc<str>,
    },
    Unavailable(UnavailableConditionInput),
}

fn evaluate_predicate(
    predicate: &ResolvedPredicate,
    path: &PredicatePath,
    values: &ConditionValues<'_>,
    dispositions: &ConditionDispositions,
    trace: &mut Vec<EvaluatedPredicate>,
) -> Result<bool, EvaluationFailure> {
    let result = match predicate {
        ResolvedPredicate::All(children) => {
            let mut result = true;
            for (index, child) in children.iter().enumerate() {
                let child_path = path.child(&format!("all/{index}"));
                if !evaluate_predicate(child, &child_path, values, dispositions, trace)? {
                    result = false;
                    break;
                }
            }
            result
        }
        ResolvedPredicate::Any(children) => {
            let mut result = false;
            for (index, child) in children.iter().enumerate() {
                let child_path = path.child(&format!("any/{index}"));
                if evaluate_predicate(child, &child_path, values, dispositions, trace)? {
                    result = true;
                    break;
                }
            }
            result
        }
        ResolvedPredicate::Not(child) => {
            !evaluate_predicate(child, &path.child("not"), values, dispositions, trace)?
        }
        ResolvedPredicate::Equals(operands) => {
            let left = resolve_operand(&operands[0], values)?;
            let right = resolve_operand(&operands[1], values)?;
            operands_equal(left, right)
        }
        ResolvedPredicate::Exists(selector) => {
            let value = resolve_json_reference(&selector.canonical_ref, values)?;
            matches!(selector.pointer.select(value), JsonSelection::Selected(_))
        }
        ResolvedPredicate::Disposition { node, is } => {
            dispositions.get(node).ok_or_else(|| {
                EvaluationFailure::Unavailable(UnavailableConditionInput::Disposition {
                    node: node.clone(),
                })
            })? == *is
        }
    };
    trace.push(EvaluatedPredicate {
        path: path.clone(),
        result,
    });
    Ok(result)
}

#[derive(Clone, Copy)]
enum SelectedOperand<'a> {
    Text(&'a str),
    Json(&'a Value),
}

fn resolve_operand<'a>(
    operand: &'a ResolvedOperand,
    values: &'a ConditionValues<'a>,
) -> Result<SelectedOperand<'a>, EvaluationFailure> {
    match &operand.value {
        ResolvedOperandValue::TextLiteral(value) => Ok(SelectedOperand::Text(value)),
        ResolvedOperandValue::JsonLiteral(value) => Ok(SelectedOperand::Json(value)),
        ResolvedOperandValue::Reference {
            canonical_ref,
            pointer,
        } => match (operand.kind, values.get(canonical_ref)) {
            (ConditionValueKind::Text, Some(CapturedConditionValue::Text(value))) => {
                Ok(SelectedOperand::Text(value))
            }
            (ConditionValueKind::Json, Some(CapturedConditionValue::Json(value))) => {
                let selected = if let Some(pointer) = pointer {
                    match pointer.select(value) {
                        JsonSelection::Selected(selected) => selected,
                        JsonSelection::Missing => {
                            return Err(EvaluationFailure::PointerMissing {
                                canonical_ref: Arc::clone(canonical_ref),
                                pointer: Arc::clone(&pointer.authored),
                            });
                        }
                    }
                } else {
                    value
                };
                Ok(SelectedOperand::Json(selected))
            }
            (ConditionValueKind::Text | ConditionValueKind::Json, None)
            | (ConditionValueKind::Text, Some(CapturedConditionValue::Json(_)))
            | (ConditionValueKind::Json, Some(CapturedConditionValue::Text(_))) => Err(
                EvaluationFailure::Unavailable(UnavailableConditionInput::Value {
                    canonical_ref: Arc::clone(canonical_ref),
                }),
            ),
        },
    }
}

fn resolve_json_reference<'a>(
    canonical_ref: &Arc<str>,
    values: &'a ConditionValues<'a>,
) -> Result<&'a Value, EvaluationFailure> {
    match values.get(canonical_ref) {
        Some(CapturedConditionValue::Json(value)) => Ok(value),
        Some(CapturedConditionValue::Text(_)) | None => Err(EvaluationFailure::Unavailable(
            UnavailableConditionInput::Value {
                canonical_ref: Arc::clone(canonical_ref),
            },
        )),
    }
}

fn operands_equal(left: SelectedOperand<'_>, right: SelectedOperand<'_>) -> bool {
    match (left, right) {
        (SelectedOperand::Text(left), SelectedOperand::Text(right)) => {
            left.as_bytes() == right.as_bytes()
        }
        (SelectedOperand::Json(left), SelectedOperand::Json(right)) => {
            json_semantically_equal(left, right)
        }
        (SelectedOperand::Text(_), SelectedOperand::Json(_))
        | (SelectedOperand::Json(_), SelectedOperand::Text(_)) => false,
    }
}

pub(super) fn json_semantically_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(left), Value::Bool(right)) => left == right,
        (Value::Number(left), Value::Number(right)) => numbers_semantically_equal(left, right),
        (Value::String(left), Value::String(right)) => left.as_bytes() == right.as_bytes(),
        (Value::Array(left), Value::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| json_semantically_equal(left, right))
        }
        (Value::Object(left), Value::Object(right)) => {
            left.len() == right.len()
                && left.iter().all(|(name, left)| {
                    right
                        .get(name)
                        .is_some_and(|right| json_semantically_equal(left, right))
                })
        }
        (Value::Null, _)
        | (Value::Bool(_), _)
        | (Value::Number(_), _)
        | (Value::String(_), _)
        | (Value::Array(_), _)
        | (Value::Object(_), _) => false,
    }
}

fn numbers_semantically_equal(left: &Number, right: &Number) -> bool {
    match (
        NormalizedNumber::parse(left.as_str()),
        NormalizedNumber::parse(right.as_str()),
    ) {
        (Some(left), Some(right)) => left == right,
        _ => false,
    }
}

#[derive(Debug, Eq, PartialEq)]
struct NormalizedNumber {
    negative: bool,
    significant_digits: Vec<u8>,
    decimal_exponent: SignedDecimal,
}

impl NormalizedNumber {
    fn parse(number: &str) -> Option<Self> {
        let (negative, unsigned) = match number.strip_prefix('-') {
            Some(unsigned) => (true, unsigned),
            None => (false, number),
        };
        let exponent_offset = unsigned.find(['e', 'E']);
        let (mantissa, explicit_exponent) = match exponent_offset {
            Some(offset) => (&unsigned[..offset], Some(&unsigned[offset + 1..])),
            None => (unsigned, None),
        };
        let (integer, fraction) = match mantissa.split_once('.') {
            Some((integer, fraction)) => (integer, fraction),
            None => (mantissa, ""),
        };
        if integer.is_empty()
            || !integer.bytes().all(|byte| byte.is_ascii_digit())
            || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        {
            return None;
        }

        let mut digits = Vec::with_capacity(integer.len().checked_add(fraction.len())?);
        digits.extend_from_slice(integer.as_bytes());
        digits.extend_from_slice(fraction.as_bytes());
        let leading_zeroes = digits.iter().take_while(|digit| **digit == b'0').count();
        if leading_zeroes == digits.len() {
            return Some(Self {
                negative: false,
                significant_digits: vec![b'0'],
                decimal_exponent: SignedDecimal::zero(),
            });
        }
        let trailing_zeroes = digits
            .iter()
            .rev()
            .take_while(|digit| **digit == b'0')
            .count();
        let significant_end = digits.len().checked_sub(trailing_zeroes)?;
        let significant_digits = digits[leading_zeroes..significant_end].to_vec();

        let mut decimal_exponent = match explicit_exponent {
            Some(exponent) => SignedDecimal::parse(exponent)?,
            None => SignedDecimal::zero(),
        };
        decimal_exponent.add_unsigned(trailing_zeroes, false);
        decimal_exponent.add_unsigned(fraction.len(), true);
        Some(Self {
            negative,
            significant_digits,
            decimal_exponent,
        })
    }
}

#[derive(Debug, Eq, PartialEq)]
struct SignedDecimal {
    negative: bool,
    magnitude: Vec<u8>,
}

impl SignedDecimal {
    fn zero() -> Self {
        Self {
            negative: false,
            magnitude: vec![0],
        }
    }

    fn parse(value: &str) -> Option<Self> {
        let (negative, digits) = if let Some(digits) = value.strip_prefix('-') {
            (true, digits)
        } else if let Some(digits) = value.strip_prefix('+') {
            (false, digits)
        } else {
            (false, value)
        };
        if digits.is_empty() || !digits.bytes().all(|digit| digit.is_ascii_digit()) {
            return None;
        }
        let first_nonzero = digits.as_bytes().iter().position(|digit| *digit != b'0');
        let Some(first_nonzero) = first_nonzero else {
            return Some(Self::zero());
        };
        Some(Self {
            negative,
            magnitude: digits.as_bytes()[first_nonzero..]
                .iter()
                .map(|digit| digit - b'0')
                .collect(),
        })
    }

    fn add_unsigned(&mut self, value: usize, negative: bool) {
        if value == 0 {
            return;
        }
        let other = Self {
            negative,
            magnitude: value
                .to_string()
                .bytes()
                .map(|digit| digit - b'0')
                .collect(),
        };
        self.add(other);
    }

    fn add(&mut self, other: Self) {
        if self.negative == other.negative {
            self.magnitude = add_decimal_magnitudes(&self.magnitude, &other.magnitude);
            return;
        }
        match compare_decimal_magnitudes(&self.magnitude, &other.magnitude) {
            std::cmp::Ordering::Greater => {
                self.magnitude = subtract_decimal_magnitudes(&self.magnitude, &other.magnitude);
            }
            std::cmp::Ordering::Less => {
                self.magnitude = subtract_decimal_magnitudes(&other.magnitude, &self.magnitude);
                self.negative = other.negative;
            }
            std::cmp::Ordering::Equal => *self = Self::zero(),
        }
    }
}

fn compare_decimal_magnitudes(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    left.len().cmp(&right.len()).then_with(|| left.cmp(right))
}

fn add_decimal_magnitudes(left: &[u8], right: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(left.len().max(right.len()).saturating_add(1));
    let mut left = left.iter().rev();
    let mut right = right.iter().rev();
    let mut carry = 0;
    loop {
        let left = left.next().copied();
        let right = right.next().copied();
        if left.is_none() && right.is_none() && carry == 0 {
            break;
        }
        let sum = left.unwrap_or(0) + right.unwrap_or(0) + carry;
        result.push(sum % 10);
        carry = sum / 10;
    }
    result.reverse();
    result
}

fn subtract_decimal_magnitudes(larger: &[u8], smaller: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(larger.len());
    let mut smaller = smaller.iter().rev();
    let mut borrow = 0_i8;
    for larger in larger.iter().rev() {
        let mut digit = i8::try_from(*larger).unwrap_or(0)
            - i8::try_from(smaller.next().copied().unwrap_or(0)).unwrap_or(0)
            - borrow;
        if digit < 0 {
            digit += 10;
            borrow = 1;
        } else {
            borrow = 0;
        }
        result.push(u8::try_from(digit).unwrap_or(0));
    }
    while result.len() > 1 && result.last() == Some(&0) {
        result.pop();
    }
    result.reverse();
    result
}

#[cfg(test)]
mod tests;
