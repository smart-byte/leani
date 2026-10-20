//! Event ABI fragments: parsing them with Alloy, decoding logs strictly, and
//! rendering decoded values as exact strings.

use std::collections::{BTreeMap, BTreeSet};
use std::iter;

use alloy_dyn_abi::{DynSolType, DynSolValue, Specifier};
use alloy_json_abi::{Event, EventParam, Param};
use alloy_primitives::{I256, U256};

/// Array and tuple levels one parameter may nest. Real events use one or
/// two; six keeps Alloy's decoder recursion limit of 16 out of reach, since a
/// dynamic array costs it two levels and a `bytes` tail one more.
const MAX_NESTING: usize = 6;

/// Values one element of a dynamic array may hold, counting each nested
/// array and tuple as one more. Alloy decodes an array by copying a full
/// template of its element for every element, and builds one even for an
/// empty array, so a larger element would let a short log allocate far more
/// than it holds. Real events hold a few values per element.
const MAX_ELEMENT_VALUES: usize = 64;

/// A parsed event fragment.
#[derive(Clone, Debug)]
pub(crate) struct EventAbi {
    pub(crate) name: String,
    pub(crate) fields: Vec<AbiField>,
    /// The non-indexed parameters, as Alloy decodes the log data.
    body: DynSolType,
}

#[derive(Clone, Debug)]
pub(crate) struct AbiField {
    pub(crate) name: String,
    pub(crate) ty: AbiType,
    pub(crate) indexed: bool,
}

/// A parameter type, with the component names that flattened keys and JSON
/// objects use.
#[derive(Clone, Debug)]
pub(crate) enum AbiType {
    Word(AbiKind),
    Bytes,
    String,
    Array(Box<AbiType>, Option<usize>),
    Tuple(Vec<(String, AbiType)>),
}

/// A value type: one ABI word, as a topic or in the log data.
#[derive(Clone, Copy, Debug)]
pub(crate) enum AbiKind {
    Address,
    Uint(u16),
    Int(u16),
    Bool,
    FixedBytes(u8),
}

/// Parses a human-readable fragment, such as
/// `event Trade(address indexed owner, bytes orderUid)`, or one JSON ABI
/// event object.
pub(crate) fn parse(abi: &str) -> Result<EventAbi, String> {
    let abi = abi.trim();
    let event = match abi.as_bytes().first() {
        Some(b'{') => serde_json::from_str::<Event>(abi)
            .map_err(|error| format!("event ABI JSON is invalid: {error}"))?,
        Some(b'[') => return Err("give one event ABI object, not an ABI array".to_owned()),
        _ => parse_readable(&normalize(abi))?,
    };
    if event.anonymous {
        return Err(
            "anonymous events are not supported; `abi = \"log0\"` stores topicless logs raw"
                .to_owned(),
        );
    }
    if !valid_identifier(&event.name) {
        return Err("event name is not a Solidity identifier".to_owned());
    }
    let mut fields = Vec::with_capacity(event.inputs.len());
    for input in &event.inputs {
        // Checked before Alloy builds the type, whose nesting costs stack.
        if !shallow(&input.ty, &input.components, 0) {
            return Err(too_deep());
        }
        let resolved = input
            .resolve()
            .map_err(|error| format!("event parameter `{}`: {error}", input.ty))?;
        fields.push(AbiField {
            name: field_name(&input.name, &input.ty)?,
            ty: abi_type(&resolved, &input.components)?,
            indexed: input.indexed,
        });
    }
    if !unique(fields.iter().map(|field| field.name.as_str())) {
        return Err("event field names must be unique".to_owned());
    }
    if fields.iter().filter(|field| field.indexed).count() > 3 {
        return Err("an event cannot contain more than three indexed fields".to_owned());
    }
    let body = DynSolType::Tuple(
        fields
            .iter()
            .filter(|field| !field.indexed)
            .map(|field| field.ty.decode_type())
            .collect(),
    );
    if !elements_fit(&body) {
        return Err(format!(
            "event ABI array elements may hold at most {MAX_ELEMENT_VALUES} values, counting each nested array and tuple as one"
        ));
    }
    Ok(EventAbi {
        name: event.name,
        fields,
        body,
    })
}

impl EventAbi {
    /// The canonical signature that topic zero hashes.
    pub(crate) fn signature(&self) -> String {
        let types = self
            .fields
            .iter()
            .map(|field| field.ty.canonical())
            .collect::<Vec<_>>();
        format!("{}({})", self.name, types.join(","))
    }

    /// The keys of the decoded values: a non-indexed tuple flattens to
    /// `name.component` keys, and every other parameter keeps its name.
    pub(crate) fn value_keys(&self) -> BTreeSet<String> {
        fn insert(name: String, ty: &AbiType, keys: &mut BTreeSet<String>) {
            if let AbiType::Tuple(components) = ty {
                for (component, ty) in components {
                    insert(format!("{name}.{component}"), ty, keys);
                }
            } else {
                keys.insert(name);
            }
        }
        let mut keys = BTreeSet::new();
        for field in &self.fields {
            if field.indexed {
                keys.insert(field.name.clone());
            } else {
                insert(field.name.clone(), &field.ty, &mut keys);
            }
        }
        keys
    }

    /// Decodes one log of this event.
    ///
    /// Only the canonical encoding decodes: the one Solidity's encoder
    /// produces, with minimal offsets, zero padding, and no trailing bytes.
    /// Alloy's decoder follows offsets leniently, so the layout is checked
    /// before it decodes, the data must re-encode to the same bytes, and
    /// each value word must fit its type.
    pub(crate) fn decode(
        &self,
        topics: &[[u8; 32]],
        data: &[u8],
    ) -> Result<BTreeMap<String, String>, String> {
        let indexed = self.fields.iter().filter(|field| field.indexed).count();
        if topics.len() != indexed.saturating_add(1) {
            return Err(format!(
                "expected {} topics, received {}",
                indexed.saturating_add(1),
                topics.len()
            ));
        }
        // Offsets sharing one tail would decode to far more values than the
        // log holds, so they fail here, before Alloy allocates anything.
        // Alloy's strict mode rejects them too, but only for compile-time
        // types so far.
        if encoded_len(&self.body, data, 0) != Some(data.len()) {
            return Err("log data is not the canonical ABI layout".to_owned());
        }
        let body = self
            .body
            .abi_decode_sequence(data)
            .map_err(|error| error.to_string())?;
        if body.abi_encode_sequence().as_deref() != Some(data) {
            return Err("log data is not the canonical ABI encoding".to_owned());
        }
        let DynSolValue::Tuple(body) = body else {
            return Err("log data did not decode as a tuple".to_owned());
        };
        let mut topics = topics.iter().skip(1);
        let mut body = body.iter();
        let mut values = BTreeMap::new();
        for field in &self.fields {
            if field.indexed {
                let topic = topics.next().ok_or("a topic is missing")?;
                // A value type's topic is its word. Any other indexed value
                // is stored only as the keccak hash of its encoding.
                let value = match field.ty {
                    AbiType::Word(kind) => decode_word(kind, *topic)?,
                    _ => format!("0x{}", hex::encode(topic)),
                };
                values.insert(field.name.clone(), value);
            } else {
                let value = body.next().ok_or("a data value is missing")?;
                render(field.name.clone(), &field.ty, value, &mut values)?;
            }
        }
        Ok(values)
    }
}

impl AbiType {
    fn canonical(&self) -> String {
        match self {
            Self::Word(kind) => kind.canonical(),
            Self::Bytes => "bytes".to_owned(),
            Self::String => "string".to_owned(),
            Self::Array(element, None) => format!("{}[]", element.canonical()),
            Self::Array(element, Some(size)) => format!("{}[{size}]", element.canonical()),
            Self::Tuple(components) => format!(
                "({})",
                components
                    .iter()
                    .map(|(_, ty)| ty.canonical())
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        }
    }

    fn decode_type(&self) -> DynSolType {
        match self {
            Self::Word(AbiKind::Address) => DynSolType::Address,
            Self::Word(AbiKind::Uint(bits)) => DynSolType::Uint(usize::from(*bits)),
            Self::Word(AbiKind::Int(bits)) => DynSolType::Int(usize::from(*bits)),
            Self::Word(AbiKind::Bool) => DynSolType::Bool,
            Self::Word(AbiKind::FixedBytes(size)) => DynSolType::FixedBytes(usize::from(*size)),
            // `string` and `bytes` encode alike. Decoding a string as bytes
            // keeps invalid UTF-8 exact for the re-encoding check, and
            // rendering then replaces it, as Alloy and graph-node do.
            Self::Bytes | Self::String => DynSolType::Bytes,
            Self::Array(element, None) => DynSolType::Array(Box::new(element.decode_type())),
            Self::Array(element, Some(size)) => {
                DynSolType::FixedArray(Box::new(element.decode_type()), *size)
            }
            Self::Tuple(components) => {
                DynSolType::Tuple(components.iter().map(|(_, ty)| ty.decode_type()).collect())
            }
        }
    }
}

impl AbiKind {
    fn canonical(self) -> String {
        match self {
            Self::Address => "address".to_owned(),
            Self::Uint(bits) => format!("uint{bits}"),
            Self::Int(bits) => format!("int{bits}"),
            Self::Bool => "bool".to_owned(),
            Self::FixedBytes(bytes) => format!("bytes{bytes}"),
        }
    }
}

/// Parses a normalized human-readable fragment into Alloy's ABI model.
///
/// Alloy's own parser cannot name tuple components, and flattened keys need
/// those names, so this accepts `(uint256 amount, bool exact) params` and
/// `tuple(…)` as viem and ethers do. Alloy then resolves the type strings.
fn parse_readable(fragment: &str) -> Result<Event, String> {
    let fragment = fragment.strip_prefix("event ").unwrap_or(fragment);
    let open = fragment.find('(').ok_or("event ABI is missing `(`")?;
    let mut reader = Reader {
        text: fragment,
        at: open,
        depth: 0,
    };
    let inputs = reader
        .parameters(true)?
        .into_iter()
        .map(|(param, indexed)| EventParam {
            ty: param.ty,
            name: param.name,
            indexed,
            components: param.components,
            internal_type: None,
        })
        .collect();
    let anonymous = match &fragment[reader.at..] {
        "" => false,
        " anonymous" => true,
        _ => return Err("event ABI has trailing or unbalanced syntax".to_owned()),
    };
    Ok(Event {
        name: fragment[..open].to_owned(),
        inputs,
        anonymous,
    })
}

/// A cursor over a normalized fragment.
struct Reader<'a> {
    text: &'a str,
    at: usize,
    /// Open parameter lists, bounded so tuples cannot exhaust the stack.
    depth: usize,
}

impl Reader<'_> {
    fn eat(&mut self, token: &str) -> bool {
        let found = self.text[self.at..].starts_with(token);
        if found {
            self.at = self.at.saturating_add(token.len());
        }
        found
    }

    fn word(&mut self) -> &str {
        let start = self.at;
        let length = self.text[start..]
            .find(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
            .unwrap_or(self.text.len() - start);
        self.at = start.saturating_add(length);
        &self.text[start..self.at]
    }

    /// `(…)`: parameters with their `indexed` flag, which only top-level
    /// event parameters may carry.
    fn parameters(&mut self, top: bool) -> Result<Vec<(Param, bool)>, String> {
        let unbalanced = || "event ABI has trailing or unbalanced syntax".to_owned();
        if !self.eat("(") {
            return Err(unbalanced());
        }
        self.depth = self.depth.saturating_add(1);
        if self.depth > MAX_NESTING.saturating_add(1) {
            return Err(too_deep());
        }
        let mut parameters = Vec::new();
        if !self.eat(")") {
            loop {
                parameters.push(self.parameter(top)?);
                if self.eat(")") {
                    break;
                }
                if !self.eat(",") {
                    return Err(unbalanced());
                }
            }
        }
        self.depth = self.depth.saturating_sub(1);
        Ok(parameters)
    }

    /// `type [indexed] [name]`, where a type is an elementary name or a
    /// tuple, either with array suffixes.
    fn parameter(&mut self, top: bool) -> Result<(Param, bool), String> {
        let tuple = self.text[self.at..].starts_with('(') || self.eat("tuple");
        let (mut ty, components) = if tuple {
            let components = self
                .parameters(false)?
                .into_iter()
                .map(|(param, _)| param)
                .collect();
            ("tuple".to_owned(), components)
        } else {
            (self.word().to_owned(), Vec::new())
        };
        while self.text[self.at..].starts_with('[') {
            let end = self.text[self.at..]
                .find(']')
                .ok_or("event ABI has an unclosed array suffix")?;
            let suffix_end = self.at.saturating_add(end).saturating_add(1);
            ty.push_str(&self.text[self.at..suffix_end]);
            self.at = suffix_end;
        }
        // `type indexed` is an unnamed indexed parameter, not a parameter
        // named `indexed`.
        let indexed = top
            && self.text[self.at..]
                .strip_prefix(" indexed")
                .is_some_and(|rest| rest.is_empty() || rest.starts_with([' ', ',', ')']));
        if indexed {
            self.at = self.at.saturating_add(" indexed".len());
        }
        let name = if self.eat(" ") {
            self.word().to_owned()
        } else {
            String::new()
        };
        Ok((
            Param {
                ty,
                name,
                components,
                internal_type: None,
            },
            indexed,
        ))
    }
}

/// Today's syntax allowed any whitespace and a trailing `;`, so fragments
/// normalize to single spaces between words and none around punctuation.
fn normalize(abi: &str) -> String {
    let mut normalized = String::with_capacity(abi.len());
    for word in abi.trim_end_matches(';').split_whitespace() {
        let joined = normalized.is_empty()
            || normalized.ends_with(['(', '[', ','])
            || word.starts_with(['(', ')', '[', ']', ',']);
        if !joined {
            normalized.push(' ');
        }
        normalized.push_str(word);
    }
    normalized
}

#[allow(clippy::match_wildcard_for_single_variants)]
fn abi_type(ty: &DynSolType, components: &[Param]) -> Result<AbiType, String> {
    Ok(match ty {
        DynSolType::Address => AbiType::Word(AbiKind::Address),
        DynSolType::Bool => AbiType::Word(AbiKind::Bool),
        DynSolType::Uint(bits) => AbiType::Word(AbiKind::Uint(width(*bits)?)),
        DynSolType::Int(bits) => AbiType::Word(AbiKind::Int(width(*bits)?)),
        DynSolType::FixedBytes(size) => AbiType::Word(AbiKind::FixedBytes(
            u8::try_from(*size).map_err(|_| format!("unsupported ABI type `{ty}`"))?,
        )),
        DynSolType::Bytes => AbiType::Bytes,
        DynSolType::String => AbiType::String,
        DynSolType::Array(element) => {
            AbiType::Array(Box::new(abi_type(element, components)?), None)
        }
        DynSolType::FixedArray(_, 0) => {
            return Err(format!("ABI type `{ty}` has no elements"));
        }
        DynSolType::FixedArray(element, size) => {
            AbiType::Array(Box::new(abi_type(element, components)?), Some(*size))
        }
        DynSolType::Tuple(types) => {
            if types.is_empty() || types.len() != components.len() {
                return Err(format!("tuple ABI type `{ty}` needs named components"));
            }
            let mut fields = Vec::with_capacity(types.len());
            for (component, ty) in components.iter().zip(types) {
                fields.push((
                    field_name(&component.name, &component.ty)?,
                    abi_type(ty, &component.components)?,
                ));
            }
            if !unique(fields.iter().map(|(name, _)| name.as_str())) {
                return Err("tuple component names must be unique".to_owned());
            }
            AbiType::Tuple(fields)
        }
        // `function` values, and EIP-712 structs should a dependency enable
        // that Alloy feature, which a named variant would not compile with.
        _ => return Err(format!("unsupported ABI type `{ty}`")),
    })
}

/// Whether a type nests arrays and tuples at most [`MAX_NESTING`] levels.
fn shallow(ty: &str, components: &[Param], depth: usize) -> bool {
    let depth = depth
        .saturating_add(ty.matches('[').count())
        .saturating_add(usize::from(!components.is_empty()));
    depth <= MAX_NESTING
        && components
            .iter()
            .all(|component| shallow(&component.ty, &component.components, depth))
}

/// Whether every dynamic array's element holds at most
/// [`MAX_ELEMENT_VALUES`]. Arrays inside an element are smaller still.
fn elements_fit(ty: &DynSolType) -> bool {
    match ty {
        DynSolType::Array(element) => template_size(element) <= MAX_ELEMENT_VALUES,
        DynSolType::FixedArray(element, _) => elements_fit(element),
        DynSolType::Tuple(types) => types.iter().all(elements_fit),
        _ => true,
    }
}

/// The values in `ty`, counting each array and tuple as one more: the size
/// of the template Alloy builds to decode it.
fn template_size(ty: &DynSolType) -> usize {
    match ty {
        DynSolType::Array(element) => template_size(element).saturating_add(1),
        DynSolType::FixedArray(element, size) => template_size(element)
            .saturating_mul(*size)
            .saturating_add(1),
        DynSolType::Tuple(types) => types
            .iter()
            .map(template_size)
            .fold(1, usize::saturating_add),
        _ => 1,
    }
}

fn too_deep() -> String {
    format!("event ABI nests arrays and tuples more than {MAX_NESTING} levels deep")
}

fn width(bits: usize) -> Result<u16, String> {
    u16::try_from(bits).map_err(|_| format!("integer ABI width {bits} is invalid"))
}

/// Decoded values are keyed by name, so every parameter and tuple component
/// needs one.
fn field_name(name: &str, ty: &str) -> Result<String, String> {
    if name.is_empty() {
        return Err(format!(
            "event parameter `{ty}` must be `type name` or `type indexed name`"
        ));
    }
    if !valid_identifier(name) {
        return Err("event field name is not a Solidity identifier".to_owned());
    }
    Ok(name.to_owned())
}

fn unique<'a>(names: impl Iterator<Item = &'a str>) -> bool {
    let mut seen = BTreeSet::new();
    names.into_iter().all(|name| seen.insert(name))
}

fn valid_identifier(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

/// Length of the tuple, array, `bytes`, or `string` encoded at `at`, if
/// `data` lays it out as Solidity's encoder does: every offset points at the
/// next unused byte and every length fits.
fn encoded_len(ty: &DynSolType, data: &[u8], at: usize) -> Option<usize> {
    match ty {
        DynSolType::Bytes | DynSolType::String => {
            let size = length(data, at)?
                .checked_next_multiple_of(32)?
                .checked_add(32)?;
            (at.checked_add(size)? <= data.len()).then_some(size)
        }
        DynSolType::Array(element) => {
            let count = length(data, at)?;
            let heads = count.checked_mul(head_size(element))?;
            let elements = sequence(
                iter::repeat_n(&**element, count),
                heads,
                data,
                at.checked_add(32)?,
            )?;
            elements.checked_add(32)
        }
        DynSolType::FixedArray(element, count) => sequence(
            iter::repeat_n(&**element, *count),
            count.checked_mul(head_size(element))?,
            data,
            at,
        ),
        DynSolType::Tuple(types) => sequence(
            types.iter(),
            types
                .iter()
                .map(head_size)
                .try_fold(0_usize, usize::checked_add)?,
            data,
            at,
        ),
        // Value types sit in their parent's head.
        _ => None,
    }
}

/// Checks the heads of a tuple's components or an array's elements at
/// `base`, then their tails in order, and returns the length of both.
fn sequence<'a>(
    items: impl Iterator<Item = &'a DynSolType>,
    heads: usize,
    data: &[u8],
    base: usize,
) -> Option<usize> {
    // Every head takes at least one word, since the parser rejects empty
    // tuples and arrays, so heads that fit bound the loop by the log's size.
    if base.checked_add(heads)? > data.len() {
        return None;
    }
    let mut head = base;
    let mut tail = heads;
    for ty in items {
        if ty.is_dynamic() {
            if length(data, head)? != tail {
                return None;
            }
            tail = tail.checked_add(encoded_len(ty, data, base.checked_add(tail)?)?)?;
        }
        head = head.saturating_add(head_size(ty));
    }
    Some(tail)
}

/// Bytes a value takes in its parent's head: an offset if it is dynamic,
/// else its whole encoding.
fn head_size(ty: &DynSolType) -> usize {
    if ty.is_dynamic() {
        32
    } else {
        ty.minimum_words().saturating_mul(32)
    }
}

/// An offset or length word, if it fits `usize`.
fn length(data: &[u8], at: usize) -> Option<usize> {
    let word = data.get(at..at.checked_add(32)?)?;
    usize::try_from(U256::from_be_slice(word)).ok()
}

/// Renders a non-indexed value: a tuple flattens to `name.component` keys,
/// an array becomes JSON text, and anything else one exact string.
fn render(
    name: String,
    ty: &AbiType,
    value: &DynSolValue,
    values: &mut BTreeMap<String, String>,
) -> Result<(), String> {
    match (ty, value) {
        (AbiType::Tuple(components), DynSolValue::Tuple(items)) => {
            for ((component, ty), item) in components.iter().zip(items) {
                render(format!("{name}.{component}"), ty, item, values)?;
            }
        }
        (AbiType::Array(..), _) => {
            let mut json = String::new();
            write_json(ty, value, &mut json)?;
            values.insert(name, json);
        }
        _ => {
            values.insert(name, leaf(ty, value)?);
        }
    }
    Ok(())
}

/// Writes JSON with every leaf as the same string a top-level value would
/// be, and tuple components in ABI order.
fn write_json(ty: &AbiType, value: &DynSolValue, json: &mut String) -> Result<(), String> {
    match (ty, value) {
        (
            AbiType::Array(element, _),
            DynSolValue::Array(items) | DynSolValue::FixedArray(items),
        ) => {
            json.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    json.push(',');
                }
                write_json(element, item, json)?;
            }
            json.push(']');
        }
        (AbiType::Tuple(components), DynSolValue::Tuple(items)) => {
            json.push('{');
            for (index, ((component, ty), item)) in components.iter().zip(items).enumerate() {
                if index > 0 {
                    json.push(',');
                }
                json.push_str(&serde_json::Value::from(component.as_str()).to_string());
                json.push(':');
                write_json(ty, item, json)?;
            }
            json.push('}');
        }
        _ => json.push_str(&serde_json::Value::from(leaf(ty, value)?).to_string()),
    }
    Ok(())
}

fn leaf(ty: &AbiType, value: &DynSolValue) -> Result<String, String> {
    match (ty, value) {
        (AbiType::Word(kind), value) => {
            decode_word(*kind, value.as_word().ok_or("a value word is missing")?.0)
        }
        (AbiType::Bytes, DynSolValue::Bytes(bytes)) => Ok(format!("0x{}", hex::encode(bytes))),
        (AbiType::String, DynSolValue::Bytes(bytes)) => {
            Ok(String::from_utf8_lossy(bytes).into_owned())
        }
        _ => Err("a decoded value does not match its ABI type".to_owned()),
    }
}

/// Renders one value word and rejects any word its type cannot encode.
fn decode_word(kind: AbiKind, word: [u8; 32]) -> Result<String, String> {
    match kind {
        AbiKind::Address => {
            if word[..12].iter().any(|byte| *byte != 0) {
                return Err("address contains non-zero ABI padding".to_owned());
            }
            Ok(format!("0x{}", hex::encode(&word[12..])))
        }
        AbiKind::Uint(bits) => {
            let padding = usize::from((256 - bits) / 8);
            if word[..padding].iter().any(|byte| *byte != 0) {
                return Err(format!("uint{bits} value exceeds its declared width"));
            }
            Ok(U256::from_be_bytes(word).to_string())
        }
        AbiKind::Int(bits) => {
            let padding = usize::from((256 - bits) / 8);
            let sign = word[padding] & 0x80 != 0;
            let expected = if sign { 0xff } else { 0x00 };
            if word[..padding].iter().any(|byte| *byte != expected) {
                return Err(format!("int{bits} value is not sign-extended"));
            }
            Ok(I256::from_raw(U256::from_be_bytes(word)).to_string())
        }
        AbiKind::Bool => match U256::from_be_bytes(word) {
            value if value == U256::ZERO => Ok("false".to_owned()),
            value if value == U256::from(1) => Ok("true".to_owned()),
            _ => Err("bool ABI value must be zero or one".to_owned()),
        },
        AbiKind::FixedBytes(bytes) => {
            let bytes = usize::from(bytes);
            if word[bytes..].iter().any(|byte| *byte != 0) {
                return Err("fixed bytes value contains non-zero ABI padding".to_owned());
            }
            Ok(format!("0x{}", hex::encode(&word[..bytes])))
        }
    }
}
