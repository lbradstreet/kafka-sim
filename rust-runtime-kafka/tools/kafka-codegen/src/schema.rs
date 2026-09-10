//! Strict parsing, semantic validation, and version-resolved Kafka message layouts.
//!
//! Layout equality includes recursively resolved fields, defaults, nullability,
//! tags, and ignorable behavior. Equal layouts do not imply equal broker semantics;
//! callers must select the versions they have separately tested.

use serde::{Deserialize, Deserializer};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error {
    pub message: String,
}
impl Error {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for Error {}

/// Inclusive nonnegative Kafka API versions. Open ranges retain their syntax so
/// rules requiring an explicit finite upper bound can be checked.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum VersionRange {
    #[default]
    None,
    Bounded {
        start: i16,
        end: i16,
    },
    Open {
        start: i16,
    },
}
impl VersionRange {
    pub fn parse(value: &str) -> Result<Self, Error> {
        fn number(s: &str) -> Result<i16, Error> {
            if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
                return Err(Error::new(format!("invalid version number {s:?}")));
            }
            s.parse()
                .map_err(|_| Error::new(format!("version outside 0..=32767: {s}")))
        }
        if value == "none" {
            return Ok(Self::None);
        }
        if let Some(start) = value.strip_suffix('+') {
            return Ok(Self::Open {
                start: number(start)?,
            });
        }
        if let Some((start, end)) = value.split_once('-') {
            let (start, end) = (number(start)?, number(end)?);
            if start > end {
                return Err(Error::new(format!("reversed version range {value:?}")));
            }
            return Ok(Self::Bounded { start, end });
        }
        let version = number(value)?;
        Ok(Self::Bounded {
            start: version,
            end: version,
        })
    }
    pub fn min(self) -> Option<i16> {
        match self {
            Self::None => None,
            Self::Bounded { start, .. } | Self::Open { start } => Some(start),
        }
    }
    pub fn max(self) -> Option<i16> {
        match self {
            Self::None => None,
            Self::Bounded { end, .. } => Some(end),
            Self::Open { .. } => Some(i16::MAX),
        }
    }
    pub fn contains(self, version: i16) -> bool {
        self.min().is_some_and(|min| version >= min) && self.max().is_some_and(|max| version <= max)
    }
    pub fn is_empty(self) -> bool {
        self == Self::None
    }
    fn contains_range(self, other: Self) -> bool {
        other.is_empty()
            || (other.min().is_some_and(|v| self.contains(v))
                && other.max().is_some_and(|v| self.contains(v)))
    }
    fn intersect(self, other: Self) -> Self {
        match (self.min(), self.max(), other.min(), other.max()) {
            (Some(a), Some(b), Some(c), Some(d)) if a.max(c) <= b.min(d) => Self::Bounded {
                start: a.max(c),
                end: b.min(d),
            },
            _ => Self::None,
        }
    }
    fn validate(self) -> Result<(), Error> {
        match self {
            Self::Bounded { start, end } if start < 0 || end < start => {
                Err(Error::new("invalid version range"))
            }
            Self::Open { start } if start < 0 => Err(Error::new("invalid negative version")),
            _ => Ok(()),
        }
    }
}
impl<'de> Deserialize<'de> for VersionRange {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::parse(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum MessageKind {
    Request,
    Response,
    Header,
    Data,
}

/// Parsed source, including recognized generation metadata. These metadata do
/// not alter Rust wire layouts; they are still type-checked during validation.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Schema {
    pub name: String,
    pub api_key: Option<i16>,
    #[serde(rename = "type")]
    pub kind: MessageKind,
    pub valid_versions: VersionRange,
    #[serde(default)]
    pub flexible_versions: VersionRange,
    #[serde(default)]
    pub fields: Vec<Field>,
    #[serde(default)]
    pub common_structs: Vec<CommonStruct>,
    #[serde(default)]
    pub listeners: Vec<Listener>,
    #[serde(default)]
    pub latest_version_unstable: bool,
}
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Listener {
    Broker,
    Controller,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CommonStruct {
    pub name: String,
    pub versions: VersionRange,
    #[serde(default)]
    pub fields: Vec<Field>,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Field {
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
    pub versions: Option<VersionRange>,
    #[serde(default)]
    pub nullable_versions: VersionRange,
    pub flexible_versions: Option<VersionRange>,
    #[serde(default)]
    pub tagged_versions: VersionRange,
    pub tag: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_default")]
    pub default: Option<DefaultLiteral>,
    #[serde(default)]
    pub ignorable: bool,
    #[serde(default)]
    pub map_key: bool,
    #[serde(default)]
    pub zero_copy: bool,
    pub entity_type: Option<EntityType>,
    pub about: Option<String>,
    // Distinguish a missing definition (reference) from an empty inline struct.
    pub fields: Option<Vec<Field>>,
}
impl Field {
    fn versions(&self) -> Result<VersionRange, Error> {
        self.versions
            .or_else(|| (!self.tagged_versions.is_empty()).then_some(self.tagged_versions))
            .ok_or_else(|| {
                Error::new(format!(
                    "field {} must specify versions or taggedVersions",
                    self.name
                ))
            })
    }
}
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EntityType {
    Unknown,
    TransactionalId,
    ProducerId,
    GroupId,
    TopicName,
    BrokerId,
}
/// Kafka's JSON reader accepts scalar defaults (including a JSON boolean used
/// by the pinned corpus). Arrays, objects, and JSON null are not default syntax.
#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum DefaultLiteral {
    String(String),
    Bool(bool),
    Number(serde_json::Number),
}
impl DefaultLiteral {
    fn text(&self) -> String {
        match self {
            Self::String(s) => s.clone(),
            Self::Bool(v) => v.to_string(),
            Self::Number(v) => v.to_string(),
        }
    }
}
fn deserialize_default<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<DefaultLiteral>, D::Error> {
    DefaultLiteral::deserialize(deserializer).map(Some)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledMessage {
    pub name: String,
    pub api_key: Option<i16>,
    pub kind: MessageKind,
    pub layouts: Vec<LayoutClass>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LayoutClass {
    pub versions: Vec<i16>,
    pub root: StructLayout,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StructLayout {
    pub name: String,
    /// Every flexible struct has a tag section, including structs with no known tags.
    pub flexible: bool,
    /// Present fields in schema order; tagged fields retain their explicit IDs.
    pub fields: Vec<FieldLayout>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FieldLayout {
    pub name: String,
    pub ty: FieldType,
    pub nullable: bool,
    /// Applies to this field's variable length prefix and to array elements.
    /// Struct tag sections are described by StructLayout::flexible separately.
    pub compact: bool,
    pub tag: Option<u32>,
    pub default: DefaultValue,
    pub ignorable: bool,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FieldType {
    Bool,
    Int8,
    Int16,
    Uint16,
    Int32,
    Uint32,
    Int64,
    Float64,
    String,
    Uuid,
    Bytes,
    Records,
    Array(Box<FieldType>),
    Struct(Box<StructLayout>),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DefaultValue {
    Bool(bool),
    Signed(i64),
    Unsigned(u64),
    /// IEEE 754 bits allow deterministic equality, including signed zero.
    Float(u64),
    String(String),
    ZeroUuid,
    EmptyBytes,
    EmptyArray,
    Null,
    Struct,
}

/// Parse comment-bearing JSON and validate the entire schema, including fields
/// that do not appear in a caller's selected version set. Unknown properties and
/// duplicate JSON properties are errors. Line comments are recognized only
/// outside strings; replacing them with spaces preserves JSON error positions.
pub fn parse(source: &str) -> Result<Schema, Error> {
    if source.len() > 8 * 1024 * 1024 {
        return Err(Error::new("schema exceeds the 8 MiB source limit"));
    }
    let json = remove_comments(source);
    let schema: Schema =
        serde_json::from_str(&json).map_err(|e| Error::new(format!("schema JSON: {e}")))?;
    // Removed APIs use validVersions=none and intentionally omit this property.
    // A live API must state its encoding explicitly, as Kafka's generator does.
    let properties: serde_json::Value =
        serde_json::from_str(&json).map_err(|e| Error::new(format!("schema JSON: {e}")))?;
    if !schema.valid_versions.is_empty() && properties.get("flexibleVersions").is_none() {
        return Err(Error::new("active schemas must specify flexibleVersions"));
    }
    validate(&schema)?;
    Ok(schema)
}
fn remove_comments(source: &str) -> String {
    let mut out = source.as_bytes().to_vec();
    let (mut i, mut quoted, mut escaped) = (0, false, false);
    while i < out.len() {
        let c = out[i];
        if quoted {
            if escaped {
                escaped = false;
            } else if c == b'\\' {
                escaped = true;
            } else if c == b'"' {
                quoted = false;
            }
        } else if c == b'"' {
            quoted = true;
        } else if c == b'/' && out.get(i + 1) == Some(&b'/') {
            while i < out.len() && out[i] != b'\n' && out[i] != b'\r' {
                out[i] = b' ';
                i += 1;
            }
            continue;
        }
        i += 1;
    }
    // Only complete comment bytes are replaced; no UTF-8 sequence is split.
    String::from_utf8(out).expect("comment replacement preserves UTF-8")
}

#[derive(Clone, Debug)]
enum ParsedType {
    Primitive(FieldType),
    Array(Box<ParsedType>),
    Named(String),
}
impl ParsedType {
    fn parse(s: &str) -> Result<Self, Error> {
        use FieldType::*;
        if let Some(element) = s.strip_prefix("[]") {
            if element.starts_with("[]") {
                return Err(Error::new(
                    "arrays of arrays are unsupported by Kafka; use a struct element",
                ));
            }
            let parsed = Self::parse(element)?;
            return Ok(Self::Array(Box::new(parsed)));
        }
        let primitive = match s {
            "bool" => Bool,
            "int8" => Int8,
            "int16" => Int16,
            "uint16" => Uint16,
            "int32" => Int32,
            "uint32" => Uint32,
            "int64" => Int64,
            "float64" => Float64,
            "string" => String,
            "uuid" => Uuid,
            "bytes" => Bytes,
            "records" => Records,
            _ if valid_name(s) && s.as_bytes()[0].is_ascii_uppercase() => {
                return Ok(Self::Named(s.to_owned()));
            }
            _ => return Err(Error::new(format!("unknown field type {s:?}"))),
        };
        Ok(Self::Primitive(primitive))
    }
    fn named(&self) -> Option<&str> {
        match self {
            Self::Named(s) => Some(s),
            Self::Array(t) => t.named(),
            _ => None,
        }
    }
    fn nullable(&self) -> bool {
        matches!(
            self,
            Self::Array(_)
                | Self::Named(_)
                | Self::Primitive(FieldType::String | FieldType::Bytes | FieldType::Records)
        )
    }
    fn variable(&self) -> bool {
        matches!(
            self,
            Self::Array(_)
                | Self::Primitive(FieldType::String | FieldType::Bytes | FieldType::Records)
        )
    }
    fn base(&self) -> &Self {
        match self {
            Self::Array(t) => t,
            _ => self,
        }
    }
}
fn valid_name(s: &str) -> bool {
    s.as_bytes().first().is_some_and(u8::is_ascii_alphabetic)
        && s.bytes().all(|b| b.is_ascii_alphanumeric())
}
fn check_name(s: &str) -> Result<(), Error> {
    if valid_name(s) {
        Ok(())
    } else {
        Err(Error::new(format!(
            "invalid schema name {s:?}; expected [A-Za-z][A-Za-z0-9]*"
        )))
    }
}
struct Definition<'a> {
    fields: &'a [Field],
    versions: VersionRange,
}
type Registry<'a> = BTreeMap<String, Definition<'a>>;
fn register<'a>(
    name: &str,
    fields: &'a [Field],
    versions: VersionRange,
    registry: &mut Registry<'a>,
    depth: usize,
) -> Result<(), Error> {
    if depth >= 128 {
        return Err(Error::new("struct nesting exceeds the 128-level limit"));
    }
    check_name(name)?;
    if !name.as_bytes()[0].is_ascii_uppercase() {
        return Err(Error::new(format!(
            "struct name {name:?} must start with an uppercase letter"
        )));
    }
    if registry
        .insert(name.to_owned(), Definition { fields, versions })
        .is_some()
    {
        return Err(Error::new(format!("duplicate struct definition {name}")));
    }
    for field in fields {
        let ty = ParsedType::parse(&field.type_name)?;
        if let Some(fields) = &field.fields {
            let name = ty.named().ok_or_else(|| {
                Error::new(format!(
                    "non-struct field {} cannot define fields",
                    field.name
                ))
            })?;
            register(name, fields, field.versions()?, registry, depth + 1)?;
        }
    }
    Ok(())
}
fn registry(schema: &Schema) -> Result<Registry<'_>, Error> {
    let mut registry = BTreeMap::new();
    register(
        &schema.name,
        &schema.fields,
        schema.valid_versions,
        &mut registry,
        0,
    )?;
    for common in &schema.common_structs {
        register(
            &common.name,
            &common.fields,
            common.versions,
            &mut registry,
            0,
        )?;
    }
    Ok(registry)
}
/// Validate the whole source independently of version selection. Public parsed
/// structures may be modified, so compile revalidates rather than trusting parse.
pub fn validate(schema: &Schema) -> Result<(), Error> {
    check_name(&schema.name)?;
    schema.valid_versions.validate()?;
    schema.flexible_versions.validate()?;
    if matches!(schema.valid_versions, VersionRange::Open { .. }) {
        return Err(Error::new("validVersions must specify a finite maximum"));
    }
    if matches!(schema.flexible_versions, VersionRange::Bounded { .. }) {
        return Err(Error::new("flexibleVersions must be none or open-ended"));
    }
    match (schema.kind, schema.api_key) {
        (MessageKind::Request | MessageKind::Response, None) => {
            return Err(Error::new("request and response schemas require apiKey"));
        }
        (_, Some(key)) if key < 0 => return Err(Error::new("apiKey cannot be negative")),
        (MessageKind::Header | MessageKind::Data, Some(_)) => {
            return Err(Error::new("header and data schemas cannot specify apiKey"));
        }
        _ => {}
    }
    if !schema.listeners.is_empty() && schema.kind != MessageKind::Request {
        return Err(Error::new("listeners are only supported on requests"));
    }
    if schema.latest_version_unstable && schema.kind != MessageKind::Request {
        return Err(Error::new(
            "latestVersionUnstable is only supported on requests",
        ));
    }
    let registry = registry(schema)?;
    for (name, definition) in &registry {
        definition.versions.validate()?;
        validate_fields(name, definition.fields, schema.flexible_versions)?;
    }
    // Traverse every definition, including unused common structs, rejecting
    // unresolved names and cycles before any selected layout can be emitted.
    let mut complete = BTreeMap::new();
    for name in registry.keys() {
        validate_references(name, &registry, &mut Vec::new(), &mut complete)?;
    }
    // A common struct must exist whenever a reachable reference does. Restrict
    // through enclosing fields: nested version ranges need not be subsets of
    // their parents' ranges in Kafka schemas.
    validate_availability(
        &schema.name,
        schema.valid_versions,
        &registry,
        &mut BTreeSet::new(),
    )?;
    for common in &schema.common_structs {
        validate_availability(
            &common.name,
            common.versions.intersect(schema.valid_versions),
            &registry,
            &mut BTreeSet::new(),
        )?;
    }
    Ok(())
}
fn validate_fields(name: &str, fields: &[Field], flexible: VersionRange) -> Result<(), Error> {
    let (mut names, mut tags) = (BTreeSet::new(), BTreeSet::new());
    for field in fields {
        check_name(&field.name)?;
        if !names.insert(&field.name) {
            return Err(Error::new(format!(
                "{name}: duplicate field {}",
                field.name
            )));
        }
        let result = validate_field(field, flexible);
        result.map_err(|e| Error::new(format!("{name}.{}: {e}", field.name)))?;
        if let Some(tag) = field.tag
            && !tags.insert(tag)
        {
            return Err(Error::new(format!("{name}: duplicate tag {tag}")));
        }
    }
    for (expected, actual) in tags.iter().enumerate() {
        if *actual != expected as u32 {
            return Err(Error::new(format!(
                "{name}: tag IDs must be contiguous starting at zero (missing {expected})"
            )));
        }
    }
    Ok(())
}
fn validate_field(field: &Field, flexible: VersionRange) -> Result<(), Error> {
    let ty = ParsedType::parse(&field.type_name)?;
    let versions = field.versions()?;
    for range in [versions, field.nullable_versions, field.tagged_versions] {
        range.validate()?;
    }
    if !field.nullable_versions.is_empty() && !ty.nullable() {
        return Err(Error::new("this type cannot be nullable"));
    }
    if let Some(override_versions) = field.flexible_versions {
        override_versions.validate()?;
        if !matches!(
            ty,
            ParsedType::Primitive(FieldType::String | FieldType::Bytes)
        ) {
            return Err(Error::new(
                "flexibleVersions override is only valid on string or bytes",
            ));
        }
        if !flexible.contains_range(override_versions) {
            return Err(Error::new(
                "field flexibleVersions must be a subset of message flexibleVersions",
            ));
        }
    }
    if field.zero_copy && !matches!(ty, ParsedType::Primitive(FieldType::Bytes)) {
        return Err(Error::new("zeroCopy is only valid on bytes"));
    }
    if let Some(entity) = field.entity_type {
        let expected = match entity {
            EntityType::Unknown => None,
            EntityType::TransactionalId | EntityType::GroupId | EntityType::TopicName => {
                Some(FieldType::String)
            }
            EntityType::ProducerId => Some(FieldType::Int64),
            EntityType::BrokerId => Some(FieldType::Int32),
        };
        if let Some(expected) = expected
            && !matches!(ty.base(), ParsedType::Primitive(actual) if *actual == expected)
        {
            return Err(Error::new("entityType does not match field type"));
        }
    }
    match field.tag {
        Some(tag) => {
            if tag > i32::MAX as u32 {
                return Err(Error::new("tag exceeds Kafka's signed int32 tag range"));
            }
            if field.map_key {
                return Err(Error::new("tagged fields cannot be map keys"));
            }
            if !matches!(field.tagged_versions, VersionRange::Open { .. }) {
                return Err(Error::new("a tag requires open-ended taggedVersions"));
            }
            if !versions.contains_range(field.tagged_versions) {
                return Err(Error::new(
                    "taggedVersions must be a subset of field versions",
                ));
            }
            if !flexible.contains_range(field.tagged_versions) {
                return Err(Error::new(
                    "taggedVersions must be a subset of message flexibleVersions",
                ));
            }
            if matches!(ty, ParsedType::Primitive(FieldType::Records)) {
                return Err(Error::new("records cannot be tagged fields"));
            }
            if field
                .flexible_versions
                .is_some_and(|r| !r.contains_range(field.tagged_versions))
            {
                return Err(Error::new(
                    "classic encoding overrides for tagged fields are unsupported",
                ));
            }
            let intersection = field.nullable_versions.intersect(field.tagged_versions);
            if !intersection.is_empty()
                && !field
                    .nullable_versions
                    .contains_range(field.tagged_versions)
            {
                return Err(Error::new(
                    "either all tagged versions must be nullable, or none",
                ));
            }
        }
        None if !field.tagged_versions.is_empty() => {
            return Err(Error::new("taggedVersions requires a tag"));
        }
        None => {}
    }
    default_value(field, &ty)?;
    Ok(())
}
fn validate_references(
    name: &str,
    registry: &Registry<'_>,
    visiting: &mut Vec<String>,
    complete: &mut BTreeMap<String, usize>,
) -> Result<usize, Error> {
    if visiting.iter().any(|n| n == name) {
        return Err(Error::new(format!(
            "recursive struct reference: {} -> {name}",
            visiting.join(" -> ")
        )));
    }
    if let Some(depth) = complete.get(name) {
        return Ok(*depth);
    }
    if visiting.len() >= 128 {
        return Err(Error::new("struct nesting exceeds the 128-level limit"));
    }
    let definition = registry
        .get(name)
        .ok_or_else(|| Error::new(format!("undefined struct type {name}")))?;
    visiting.push(name.to_owned());
    let mut depth = 1;
    for field in definition.fields {
        if let Some(target) = ParsedType::parse(&field.type_name)?.named() {
            depth = depth.max(1 + validate_references(target, registry, visiting, complete)?);
        }
    }
    visiting.pop();
    if depth > 128 {
        return Err(Error::new("struct nesting exceeds the 128-level limit"));
    }
    complete.insert(name.to_owned(), depth);
    Ok(depth)
}
fn validate_availability(
    name: &str,
    active: VersionRange,
    registry: &Registry<'_>,
    seen: &mut BTreeSet<(String, Option<i16>, Option<i16>)>,
) -> Result<(), Error> {
    if active.is_empty() || !seen.insert((name.to_owned(), active.min(), active.max())) {
        return Ok(());
    }
    let definition = &registry[name];
    if !definition.versions.contains_range(active) {
        return Err(Error::new(format!(
            "struct {name} is unavailable for some versions of its reference"
        )));
    }
    for field in definition.fields {
        if let Some(target) = ParsedType::parse(&field.type_name)?.named() {
            validate_availability(target, active.intersect(field.versions()?), registry, seen)?;
        }
    }
    Ok(())
}
fn default_value(field: &Field, ty: &ParsedType) -> Result<DefaultValue, Error> {
    use DefaultValue as D;
    use FieldType as T;
    let text = field
        .default
        .as_ref()
        .map(DefaultLiteral::text)
        .unwrap_or_default();
    if text == "null" && ty.nullable() {
        if !field.nullable_versions.contains_range(field.versions()?) {
            return Err(Error::new(
                "null default requires all field versions to be nullable",
            ));
        }
        return Ok(D::Null);
    }
    let invalid = || Error::new(format!("invalid default {text:?} for {}", field.type_name));
    match ty {
        ParsedType::Primitive(T::Bool) => match text.to_ascii_lowercase().as_str() {
            "" | "false" => Ok(D::Bool(false)),
            "true" => Ok(D::Bool(true)),
            _ => Err(invalid()),
        },
        ParsedType::Primitive(T::Int8 | T::Int16 | T::Int32 | T::Int64 | T::Uint16 | T::Uint32) => {
            // Kafka accepts hexadecimal literals; octal is documented by the
            // schema README and Java emits such literals without rewriting them.
            let value = if text.is_empty() {
                0
            } else {
                let (negative, digits) = if let Some(s) = text.strip_prefix('-') {
                    (true, s)
                } else {
                    (false, text.strip_prefix('+').unwrap_or(&text))
                };
                let (base, digits) = if let Some(s) = digits.strip_prefix("0x") {
                    (16, s)
                } else if digits.len() > 1 && digits.starts_with('0') {
                    (8, &digits[1..])
                } else {
                    (10, digits)
                };
                if digits.is_empty() || !digits.chars().all(|c| c.is_digit(base)) {
                    return Err(invalid());
                }
                let magnitude = i128::from_str_radix(digits, base).map_err(|_| invalid())?;
                if negative { -magnitude } else { magnitude }
            };
            let ParsedType::Primitive(primitive) = ty else {
                unreachable!()
            };
            let (min, max, unsigned) = match primitive {
                T::Int8 => (i8::MIN as i128, i8::MAX as i128, false),
                T::Int16 => (i16::MIN as i128, i16::MAX as i128, false),
                T::Int32 => (i32::MIN as i128, i32::MAX as i128, false),
                T::Int64 => (i64::MIN as i128, i64::MAX as i128, false),
                T::Uint16 => (0, u16::MAX as i128, true),
                T::Uint32 => (0, u32::MAX as i128, true),
                _ => unreachable!(),
            };
            if value < min || value > max {
                return Err(invalid());
            }
            Ok(if unsigned {
                D::Unsigned(value as u64)
            } else {
                D::Signed(value as i64)
            })
        }
        ParsedType::Primitive(T::Float64) => {
            let value = if text.is_empty() {
                0.0
            } else {
                text.parse::<f64>().map_err(|_| invalid())?
            };
            if !value.is_finite() {
                return Err(Error::new("non-finite float defaults are unsupported"));
            }
            Ok(D::Float(value.to_bits()))
        }
        ParsedType::Primitive(T::String) => Ok(D::String(text)),
        ParsedType::Primitive(T::Uuid) if text.is_empty() => Ok(D::ZeroUuid),
        ParsedType::Primitive(T::Uuid) => Err(Error::new("nonzero UUID defaults are unsupported")),
        ParsedType::Primitive(T::Bytes) if text.is_empty() => Ok(D::EmptyBytes),
        ParsedType::Primitive(T::Records) if text.is_empty() => Ok(D::Null),
        ParsedType::Array(_) if text.is_empty() => Ok(D::EmptyArray),
        ParsedType::Named(_) if text.is_empty() => Ok(D::Struct),
        _ => Err(invalid()),
    }
}

/// Resolve selected versions and deduplicate complete recursive layouts. Input
/// versions must be nonempty, unique, and supported by validVersions. Output
/// classes and their version lists are deterministic regardless of input order.
pub fn compile(schema: &Schema, versions: &[i16]) -> Result<CompiledMessage, Error> {
    validate(schema)?;
    if versions.is_empty() {
        return Err(Error::new("at least one message version must be selected"));
    }
    let mut selected = BTreeSet::new();
    for &version in versions {
        if !schema.valid_versions.contains(version) {
            return Err(Error::new(format!(
                "{} does not support version {version}",
                schema.name
            )));
        }
        if !selected.insert(version) {
            return Err(Error::new(format!("duplicate selected version {version}")));
        }
    }
    let registry = registry(schema)?;
    let mut layouts: Vec<LayoutClass> = Vec::new();
    let mut budget = 1_000_000_usize;
    for version in selected {
        let root = resolve_struct(
            &schema.name,
            version,
            schema.flexible_versions.contains(version),
            &registry,
            &mut budget,
        )?;
        if let Some(existing) = layouts.iter_mut().find(|layout| layout.root == root) {
            existing.versions.push(version);
        } else {
            layouts.push(LayoutClass {
                versions: vec![version],
                root,
            });
        }
    }
    Ok(CompiledMessage {
        name: schema.name.clone(),
        api_key: schema.api_key,
        kind: schema.kind,
        layouts,
    })
}
fn resolve_struct(
    name: &str,
    version: i16,
    flexible: bool,
    registry: &Registry<'_>,
    budget: &mut usize,
) -> Result<StructLayout, Error> {
    *budget = budget.checked_sub(1).ok_or_else(|| {
        Error::new("resolved layout exceeds the one-million-node compilation limit")
    })?;
    let definition = &registry[name];
    let mut fields = Vec::new();
    for field in definition.fields {
        if !field.versions()?.contains(version) {
            continue;
        }
        *budget = budget.checked_sub(1).ok_or_else(|| {
            Error::new("resolved layout exceeds the one-million-node compilation limit")
        })?;
        let parsed = ParsedType::parse(&field.type_name)?;
        let compact = parsed.variable()
            && field
                .flexible_versions
                .map_or(flexible, |r| r.contains(version));
        fields.push(FieldLayout {
            name: field.name.clone(),
            ty: resolve_type(&parsed, version, flexible, registry, budget)?,
            nullable: field.nullable_versions.contains(version),
            compact,
            tag: field
                .tag
                .filter(|_| field.tagged_versions.contains(version)),
            default: default_value(field, &parsed)?,
            ignorable: field.ignorable,
        });
    }
    Ok(StructLayout {
        name: name.to_owned(),
        flexible,
        fields,
    })
}
fn resolve_type(
    ty: &ParsedType,
    version: i16,
    flexible: bool,
    registry: &Registry<'_>,
    budget: &mut usize,
) -> Result<FieldType, Error> {
    Ok(match ty {
        ParsedType::Primitive(ty) => ty.clone(),
        ParsedType::Array(element) => FieldType::Array(Box::new(resolve_type(
            element, version, flexible, registry, budget,
        )?)),
        ParsedType::Named(name) => FieldType::Struct(Box::new(resolve_struct(
            name, version, flexible, registry, budget,
        )?)),
    })
}

#[cfg(test)]
mod tests;
