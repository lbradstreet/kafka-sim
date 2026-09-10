//! Length-delimited Kafka frames, checked before exposing any response state.
use crate::plan::{EncodeLimits, SendPlan};
use crate::wire::{DecodeLimits, Error, Reader, Result, Wire, Writer};
use crate::{Request, Response, api_version, request_header, response_header};

/// A completely validated request frame. Bodies and headers borrow the input.
#[derive(Debug, Clone)]
pub struct RequestFrame<'a> {
    pub api_key: i16,
    pub version: i16,
    pub correlation_id: i32,
    pub client_id: Option<&'a str>,
    pub header: request_header::View<'a>,
    pub body: Request<'a>,
}

/// A completely validated response with its expected correlation ID checked.
#[derive(Debug, Clone)]
pub struct ResponseFrame<'a> {
    pub correlation_id: i32,
    pub header: response_header::View<'a>,
    pub body: Response<'a>,
}

impl<'a> Request<'a> {
    /// Builds a frame without flattening external record spans.
    ///
    /// # Errors
    /// Rejects unsupported versions, a body for a different layout, invalid
    /// values, length overflow, or exhaustion of any encoding budget.
    pub fn plan_frame(
        &self,
        version: i16,
        correlation_id: i32,
        client_id: Option<&'a str>,
        limits: EncodeLimits,
    ) -> Result<SendPlan<'a>> {
        let spec = api_version(self.api_key(), version)?;
        let mut writer = Writer::new(version, false, limits);
        writer.write_i32(0)?;
        match spec.request_header_version {
            1 => request_header::v1::RequestHeader {
                request_api_key: self.api_key(),
                request_api_version: version,
                correlation_id,
                client_id,
                ..Default::default()
            }
            .write(&mut writer)?,
            2 => request_header::v2::RequestHeader {
                request_api_key: self.api_key(),
                request_api_version: version,
                correlation_id,
                client_id,
                ..Default::default()
            }
            .write(&mut writer)?,
            _ => return Err(Error::InvalidValue("unsupported request header")),
        }
        self.write_body(&mut writer, version)?;
        writer.finish_frame()
    }
}
impl<'a> Response<'a> {
    /// Builds a response frame, also useful for deterministic broker providers.
    ///
    /// # Errors
    /// Rejects unsupported versions, invalid values, or exhausted budgets.
    pub fn plan_frame(
        &self,
        version: i16,
        correlation_id: i32,
        limits: EncodeLimits,
    ) -> Result<SendPlan<'a>> {
        let spec = api_version(self.api_key(), version)?;
        let mut writer = Writer::new(version, false, limits);
        writer.write_i32(0)?;
        match spec.response_header_version {
            0 => response_header::v0::ResponseHeader {
                correlation_id,
                ..Default::default()
            }
            .write(&mut writer)?,
            1 => response_header::v1::ResponseHeader {
                correlation_id,
                ..Default::default()
            }
            .write(&mut writer)?,
            _ => return Err(Error::InvalidValue("unsupported response header")),
        }
        self.write_body(&mut writer, version)?;
        writer.finish_frame()
    }
}
fn framed_reader(bytes: &[u8], version: i16, limits: DecodeLimits) -> Result<Reader<'_>> {
    let mut reader = Reader::new(bytes, version, false, limits)?;
    let length = reader.read_i32()?;
    if length < 0 {
        return Err(Error::InvalidLength {
            value: i64::from(length),
        });
    }
    let length = length as usize;
    if length != reader.remaining() {
        return Err(Error::FrameLengthMismatch {
            declared: length,
            actual: reader.remaining(),
        });
    }
    Ok(reader)
}

/// Decodes exactly one complete frame, including every nested field and tag.
///
/// # Errors
/// Rejects invalid framing, unknown API versions, malformed bodies or headers,
/// trailing input, or exhaustion of any decode budget.
pub fn decode_request(bytes: &[u8], limits: DecodeLimits) -> Result<RequestFrame<'_>> {
    let reader = framed_reader(bytes, 0, limits)?;
    let mut peek = reader.clone();
    let api_key = peek.read_i16()?;
    let version = peek.read_i16()?;
    let spec = api_version(api_key, version)?;
    let mut reader = framed_reader(bytes, version, limits)?;
    let header = request_header::View::read(&mut reader, spec.request_header_version)?;
    let (correlation_id, client_id) = match &header {
        request_header::View::V1(header) => (header.correlation_id, header.client_id),
        request_header::View::V2(header) => (header.correlation_id, header.client_id),
    };
    let body = Request::read_body(&mut reader, api_key, version)?;
    reader.finish()?;
    Ok(RequestFrame {
        api_key,
        version,
        correlation_id,
        client_id,
        header,
        body,
    })
}

/// Decodes a response for a previously negotiated request version.
///
/// `ApiVersions` always uses response header v0. If negotiation returns an
/// `UNSUPPORTED_VERSION` response using body v0, the caller explicitly retries
/// this function with version 0; there is no ambiguous automatic fallback.
///
/// # Errors
/// Rejects a mismatched correlation ID, unsupported version, malformed frame,
/// trailing input, or exhausted budgets. No partial view is returned.
pub fn decode_response(
    bytes: &[u8],
    api_key: i16,
    version: i16,
    expected_correlation_id: i32,
    limits: DecodeLimits,
) -> Result<ResponseFrame<'_>> {
    let spec = api_version(api_key, version)?;
    let mut reader = framed_reader(bytes, version, limits)?;
    let header = response_header::View::read(&mut reader, spec.response_header_version)?;
    let correlation_id = match &header {
        response_header::View::V0(header) => header.correlation_id,
        response_header::View::V1(header) => header.correlation_id,
    };
    if correlation_id != expected_correlation_id {
        return Err(Error::CorrelationMismatch {
            expected: expected_correlation_id,
            actual: correlation_id,
        });
    }
    let body = Response::read_body(&mut reader, api_key, version)?;
    reader.finish()?;
    Ok(ResponseFrame {
        correlation_id,
        header,
        body,
    })
}
