//! The strict length-prefixed wire format between the sim client and server.
//!
//! One request frame travels client-to-server per connection; the server
//! replies with one response frame and half-closes. Decoders accept only
//! canonical encodings: unknown opcodes, short fields, and trailing bytes
//! are `InvalidData` errors, never defaults.

use std::io;

pub(crate) enum Request {
    Put {
        bucket: String,
        key: String,
        body: Vec<u8>,
    },
    Get {
        bucket: String,
        key: String,
    },
    Head {
        bucket: String,
        key: String,
    },
    Delete {
        bucket: String,
        key: String,
    },
    List {
        bucket: String,
        prefix: Option<String>,
    },
}

pub(crate) struct ObjectSummary {
    pub(crate) key: String,
    pub(crate) size: u64,
    pub(crate) e_tag: String,
}

pub(crate) enum Response {
    PutOk { e_tag: String },
    GetOk { e_tag: String, body: Vec<u8> },
    HeadOk { e_tag: String, content_length: u64 },
    DeleteOk,
    ListOk { objects: Vec<ObjectSummary> },
    Error { code: ErrorCode, message: String },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ErrorCode {
    NoSuchBucket,
    NoSuchKey,
    NotFound,
}

const OP_PUT: u8 = 1;
const OP_GET: u8 = 2;
const OP_HEAD: u8 = 3;
const OP_DELETE: u8 = 4;
const OP_LIST: u8 = 5;

const RESP_PUT_OK: u8 = 1;
const RESP_GET_OK: u8 = 2;
const RESP_HEAD_OK: u8 = 3;
const RESP_DELETE_OK: u8 = 4;
const RESP_LIST_OK: u8 = 5;
const RESP_ERROR: u8 = 255;

const CODE_NO_SUCH_BUCKET: u8 = 1;
const CODE_NO_SUCH_KEY: u8 = 2;
const CODE_NOT_FOUND: u8 = 3;

fn invalid(reason: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, reason)
}

fn put_u64(frame: &mut Vec<u8>, value: u64) {
    frame.extend_from_slice(&value.to_le_bytes());
}

fn put_bytes(frame: &mut Vec<u8>, bytes: &[u8]) {
    put_u64(frame, bytes.len() as u64);
    frame.extend_from_slice(bytes);
}

fn put_str(frame: &mut Vec<u8>, text: &str) {
    put_bytes(frame, text.as_bytes());
}

fn put_opt_str(frame: &mut Vec<u8>, text: Option<&str>) {
    match text {
        None => frame.push(0),
        Some(text) => {
            frame.push(1);
            put_str(frame, text);
        }
    }
}

struct Cursor<'a> {
    data: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }

    fn take_u8(&mut self) -> io::Result<u8> {
        let byte = *self
            .data
            .get(self.position)
            .ok_or_else(|| invalid("frame is truncated"))?;
        self.position += 1;
        Ok(byte)
    }

    fn take_u64(&mut self) -> io::Result<u64> {
        let end = self
            .position
            .checked_add(8)
            .filter(|end| *end <= self.data.len())
            .ok_or_else(|| invalid("frame is truncated"))?;
        let mut raw = [0_u8; 8];
        raw.copy_from_slice(&self.data[self.position..end]);
        self.position = end;
        Ok(u64::from_le_bytes(raw))
    }

    fn take_bytes(&mut self) -> io::Result<Vec<u8>> {
        let length = usize::try_from(self.take_u64()?)
            .map_err(|_| invalid("field length exceeds the address space"))?;
        let end = self
            .position
            .checked_add(length)
            .filter(|end| *end <= self.data.len())
            .ok_or_else(|| invalid("frame is truncated"))?;
        let bytes = self.data[self.position..end].to_vec();
        self.position = end;
        Ok(bytes)
    }

    fn take_str(&mut self) -> io::Result<String> {
        String::from_utf8(self.take_bytes()?).map_err(|_| invalid("field is not UTF-8"))
    }

    fn take_opt_str(&mut self) -> io::Result<Option<String>> {
        match self.take_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.take_str()?)),
            _ => Err(invalid("option tag is not canonical")),
        }
    }

    fn finish(self) -> io::Result<()> {
        if self.position == self.data.len() {
            Ok(())
        } else {
            Err(invalid("frame has trailing bytes"))
        }
    }
}

pub(crate) fn encode_request(request: &Request) -> Vec<u8> {
    let mut frame = Vec::new();
    match request {
        Request::Put { bucket, key, body } => {
            frame.push(OP_PUT);
            put_str(&mut frame, bucket);
            put_str(&mut frame, key);
            put_bytes(&mut frame, body);
        }
        Request::Get { bucket, key } => {
            frame.push(OP_GET);
            put_str(&mut frame, bucket);
            put_str(&mut frame, key);
        }
        Request::Head { bucket, key } => {
            frame.push(OP_HEAD);
            put_str(&mut frame, bucket);
            put_str(&mut frame, key);
        }
        Request::Delete { bucket, key } => {
            frame.push(OP_DELETE);
            put_str(&mut frame, bucket);
            put_str(&mut frame, key);
        }
        Request::List { bucket, prefix } => {
            frame.push(OP_LIST);
            put_str(&mut frame, bucket);
            put_opt_str(&mut frame, prefix.as_deref());
        }
    }
    frame
}

pub(crate) fn decode_request(data: &[u8]) -> io::Result<Request> {
    let mut cursor = Cursor::new(data);
    let request = match cursor.take_u8()? {
        OP_PUT => Request::Put {
            bucket: cursor.take_str()?,
            key: cursor.take_str()?,
            body: cursor.take_bytes()?,
        },
        OP_GET => Request::Get {
            bucket: cursor.take_str()?,
            key: cursor.take_str()?,
        },
        OP_HEAD => Request::Head {
            bucket: cursor.take_str()?,
            key: cursor.take_str()?,
        },
        OP_DELETE => Request::Delete {
            bucket: cursor.take_str()?,
            key: cursor.take_str()?,
        },
        OP_LIST => Request::List {
            bucket: cursor.take_str()?,
            prefix: cursor.take_opt_str()?,
        },
        _ => return Err(invalid("unknown request opcode")),
    };
    cursor.finish()?;
    Ok(request)
}

pub(crate) fn encode_response(response: &Response) -> Vec<u8> {
    let mut frame = Vec::new();
    match response {
        Response::PutOk { e_tag } => {
            frame.push(RESP_PUT_OK);
            put_str(&mut frame, e_tag);
        }
        Response::GetOk { e_tag, body } => {
            frame.push(RESP_GET_OK);
            put_str(&mut frame, e_tag);
            put_bytes(&mut frame, body);
        }
        Response::HeadOk {
            e_tag,
            content_length,
        } => {
            frame.push(RESP_HEAD_OK);
            put_str(&mut frame, e_tag);
            put_u64(&mut frame, *content_length);
        }
        Response::DeleteOk => frame.push(RESP_DELETE_OK),
        Response::ListOk { objects } => {
            frame.push(RESP_LIST_OK);
            put_u64(&mut frame, objects.len() as u64);
            for object in objects {
                put_str(&mut frame, &object.key);
                put_u64(&mut frame, object.size);
                put_str(&mut frame, &object.e_tag);
            }
        }
        Response::Error { code, message } => {
            frame.push(RESP_ERROR);
            frame.push(match code {
                ErrorCode::NoSuchBucket => CODE_NO_SUCH_BUCKET,
                ErrorCode::NoSuchKey => CODE_NO_SUCH_KEY,
                ErrorCode::NotFound => CODE_NOT_FOUND,
            });
            put_str(&mut frame, message);
        }
    }
    frame
}

pub(crate) fn decode_response(data: &[u8]) -> io::Result<Response> {
    let mut cursor = Cursor::new(data);
    let response = match cursor.take_u8()? {
        RESP_PUT_OK => Response::PutOk {
            e_tag: cursor.take_str()?,
        },
        RESP_GET_OK => Response::GetOk {
            e_tag: cursor.take_str()?,
            body: cursor.take_bytes()?,
        },
        RESP_HEAD_OK => Response::HeadOk {
            e_tag: cursor.take_str()?,
            content_length: cursor.take_u64()?,
        },
        RESP_DELETE_OK => Response::DeleteOk,
        RESP_LIST_OK => {
            let count = cursor.take_u64()?;
            let mut objects = Vec::new();
            for _ in 0..count {
                objects.push(ObjectSummary {
                    key: cursor.take_str()?,
                    size: cursor.take_u64()?,
                    e_tag: cursor.take_str()?,
                });
            }
            Response::ListOk { objects }
        }
        RESP_ERROR => {
            let code = match cursor.take_u8()? {
                CODE_NO_SUCH_BUCKET => ErrorCode::NoSuchBucket,
                CODE_NO_SUCH_KEY => ErrorCode::NoSuchKey,
                CODE_NOT_FOUND => ErrorCode::NotFound,
                _ => return Err(invalid("unknown error code")),
            };
            Response::Error {
                code,
                message: cursor.take_str()?,
            }
        }
        _ => return Err(invalid("unknown response opcode")),
    };
    cursor.finish()?;
    Ok(response)
}
