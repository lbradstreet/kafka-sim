//! The deterministic S3 model.
//!
//! State is `BTreeMap`-ordered so listings and iteration order are
//! deterministic by construction, never by hasher seed.

use crate::sim::etag;
use crate::sim::wire::{ErrorCode, ObjectSummary, Request, Response};
use std::cell::RefCell;
use std::collections::BTreeMap;

struct StoredObject {
    body: Vec<u8>,
    e_tag: String,
}

/// An in-memory S3 model with S3's observable semantics for the operations
/// the shim supports: last-write-wins puts, idempotent deletes, complete
/// key-ordered listings, and typed missing-bucket/missing-key errors.
#[derive(Default)]
pub struct S3Service {
    buckets: RefCell<BTreeMap<String, BTreeMap<String, StoredObject>>>,
}

impl S3Service {
    /// Returns an empty model.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a bucket; creating an existing bucket is a no-op.
    pub fn create_bucket(&self, bucket: &str) {
        self.buckets
            .borrow_mut()
            .entry(bucket.to_string())
            .or_default();
    }

    pub(crate) fn handle(&self, request: &Request) -> Response {
        match request {
            Request::Put { bucket, key, body } => self.put(bucket, key, body),
            Request::Get { bucket, key } => self.get(bucket, key),
            Request::Head { bucket, key } => self.head(bucket, key),
            Request::Delete { bucket, key } => self.delete(bucket, key),
            Request::List { bucket, prefix } => self.list(bucket, prefix.as_deref()),
        }
    }

    fn put(&self, bucket: &str, key: &str, body: &[u8]) -> Response {
        let mut buckets = self.buckets.borrow_mut();
        let Some(objects) = buckets.get_mut(bucket) else {
            return no_such_bucket(bucket);
        };
        let e_tag = etag::compute(body);
        objects.insert(
            key.to_string(),
            StoredObject {
                body: body.to_vec(),
                e_tag: e_tag.clone(),
            },
        );
        Response::PutOk { e_tag }
    }

    fn get(&self, bucket: &str, key: &str) -> Response {
        let buckets = self.buckets.borrow();
        let Some(objects) = buckets.get(bucket) else {
            return no_such_bucket(bucket);
        };
        match objects.get(key) {
            Some(object) => Response::GetOk {
                e_tag: object.e_tag.clone(),
                body: object.body.clone(),
            },
            None => Response::Error {
                code: ErrorCode::NoSuchKey,
                message: format!("key {key:?} does not exist"),
            },
        }
    }

    fn head(&self, bucket: &str, key: &str) -> Response {
        let buckets = self.buckets.borrow();
        let Some(objects) = buckets.get(bucket) else {
            return Response::Error {
                code: ErrorCode::NotFound,
                message: format!("bucket {bucket:?} does not exist"),
            };
        };
        match objects.get(key) {
            Some(object) => Response::HeadOk {
                e_tag: object.e_tag.clone(),
                content_length: object.body.len() as u64,
            },
            None => Response::Error {
                code: ErrorCode::NotFound,
                message: format!("key {key:?} does not exist"),
            },
        }
    }

    fn delete(&self, bucket: &str, key: &str) -> Response {
        let mut buckets = self.buckets.borrow_mut();
        let Some(objects) = buckets.get_mut(bucket) else {
            return no_such_bucket(bucket);
        };
        // S3's delete is idempotent: removing an absent key succeeds.
        objects.remove(key);
        Response::DeleteOk
    }

    fn list(&self, bucket: &str, prefix: Option<&str>) -> Response {
        let buckets = self.buckets.borrow();
        let Some(objects) = buckets.get(bucket) else {
            return no_such_bucket(bucket);
        };
        let objects = objects
            .iter()
            .filter(|(key, _)| prefix.is_none_or(|prefix| key.starts_with(prefix)))
            .map(|(key, object)| ObjectSummary {
                key: key.clone(),
                size: object.body.len() as u64,
                e_tag: object.e_tag.clone(),
            })
            .collect();
        Response::ListOk { objects }
    }
}

fn no_such_bucket(bucket: &str) -> Response {
    Response::Error {
        code: ErrorCode::NoSuchBucket,
        message: format!("bucket {bucket:?} does not exist"),
    }
}
