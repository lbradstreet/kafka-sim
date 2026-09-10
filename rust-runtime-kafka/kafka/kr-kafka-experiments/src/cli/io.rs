use serde::Serialize;
use std::{
    fs,
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
pub const MAX_INDEX_BYTES: usize = 4 * 1024 * 1024;
pub fn read(path: &Path, limit: usize) -> Result<Vec<u8>, String> {
    let file = fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let meta = file.metadata().map_err(|e| e.to_string())?;
    if !meta.is_file() || meta.len() > limit as u64 {
        return Err(format!("{} exceeds its file bound", path.display()));
    }
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    if bytes.len() > limit {
        return Err("file grew past its bound".into());
    }
    Ok(bytes)
}
pub fn imported(root: &Path, relative: &str) -> Result<PathBuf, String> {
    if relative.is_empty()
        || !Path::new(relative)
            .components()
            .all(|c| matches!(c, Component::Normal(_)))
    {
        return Err("artifact paths must be relative normal components".into());
    }
    let root = root.canonicalize().map_err(|e| e.to_string())?;
    let path = root
        .join(relative)
        .canonicalize()
        .map_err(|e| e.to_string())?;
    if !path.starts_with(&root) {
        return Err("artifact path escapes the input directory".into());
    }
    Ok(path)
}
struct Capped<W> {
    inner: W,
    remaining: usize,
}
impl<W: Write> Write for Capped<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.len() > self.remaining {
            return Err(std::io::Error::other("serialized artifact byte cap"));
        }
        let n = self.inner.write(buf)?;
        self.remaining -= n;
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
pub fn write_json(path: &Path, value: &impl Serialize, limit: usize) -> Result<(), String> {
    atomic_write(path, limit, |writer| {
        serde_json::to_writer(writer, value).map_err(|e| e.to_string())
    })
}
pub fn write_bytes(path: &Path, bytes: &[u8], limit: usize) -> Result<(), String> {
    atomic_write(path, limit, |writer| {
        writer.write_all(bytes).map_err(|e| e.to_string())
    })
}
fn atomic_write(
    path: &Path,
    limit: usize,
    write: impl FnOnce(&mut dyn Write) -> Result<(), String>,
) -> Result<(), String> {
    let parent = path.parent().ok_or("artifact parent")?;
    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    let temp = path.with_extension(format!(
        "tmp-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| {
        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .map_err(|e| e.to_string())?;
        let mut writer = Capped {
            inner: std::io::BufWriter::with_capacity(1024 * 1024, file),
            remaining: limit,
        };
        write(&mut writer)?;
        writer.flush().map_err(|e| e.to_string())?;
        fs::rename(&temp, path).map_err(|e| e.to_string())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn partial_serialization_does_not_replace_a_completed_artifact() {
        let dir = std::env::temp_dir().join(format!("experiment-atomic-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let p = dir.join("data.json");
        fs::write(&p, b"old").unwrap();
        assert!(write_json(&p, &vec!["large"; 100], 10).is_err());
        assert_eq!(fs::read(&p).unwrap(), b"old");
        assert!(imported(&dir, "../data.json").is_err());
        assert!(imported(&dir, "/data.json").is_err());
        fs::remove_dir_all(dir).unwrap();
    }
}
