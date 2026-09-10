//! A component written once against the tokio-shaped facade.
//!
//! Compiled against real tokio without `--cfg kr_runtime_sim` and against the kr-runtime
//! runtimes with it; the test files assert the same component behaves
//! according to each backend's clock.

use kr_runtime_tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use kr_runtime_tokio::time::{Duration, Instant, sleep};
use std::io;

/// Sleeps `count` times for `tick` and reports the completed count and the
/// elapsed time observed through the facade's own clock.
pub(crate) async fn timed_ticks(count: u32, tick: Duration) -> (u32, Duration) {
    let start = Instant::now();
    let mut completed = 0;
    for _ in 0..count {
        sleep(tick).await;
        completed += 1;
    }
    (completed, start.elapsed())
}

/// Server side of one exchange: reads until EOF, echoes everything back,
/// then shuts down its write half. Returns what it received.
pub(crate) async fn echo_once<S>(mut stream: S) -> io::Result<Vec<u8>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut received = Vec::new();
    stream.read_to_end(&mut received).await?;
    stream.write_all(&received).await?;
    stream.shutdown().await?;
    Ok(received)
}

/// Client side of one exchange: writes the payload, half-closes so the
/// server observes EOF, then reads the full response.
pub(crate) async fn request_response<S>(mut stream: S, payload: &[u8]) -> io::Result<Vec<u8>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream.write_all(payload).await?;
    stream.shutdown().await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    Ok(response)
}
