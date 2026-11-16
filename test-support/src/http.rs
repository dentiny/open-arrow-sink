//! Valid HTTP fixtures with bounded generated bodies and request-aware routing.
use axum::{
    Router,
    body::{Body, Bytes},
    http::{Response, StatusCode, Uri},
    routing::any,
};
use futures::stream;
use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

pub struct Server {
    pub url: String,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A response generated for each request. `gate` delays the body, not headers.
pub struct Reply {
    status: StatusCode,
    delay: Duration,
    gate: Option<oneshot::Receiver<()>>,
    body: Payload,
}
enum Payload {
    Bytes(Bytes),
    Repeated { byte: u8, size: usize },
}
impl Reply {
    pub fn bytes(bytes: impl Into<Bytes>) -> Self {
        Self {
            status: StatusCode::OK,
            delay: Duration::ZERO,
            gate: None,
            body: Payload::Bytes(bytes.into()),
        }
    }
    /// Generate at most 64 KiB per body chunk, independent of total size.
    pub fn repeated(byte: u8, size: usize) -> Self {
        Self {
            status: StatusCode::OK,
            delay: Duration::ZERO,
            gate: None,
            body: Payload::Repeated { byte, size },
        }
    }
    pub fn status(mut self, status: StatusCode) -> Self {
        self.status = status;
        self
    }
    pub fn delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }
    pub fn gated(mut self, gate: oneshot::Receiver<()>) -> Self {
        self.gate = Some(gate);
        self
    }
    async fn response(self) -> Response<Body> {
        tokio::time::sleep(self.delay).await;
        let size = match &self.body {
            Payload::Bytes(bytes) => bytes.len(),
            Payload::Repeated { size, .. } => *size,
        };
        let body = stream::unfold((self.body, self.gate), |(payload, gate)| async move {
            if let Some(gate) = gate {
                let _ = gate.await;
            }
            match payload {
                Payload::Bytes(bytes) if bytes.is_empty() => None,
                Payload::Bytes(bytes) => Some((
                    Ok::<_, Infallible>(bytes),
                    (Payload::Bytes(Bytes::new()), None),
                )),
                Payload::Repeated { size: 0, .. } => None,
                Payload::Repeated { byte, size } => {
                    let count = size.min(64 * 1024);
                    Some((
                        Ok(Bytes::from(vec![byte; count])),
                        (
                            Payload::Repeated {
                                byte,
                                size: size - count,
                            },
                            None,
                        ),
                    ))
                }
            }
        });
        Response::builder()
            .status(self.status)
            .header("content-length", size)
            .body(Body::from_stream(body))
            .unwrap()
    }
}

/// Route on the original URI (including escaped path/query) and zero-based attempt.
/// The handler runs for each request, including retries, and can capture counters
/// or channels for assertions without manually parsing TCP/HTTP framing.
pub async fn serve(handler: impl Fn(Uri, usize) -> Reply + Send + Sync + 'static) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let handler = Arc::new(handler);
    let attempts = Arc::new(AtomicUsize::new(0));
    let app = Router::new().fallback(any(move |uri: Uri| {
        let reply = handler(uri, attempts.fetch_add(1, Ordering::SeqCst));
        async move { reply.response().await }
    }));
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Server { url, task }
}

pub use axum::http::StatusCode as HttpStatus;
