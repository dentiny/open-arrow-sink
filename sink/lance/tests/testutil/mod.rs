use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

pub(crate) struct Server {
    pub(crate) url: String,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
pub(crate) async fn server() -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let mut handlers = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                connection = listener.accept() => {
                    let (mut socket, _) = connection.unwrap();
                    handlers.spawn(async move {
                        let mut request = Vec::new();
                        let mut buffer = [0; 4096];
                        while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                            let count = socket.read(&mut buffer).await.unwrap();
                            if count == 0 { return; }
                            request.extend_from_slice(&buffer[..count]);
                        }
                        let request = String::from_utf8(request).unwrap();
                        let path = request.split_whitespace().nth(1).unwrap();
                        let size = if path == "/empty" { 0 } else { 17 * 1024 * 1024 };
                        let headers = format!("HTTP/1.1 200 OK\r\nContent-Length: {size}\r\nConnection: close\r\n\r\n");
                        if socket.write_all(headers.as_bytes()).await.is_err() { return; }
                        let chunk = vec![b'x'; 64 * 1024];
                        for _ in 0..size / chunk.len() {
                            if socket.write_all(&chunk).await.is_err() { return; }
                        }
                    });
                }
                _ = handlers.join_next(), if !handlers.is_empty() => {}
            }
        }
    });
    Server { url, task }
}
