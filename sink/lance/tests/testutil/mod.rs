pub(crate) use open_arrow_sink_test_support::http::Server;
use open_arrow_sink_test_support::http::{Reply, serve};

pub(crate) async fn server() -> Server {
    serve(|uri, _| {
        Reply::repeated(
            b'x',
            if uri.path() == "/empty" {
                0
            } else {
                17 * 1024 * 1024
            },
        )
    })
    .await
}
