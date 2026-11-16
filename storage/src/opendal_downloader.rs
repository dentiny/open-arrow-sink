//! Default download I/O, with no destination writes or payload accumulation.
use crate::{
    DownloadContext, DownloadStream, Downloader, HttpConcurrencyLimitLayer, Storage, StorageConfig,
    error, validation,
};
use futures::TryStreamExt;
use http::{Request, Response, Uri};
use open_arrow_sink_core::{Error, Result, async_trait};
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter, Operator, services::Http};

const HTTP_PLACEHOLDER: &str = "https://download.invalid/";

#[derive(Clone, Debug)]
pub struct OpenDalDownloader {
    http: Operator,
    transport: HttpTransporter,
    http_budget: Option<HttpConcurrencyLimitLayer>,
}
impl OpenDalDownloader {
    pub fn new(config: StorageConfig) -> Result<Self> {
        opendal::install_default();
        let http = config.apply(
            Operator::new(Http::default().endpoint(HTTP_PLACEHOLDER.trim_end_matches('/')))
                .map_err(Error::backend)?,
        )?;
        let transport = http.base_context().http_transport().clone();
        Ok(Self {
            http,
            transport,
            http_budget: None,
        })
    }
    pub fn with_http_config(mut self, config: StorageConfig) -> Result<Self> {
        self.http = config.apply(self.http)?;
        Ok(self)
    }
    pub fn with_http_transport(mut self, transport: HttpTransporter) -> Self {
        self.transport = match &self.http_budget {
            Some(limit) => limit.wrap(transport),
            None => transport,
        };
        self
    }
    pub fn with_http_storage(mut self, storage: Storage) -> Result<Self> {
        if storage.operator().info().scheme() != "http" {
            return Err(error::http_storage_service());
        }
        self.http_budget = Some(storage.http_concurrency_limit());
        self.http = storage.operator();
        self.transport = self.http.base_context().http_transport().clone();
        Ok(self)
    }
    /// Configure a HTTP namespace for clients that share transport policies.
    pub fn http_storage(config: StorageConfig) -> Result<Storage> {
        let source = Self::new(config)?;
        Storage::new(
            source.http,
            HTTP_PLACEHOLDER.parse().map_err(Error::backend)?,
        )
    }
}
#[async_trait]
impl Downloader for OpenDalDownloader {
    async fn open(&self, source: &str, context: DownloadContext) -> Result<DownloadStream> {
        let url = crate::Url::parse(source).map_err(Error::backend)?;
        let (operator, path) = if matches!(url.scheme(), "http" | "https") {
            let uri = validation::validate_download_url(source)?;
            let transport = if self
                .http_budget
                .as_ref()
                .is_some_and(|limit| limit.shares_budget(&context.http_limit))
            {
                self.transport.clone()
            } else {
                context.http_limit.wrap(self.transport.clone())
            };
            let transport = HttpTransporter::new(UrlTransport {
                uri,
                inner: transport,
            });
            let operator = self.http.clone();
            let ctx = operator.base_context().with_http_transport(transport);
            (operator.with_context(ctx), "blob".to_owned())
        } else {
            let (storage, path) = context.resolve_source(&url)?;
            let operator = storage.operator();
            let operator = if storage
                .http_concurrency_limit()
                .shares_budget(&context.http_limit)
            {
                operator
            } else {
                let ctx = operator.base_context();
                let transport = context.http_limit.wrap(ctx.http_transport().clone());
                let ctx = ctx.with_http_transport(transport);
                operator.with_context(ctx)
            };
            (operator, path.as_str().to_owned())
        };
        let mut reader = operator
            .reader(&path)
            .await
            .map_err(Error::backend)?
            .into_stream(..)
            .await
            .map_err(Error::backend)?;
        Ok(Box::pin(async_stream::try_stream! {
            while let Some(mut buffer) = reader.try_next().await.map_err(Error::backend)? {
                while !buffer.is_empty() {
                    let size = context.chunk_bytes().min(buffer.len());
                    yield bytes::Bytes::from(buffer.split_to(size).to_vec());
                }
            }
        }))
    }
}
struct UrlTransport {
    uri: Uri,
    inner: HttpTransporter,
}
impl HttpTransport for UrlTransport {
    async fn fetch(&self, mut request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        *request.uri_mut() = self.uri.clone();
        Ok(error::download_response(self.inner.fetch(request).await?))
    }
}
