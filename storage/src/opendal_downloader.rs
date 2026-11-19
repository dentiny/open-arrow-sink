//! Default source dispatch and OpenDAL download I/O.
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
    http_metrics: Option<open_arrow_sink_core::IoObservers>,
}
impl OpenDalDownloader {
    pub fn new(config: StorageConfig) -> Result<Self> {
        opendal::install_default();
        let http = config.apply(
            Operator::new(Http::default().endpoint(HTTP_PLACEHOLDER.trim_end_matches('/')))
                .map_err(|error| error::map_error("configure_download", HTTP_PLACEHOLDER, error))?,
        )?;
        let transport = http.base_context().http_transport().clone();
        Ok(Self {
            http,
            transport,
            http_budget: None,
            http_metrics: None,
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
        let crate::backend::UnobservedStorage { storage, observers } = storage.without_io_metrics();
        self.http_metrics = observers;
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
        if let Some(stream) = crate::inline_source::open(source, &context)? {
            return Ok(stream);
        }
        let mut url = crate::Url::parse(source).map_err(Error::backend)?;
        let (operator, path) = if matches!(url.scheme(), "http" | "https") {
            let uri = validation::validate_download_url(source)?;
            // Resolve credentials from explicit source namespaces. Keep query strings
            // on the request URI, but omit them when matching storage scopes.
            let mut scope = url.clone();
            scope.set_query(None);
            scope.set_fragment(None);
            let downloader = match context.sources.configured_storage_for(&scope) {
                Some(storage) => self.clone().with_http_storage(storage)?,
                None => self.clone(),
            };
            let transport = if downloader
                .http_budget
                .as_ref()
                .is_some_and(|limit| limit.shares_budget(&context.http_limit))
            {
                downloader.transport.clone()
            } else {
                context.http_limit.wrap(downloader.transport.clone())
            };
            let transport = HttpTransporter::new(UrlTransport {
                uri,
                inner: transport,
            });
            let mut operator = downloader.http.clone();
            if let Some(observers) = context.unobserved_metrics(downloader.http_metrics.clone()) {
                for metrics in observers.iter() {
                    operator = operator.layer(crate::IoMetricsLayer(metrics.clone()));
                }
            }
            let ctx = operator.base_context().with_http_transport(transport);
            (operator.with_context(ctx), "blob".to_owned())
        } else {
            crate::url::sanitize_download_url(&mut url);
            let crate::ResolvedSource { storage, path } = context.resolve_source(&url)?;
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
        let mut builder = operator.reader_with(&path);
        if let Some(io_request_size) = context.io_request_size() {
            builder = builder.chunk(io_request_size.get());
        }
        let mut reader = builder
            .await
            .map_err(|error| error::map_error("read", &path, error))?
            .into_stream(..)
            .await
            .map_err(|error| error::map_error("read", &path, error))?;
        Ok(Box::pin(async_stream::try_stream! {
            while let Some(mut buffer) = reader.try_next().await.map_err(|error| error::map_error("read", &path, error))? {
                while !buffer.is_empty() {
                    let size = context.chunk_bytes().min(buffer.len());
                    yield buffer.split_to(size).to_bytes();
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
