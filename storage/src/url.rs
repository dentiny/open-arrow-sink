use ::url::Url;

/// Strip a single well-formed S3 MD5 query as metadata, without verifying the
/// payload checksum. Preserve other queries for normal storage URL validation.
pub(crate) fn sanitize_download_url(url: &mut Url) {
    if url.scheme() == "s3"
        && url
            .query()
            .and_then(|query| query.strip_prefix("md5="))
            .is_some_and(|md5| md5.len() == 32 && md5.bytes().all(|b| b.is_ascii_hexdigit()))
    {
        url.set_query(None);
    }
}
