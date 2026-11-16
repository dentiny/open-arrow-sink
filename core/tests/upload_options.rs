use std::num::NonZeroUsize;

use arrow_schema::Schema;
use open_arrow_sink_core::{
    Ordering, SinkCapability, UploadConfig, WriteMode, WriteOptions, validation,
};

#[test]
fn explicit_upload_options_require_backend_support() {
    let capability = SinkCapability::default().with_supported_modes(&[WriteMode::Create]);
    let defaults = WriteOptions::default().with_ordering(Ordering::Unordered);
    validation::validate_options(&Schema::empty(), &defaults, capability.clone()).unwrap();
    for upload in [
        UploadConfig::default().with_part_size(NonZeroUsize::new(16 * 1024 * 1024)),
        UploadConfig::default().with_concurrency(NonZeroUsize::new(4)),
    ] {
        let options = defaults.clone().with_upload(upload);
        assert!(matches!(
            validation::validate_options(&Schema::empty(), &options, capability.clone()),
            Err(open_arrow_sink_core::Error::Unsupported(_))
        ));
        validation::validate_options(
            &Schema::empty(),
            &options,
            capability.clone().with_supports_upload_options(true),
        )
        .unwrap();
    }
}
