use arrow_schema::Schema;
use open_arrow_sink_core::{
    Compression, CompressionOptions, Ordering, SinkCapability, WriteMode, validation,
};

#[test]
fn default_capability_requires_explicit_opt_in() {
    let capability = SinkCapability::default();
    for mode in [WriteMode::Create, WriteMode::Append, WriteMode::Overwrite] {
        assert!(validation::validate_mode_options(mode, capability.supported_modes).is_err());
    }
    assert!(!capability.supports_blob_streaming);
    assert!(
        validation::validate_ordering_options(Ordering::PreserveInput, capability.ordering)
            .is_err()
    );
    assert!(
        validation::validate_compression_options(
            &Schema::empty(),
            &CompressionOptions {
                global: Some(Compression::Uncompressed),
                ..Default::default()
            },
            capability.compression,
        )
        .is_err()
    );
}
