use open_arrow_sink_core::{Error, WriteMode, WriteOptions, validation::validate_mode_options};

#[test]
fn omitted_mode_preserves_backend_default() {
    assert_eq!(WriteOptions::default().mode, None);
    assert!(validate_mode_options(None, &[]).is_ok());
}

#[test]
fn explicit_modes_require_capability_support() {
    for mode in [WriteMode::Create, WriteMode::Append, WriteMode::Overwrite] {
        assert!(validate_mode_options(Some(mode), &[mode]).is_ok());
        assert!(matches!(
            validate_mode_options(Some(mode), &[]),
            Err(Error::Unsupported(_))
        ));
    }
}
