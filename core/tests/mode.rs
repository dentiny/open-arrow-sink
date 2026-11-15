use open_arrow_sink_core::{Error, WriteMode, WriteOptions, validation::validate_mode_options};

#[test]
fn create_is_the_uniform_default() {
    assert_eq!(WriteMode::default(), WriteMode::Create);
    assert_eq!(WriteOptions::default().mode, WriteMode::Create);
}

#[test]
fn default_mode_requires_capability_support() {
    assert!(validate_mode_options(WriteOptions::default().mode, &[WriteMode::Create]).is_ok());
    assert!(matches!(
        validate_mode_options(WriteOptions::default().mode, &[WriteMode::Append]),
        Err(Error::Unsupported(_))
    ));
}

#[test]
fn all_modes_require_capability_support() {
    for mode in [WriteMode::Create, WriteMode::Append, WriteMode::Overwrite] {
        assert!(validate_mode_options(mode, &[mode]).is_ok());
        assert!(matches!(
            validate_mode_options(mode, &[]),
            Err(Error::Unsupported(_))
        ));
    }
}
