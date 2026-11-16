use open_arrow_sink_core::{Ordering, WriteOptions, validation};

#[test]
fn default_ordering_is_accepted_by_unordered_sinks() {
    let options = WriteOptions::default();
    assert_eq!(Ordering::default(), Ordering::Unordered);
    assert_eq!(options.ordering, Ordering::Unordered);
    validation::validate_ordering_options(options.ordering, Ordering::Unordered).unwrap();
}

#[test]
fn preserve_input_requires_an_explicit_backend_guarantee() {
    assert!(
        validation::validate_ordering_options(Ordering::PreserveInput, Ordering::Unordered)
            .is_err()
    );
    validation::validate_ordering_options(Ordering::PreserveInput, Ordering::PreserveInput)
        .unwrap();
}
