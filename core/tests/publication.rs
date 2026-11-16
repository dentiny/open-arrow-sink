use open_arrow_sink_core::{Error, IndexCommitError, PublicationError, PublicationOutcome};
use std::error::Error as _;

#[test]
fn context_preserves_source_and_known_outcome_through_wrappers() {
    let error = Error::Backend(Box::new(std::io::Error::other("response lost")))
        .with_publication_outcome(PublicationOutcome::NotPublished)
        .with_publication_outcome(PublicationOutcome::Unknown);
    assert_eq!(
        error.publication_outcome(),
        PublicationOutcome::NotPublished
    );
    let Error::Backend(source) = &error else {
        panic!("context must be a backend source")
    };
    assert_eq!(
        source.downcast_ref::<PublicationError>().unwrap().outcome,
        PublicationOutcome::NotPublished
    );
    let mut source = error.source().unwrap();
    while let Some(inner) = source.source() {
        source = inner;
    }
    assert_eq!(
        source.downcast_ref::<std::io::Error>().unwrap().to_string(),
        "response lost"
    );
}

#[test]
fn unclassified_errors_never_promise_no_publication() {
    for error in [
        Error::Unsupported("writer poisoned".into()),
        Error::SchemaMismatch,
        Error::Backend(Box::new(std::io::Error::other("request failed"))),
    ] {
        assert_eq!(error.publication_outcome(), PublicationOutcome::Unknown);
    }
}

#[test]
fn postcommit_index_error_remains_directly_downcastable() {
    let error = Error::Backend(Box::new(IndexCommitError {
        message: "index failed".into(),
    }));
    assert_eq!(error.publication_outcome(), PublicationOutcome::Published);
    let Error::Backend(source) = error else {
        unreachable!()
    };
    assert!(source.downcast_ref::<IndexCommitError>().is_some());
}
