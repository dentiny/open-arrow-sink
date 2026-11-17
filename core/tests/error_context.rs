use open_arrow_sink_core::{Error, IndexCommitError};
use std::error::Error as _;

#[test]
fn context_preserves_actionable_categories() {
    let errors = [
        Error::Unsupported("message".into()),
        Error::AlreadyExists("message".into()),
        Error::InvalidConfig("message".into()),
        Error::InvalidInput("message".into()),
        Error::NotFound("message".into()),
    ];
    for error in errors {
        let category = std::mem::discriminant(&error);
        let contextual = error.context("destination");
        assert_eq!(std::mem::discriminant(&contextual), category);
        assert!(contextual.to_string().ends_with("destination: message"));
    }
}

#[test]
fn context_keeps_lifecycle_and_publication_signals() {
    let errors = [
        Error::Poisoned,
        Error::SchemaMismatch,
        Error::IndexCommit(IndexCommitError::new("index unavailable")),
    ];
    for error in errors {
        let category = std::mem::discriminant(&error);
        let message = error.to_string();
        let contextual = error.context("destination");
        assert_eq!(std::mem::discriminant(&contextual), category);
        assert_eq!(contextual.to_string(), message);
    }
}

#[test]
fn repeated_context_keeps_native_backend_source() {
    let error = Error::backend(std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "native denial",
    ))
    .context("read object")
    .context(format_args!("blob row {}", 7));
    assert!(matches!(error, Error::Backend(_)));
    assert!(
        error
            .to_string()
            .contains("blob row 7: backend error: read object")
    );
    let mut source: &(dyn std::error::Error + 'static) = &error;
    loop {
        if let Some(native) = source.downcast_ref::<std::io::Error>() {
            assert_eq!(native.kind(), std::io::ErrorKind::PermissionDenied);
            assert_eq!(native.to_string(), "native denial");
            break;
        }
        source = source.source().expect("native backend source is retained");
    }
    assert!(error.source().is_some());
}
