use std::error::Error as _;

use velr::{Error, ErrorDetail, ErrorKind, Velr};

fn require_details(error: &Error) -> Option<&velr::ErrorDiagnostic> {
    let details = error.diagnostic();
    if std::env::var_os("VELR_REQUIRE_STRUCTURED_ERRORS").is_some() {
        assert!(details.is_some(), "runtime omitted metadata: {error}");
    }
    details
}

#[test]
fn query_and_constraint_errors_expose_typed_identity() {
    let db = Velr::open(None).unwrap();
    let query = db.run("RETURN toUppper('a')").unwrap_err();
    if let Some(details) = require_details(&query) {
        assert_eq!(query.kind(), ErrorKind::Syntax);
        assert!(query.is_query_error());
        assert_eq!(query.detail(), Some(ErrorDetail::UnknownFunction));
        assert_eq!(details.phase.as_deref(), Some("compile"));
        assert!(details.detail.is_some());
    }

    db.run("CREATE (a:Guarded)-[:R]->(:Guarded)").unwrap();
    let constraint = db.run("MATCH (n:Guarded) DELETE n").unwrap_err();
    if let Some(details) = require_details(&constraint) {
        assert_eq!(constraint.kind(), ErrorKind::Constraint);
        assert!(constraint.is_query_error());
        assert_eq!(constraint.detail(), Some(ErrorDetail::DeleteConnectedNode));
        assert_eq!(details.detail_kind(), constraint.detail());
        assert_eq!(details.detail.as_deref(), Some("DeleteConnectedNode"));
        assert!(!constraint.is_retryable());
    }
}

#[test]
fn local_errors_preserve_display_sources_and_stop_codes() {
    let error = Error::with_source(
        -1,
        "conversion failed",
        std::io::Error::other("original cause"),
    );
    assert_eq!(error.to_string(), "velr error (code -1): conversion failed");
    assert_eq!(error.source().unwrap().to_string(), "original cause");
    assert_eq!(
        error.clone().source().unwrap().to_string(),
        "original cause"
    );
    assert_eq!(Error::new(-5, "cancelled").kind(), ErrorKind::Cancelled);
    assert_eq!(
        Error::new(-6, "deadline").kind(),
        ErrorKind::DeadlineExceeded
    );
    assert_eq!(Error::new(-3, "closed").kind(), ErrorKind::HandleState);
    assert!(error.diagnostic().is_none());
}

#[test]
fn deferred_row_failure_keeps_owned_diagnostics() {
    let db = Velr::open(None).unwrap();
    let mut table = db.exec_one("UNWIND [2, 0] AS n RETURN 2 / n").unwrap();
    let mut rows = table.rows().unwrap();
    let error = loop {
        match rows.next(|_| Ok(())) {
            Ok(true) => {}
            Ok(false) => panic!("division by zero must fail during row fetching"),
            Err(error) => break error,
        }
    };
    drop(rows);
    drop(table);
    drop(db);
    if let Some(details) = require_details(&error) {
        assert_eq!(error.kind(), ErrorKind::Arithmetic);
        assert!(error.is_query_error());
        assert_eq!(error.detail(), Some(ErrorDetail::DivisionByZero));
        assert_eq!(details.detail.as_deref(), Some("DivisionByZero"));
        assert_eq!(details.phase.as_deref(), Some("runtime"));
    }
}

#[test]
fn native_filesystem_failures_preserve_a_rust_source() {
    let nonexistent = std::env::temp_dir()
        .join(format!(
            "velr-error-missing-parent-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
        .join("database.sqlite");
    let error = Velr::open(Some(nonexistent.to_str().unwrap()))
        .err()
        .expect("missing parent rejected");
    if let Some(details) = require_details(&error) {
        assert_eq!(error.kind(), ErrorKind::Io);
        assert!(!error.is_query_error());
        assert_eq!(details.io_kind.as_deref(), Some("not_found"));
        assert!(error
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .is_some());
    }
}

#[test]
fn application_errors_accept_messages_and_preserve_original_sources() {
    for error in [
        Error::application("application rejected the row"),
        Error::application(String::from("application rejected the row")),
    ] {
        assert_eq!(error.message, "application rejected the row");
        assert_eq!(error.source().unwrap().to_string(), error.message);
        assert_eq!(error.kind(), ErrorKind::Other);
        assert!(!error.is_query_error());
        assert!(!error.is_retryable());
        assert!(error.diagnostic().is_none());
        assert!(error.detail().is_none());
    }

    let source: Box<dyn std::error::Error + Send + Sync> = Box::new(std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "input rejected",
    ));
    let error = Error::application(source);
    assert_eq!(error.message, "input rejected");
    assert_eq!(
        error
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .kind(),
        std::io::ErrorKind::PermissionDenied
    );
    let cloned = error.clone();
    assert!(std::ptr::eq(
        error.source().unwrap(),
        cloned.source().unwrap()
    ));
}

#[test]
fn application_errors_work_with_map_err_and_row_callbacks() {
    fn parse_integer(input: &str) -> velr::Result<i64> {
        input.parse().map_err(Error::application)
    }

    assert_eq!(parse_integer("42").unwrap(), 42);
    assert!(parse_integer("invalid")
        .unwrap_err()
        .source()
        .unwrap()
        .is::<std::num::ParseIntError>());

    let db = Velr::open(None).unwrap();
    let mut table = db.exec_one("UNWIND [1, 2] AS n RETURN n").unwrap();
    let mut visited = 0;
    let error = table
        .for_each_row(|_| {
            visited += 1;
            Err(Error::application(std::io::Error::other("consumer failed")))
        })
        .unwrap_err();
    assert_eq!(visited, 1);
    assert_eq!(error.message, "consumer failed");
    assert_eq!(error.kind(), ErrorKind::Other);
    assert!(!error.is_query_error());
    assert!(error.detail().is_none());
    assert!(error.source().unwrap().is::<std::io::Error>());
    drop(table);
    db.run("RETURN 42").unwrap();
}
