//! Structured diagnostics returned with an individual runtime error.

use serde_json::Value;

/// A source location in the original submitted Cypher text.
///
/// Both coordinates start at one. Columns count Unicode display width, matching the rendered
/// diagnostic, rather than UTF-8 bytes or UTF-16 code units. A diagnostic omits the position when no precise location
/// is known.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ErrorPosition {
    /// One-based source line.
    pub line: usize,
    /// One-based display column in the rendered query.
    pub column: usize,
}

/// A recognized stable detail identifier supplied with a runtime diagnostic.
///
/// Details describe the specific reason for a failure; they do not establish its
/// broad category, query phase, or retryability. Some details can appear under
/// more than one category. Use the error's classification alongside this value.
/// [`ErrorDiagnostic::detail_kind`] returns [`Self::Other`] for an identifier this
/// driver does not recognize, preserving the original string in
/// [`ErrorDiagnostic::detail`]. Future releases can add variants.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorDetail {
    /// The query contains syntax that cannot be parsed or accepted.
    UnexpectedSyntax,
    /// A query variable is not defined in the current scope.
    UndefinedVariable,
    /// A query calls an unknown function.
    UnknownFunction,
    /// A variable is bound again where a fresh binding is required.
    VariableAlreadyBound,
    /// A variable is used with an incompatible graph-entity or value type.
    VariableTypeConflict,
    /// A function call supplies an unsupported number of arguments.
    InvalidArgumentCount,
    /// A procedure call supplies an unsupported number of arguments.
    InvalidNumberOfArguments,
    /// An argument has an incompatible type.
    InvalidArgumentType,
    /// An argument has a value outside the operation's supported domain.
    InvalidArgumentValue,
    /// A numeric argument is outside its permitted range.
    NumberOutOfRange,
    /// An expression uses aggregation in an unsupported way.
    InvalidAggregation,
    /// An expression mixes aggregation with a reference that is not a grouping key.
    AmbiguousAggregationExpression,
    /// An aggregate expression contains another aggregate where nesting is forbidden.
    NestedAggregation,
    /// The branches of a union expose different result columns.
    DifferentColumnsInUnion,
    /// A result contains conflicting column names.
    ColumnNameConflict,
    /// A property assignment contains a value that cannot be stored as a property.
    InvalidPropertyType,
    /// A query parameter is used in a position that does not accept it.
    InvalidParameterUse,
    /// The caller did not supply a required query parameter.
    MissingParameter,
    /// A procedure name is unavailable on this connection.
    UnknownProcedure,
    /// Procedure resolution reports an unavailable procedure under the active profile.
    ProcedureNotFound,
    /// A query accesses an entity deleted in the current transaction.
    DeletedEntityAccess,
    /// A node cannot be deleted while it still has relationships.
    DeleteConnectedNode,
    /// A merge pattern contains a null property value that cannot identify an entity.
    MergeReadOwnWrites,
    /// An arithmetic operation divides by zero.
    DivisionByZero,
    /// Integer arithmetic or a literal exceeds the supported integer range.
    IntegerOverflow,
    /// A floating-point literal exceeds the finite floating-point range.
    FloatingPointOverflow,
    /// Database access conflicts with another connection.
    DatabaseBusy,
    /// Database access conflicts with an active operation or shared-cache lock.
    DatabaseLocked,
    /// A write requires a writable database connection.
    ReadOnlyConnection,
    /// The database requires explicit migration before the requested operation.
    MigrationRequired,
    /// The database schema does not satisfy the runtime's format requirements.
    DatabaseSchemaMismatch,
    /// A read-only open requires an initialized database.
    DatabaseUninitialized,
    /// The database could not be opened or created at the requested location.
    DatabaseOpenFailed,
    /// Database creation encountered an existing destination.
    DatabaseAlreadyExists,
    /// An operation requiring one result table produced no result tables.
    NoResultTables,
    /// An operation requiring one result table produced more than one result table.
    MultipleResultTables,
    /// Query execution or result fetching stopped after cancellation.
    QueryCancelled,
    /// Query execution or result fetching exceeded its deadline.
    DeadlineExceeded,
    /// The runtime supplied an identifier this driver does not recognize.
    ///
    /// Read [`ErrorDiagnostic::detail`] to retain or log its original value.
    Other,
}

/// Machine-readable information accompanying a rendered runtime diagnostic.
///
/// Category and detail identifiers are independent of diagnostic wording.
/// Optional information remains absent when the runtime cannot supply it.
/// Older runtime libraries can return an error without this information.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ErrorDiagnostic {
    /// Broad operation kind, such as `query`, `io`, `state`, or `argument`.
    pub kind: Option<String>,
    /// Stable broad classification, such as `SyntaxError` or `DatabaseBusy`.
    pub category: Option<String>,
    /// Stable detail identifier within the category.
    pub detail: Option<String>,
    /// Language-defined phase: `compile` or `runtime`, when applicable.
    pub phase: Option<String>,
    /// Human-readable explanation. Its wording may change between releases.
    pub message: String,
    /// Optional human-readable remediation guidance.
    pub hint: Option<String>,
    /// Precise source location when known.
    pub position: Option<ErrorPosition>,
    /// Whether the failure condition may clear on a later attempt.
    ///
    /// This does not guarantee that replaying a write is safe. Follow the
    /// transaction's recovery rules before retrying an operation.
    pub retryable: bool,
    /// SQLite extended result code, when SQLite reported the failure.
    pub sqlite_errorcode: Option<i32>,
    /// Symbolic SQLite result-code name, when known.
    pub sqlite_errorname: Option<String>,
    /// Portable filesystem failure kind when supplied by the runtime.
    pub io_kind: Option<String>,
    /// Original operating-system error number for this runtime's platform.
    pub os_code: Option<i32>,
    /// Filesystem operation associated with the error, when known.
    pub operation: Option<String>,
    /// Filesystem path associated with the error, when known.
    pub path: Option<String>,
}

impl ErrorDiagnostic {
    /// Return the recognized detail while preserving the raw identifier separately.
    ///
    /// Returns `None` when [`Self::detail`] is absent, and
    /// `Some(`[`ErrorDetail::Other`]`)` when a present identifier is unknown to this
    /// driver. Matching is exact and case-sensitive; no diagnostic message text is
    /// interpreted. The method does not allocate or modify the diagnostic.
    pub fn detail_kind(&self) -> Option<ErrorDetail> {
        let detail = self.detail.as_deref()?;
        Some(match detail {
            "UnexpectedSyntax" => ErrorDetail::UnexpectedSyntax,
            "UndefinedVariable" => ErrorDetail::UndefinedVariable,
            "UnknownFunction" => ErrorDetail::UnknownFunction,
            "VariableAlreadyBound" => ErrorDetail::VariableAlreadyBound,
            "VariableTypeConflict" => ErrorDetail::VariableTypeConflict,
            "InvalidArgumentCount" => ErrorDetail::InvalidArgumentCount,
            "InvalidNumberOfArguments" => ErrorDetail::InvalidNumberOfArguments,
            "InvalidArgumentType" => ErrorDetail::InvalidArgumentType,
            "InvalidArgumentValue" => ErrorDetail::InvalidArgumentValue,
            "NumberOutOfRange" => ErrorDetail::NumberOutOfRange,
            "InvalidAggregation" => ErrorDetail::InvalidAggregation,
            "AmbiguousAggregationExpression" => ErrorDetail::AmbiguousAggregationExpression,
            "NestedAggregation" => ErrorDetail::NestedAggregation,
            "DifferentColumnsInUnion" => ErrorDetail::DifferentColumnsInUnion,
            "ColumnNameConflict" => ErrorDetail::ColumnNameConflict,
            "InvalidPropertyType" => ErrorDetail::InvalidPropertyType,
            "InvalidParameterUse" => ErrorDetail::InvalidParameterUse,
            "MissingParameter" => ErrorDetail::MissingParameter,
            "UnknownProcedure" => ErrorDetail::UnknownProcedure,
            "ProcedureNotFound" => ErrorDetail::ProcedureNotFound,
            "DeletedEntityAccess" => ErrorDetail::DeletedEntityAccess,
            "DeleteConnectedNode" => ErrorDetail::DeleteConnectedNode,
            "MergeReadOwnWrites" => ErrorDetail::MergeReadOwnWrites,
            "DivisionByZero" => ErrorDetail::DivisionByZero,
            "IntegerOverflow" => ErrorDetail::IntegerOverflow,
            "FloatingPointOverflow" => ErrorDetail::FloatingPointOverflow,
            "DatabaseBusy" => ErrorDetail::DatabaseBusy,
            "DatabaseLocked" => ErrorDetail::DatabaseLocked,
            "ReadOnlyConnection" => ErrorDetail::ReadOnlyConnection,
            "MigrationRequired" => ErrorDetail::MigrationRequired,
            "DatabaseSchemaMismatch" => ErrorDetail::DatabaseSchemaMismatch,
            "DatabaseUninitialized" => ErrorDetail::DatabaseUninitialized,
            "DatabaseOpenFailed" => ErrorDetail::DatabaseOpenFailed,
            "DatabaseAlreadyExists" => ErrorDetail::DatabaseAlreadyExists,
            "NoResultTables" => ErrorDetail::NoResultTables,
            "MultipleResultTables" => ErrorDetail::MultipleResultTables,
            "QueryCancelled" => ErrorDetail::QueryCancelled,
            "DeadlineExceeded" => ErrorDetail::DeadlineExceeded,
            _ => ErrorDetail::Other,
        })
    }

    pub(crate) fn from_json(bytes: &[u8]) -> Option<Self> {
        let value: Value = serde_json::from_slice(bytes).ok()?;
        let object = value.as_object()?;
        if object.get("version").and_then(Value::as_u64) != Some(1) {
            return None;
        }
        let text = |name: &str| object.get(name).and_then(Value::as_str).map(str::to_owned);
        let phase = text("phase").filter(|phase| matches!(phase.as_str(), "compile" | "runtime"));
        let position = object
            .get("position")
            .and_then(|position| {
                let line = usize::try_from(position.get("line")?.as_u64()?).ok()?;
                let column = usize::try_from(position.get("column")?.as_u64()?).ok()?;
                (line > 0 && column > 0).then_some(ErrorPosition { line, column })
            })
            .or_else(|| {
                let line = usize::try_from(object.get("line")?.as_u64()?).ok()?;
                let column = usize::try_from(object.get("column")?.as_u64()?).ok()?;
                (line > 0 && column > 0).then_some(ErrorPosition { line, column })
            });
        Some(Self {
            kind: text("kind"),
            category: text("category"),
            detail: text("detail"),
            phase,
            message: text("message")?,
            hint: text("hint"),
            position,
            retryable: object
                .get("retryable")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            sqlite_errorcode: object
                .get("sqlite_errorcode")
                .and_then(Value::as_i64)
                .and_then(|code| i32::try_from(code).ok()),
            sqlite_errorname: text("sqlite_errorname"),
            io_kind: text("io_kind"),
            os_code: object
                .get("os_code")
                .and_then(Value::as_i64)
                .and_then(|code| i32::try_from(code).ok()),
            operation: text("operation"),
            path: text("path"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_version_and_optional_fields_without_interpreting_diagnostic_prose() {
        let details = ErrorDiagnostic::from_json(
            br#"{
            "version":1,"category":"TypeError","detail":"InvalidArgumentType",
            "phase":"runtime","message":"explanation","position":{"line":2,"column":3},
            "retryable":false,"future_field":true
        }"#,
        )
        .unwrap();
        assert_eq!(details.category.as_deref(), Some("TypeError"));
        assert_eq!(details.position.unwrap().column, 3);
        assert!(ErrorDiagnostic::from_json(br#"{"version":2,"message":"unknown"}"#).is_none());
        assert!(
            ErrorDiagnostic::from_json(b"SyntaxError at compile time: InvalidArgumentType")
                .is_none()
        );
        let details = ErrorDiagnostic::from_json(
            br#"{"version":1,"message":"unchanged","position":{"line":0,"column":1}}"#,
        )
        .unwrap();
        assert!(details.position.is_none());
    }

    #[test]
    fn typed_details_preserve_raw_identifiers_and_forward_compatibility() {
        for (identifier, expected) in [
            ("UnknownFunction", ErrorDetail::UnknownFunction),
            ("InvalidArgumentType", ErrorDetail::InvalidArgumentType),
            ("MissingParameter", ErrorDetail::MissingParameter),
            ("UnknownProcedure", ErrorDetail::UnknownProcedure),
            ("ProcedureNotFound", ErrorDetail::ProcedureNotFound),
            ("DeletedEntityAccess", ErrorDetail::DeletedEntityAccess),
            ("DeleteConnectedNode", ErrorDetail::DeleteConnectedNode),
            ("DivisionByZero", ErrorDetail::DivisionByZero),
            ("DatabaseBusy", ErrorDetail::DatabaseBusy),
            ("DatabaseLocked", ErrorDetail::DatabaseLocked),
            ("ReadOnlyConnection", ErrorDetail::ReadOnlyConnection),
            ("MigrationRequired", ErrorDetail::MigrationRequired),
            ("NoResultTables", ErrorDetail::NoResultTables),
            ("QueryCancelled", ErrorDetail::QueryCancelled),
            ("DeadlineExceeded", ErrorDetail::DeadlineExceeded),
            ("FutureRuntimeDetail", ErrorDetail::Other),
            ("unknownfunction", ErrorDetail::Other),
            (" UnknownFunction", ErrorDetail::Other),
            ("", ErrorDetail::Other),
        ] {
            let payload = serde_json::json!({
                "version": 1,
                "detail": identifier,
                "message": "an unrelated diagnostic",
            });
            let details = ErrorDiagnostic::from_json(&serde_json::to_vec(&payload).unwrap())
                .expect("versioned diagnostic");
            assert_eq!(details.detail_kind(), Some(expected), "{identifier:?}");
            assert_eq!(details.detail.as_deref(), Some(identifier));
            let mut error = crate::Error::new(-4, "an unrelated report");
            error.diagnostic = Some(details);
            assert_eq!(error.detail(), Some(expected));
        }

        for payload in [
            br#"{"version":1,"message":"UnknownFunction"}"#.as_slice(),
            br#"{"version":1,"detail":null,"message":"MissingParameter"}"#.as_slice(),
            br#"{"version":1,"detail":7,"message":"DivisionByZero"}"#.as_slice(),
        ] {
            let details = ErrorDiagnostic::from_json(payload).unwrap();
            assert_eq!(details.detail_kind(), None);
        }
        assert_eq!(crate::Error::application("UnknownFunction").detail(), None);
    }

    #[test]
    fn query_family_helper_excludes_operational_and_application_failures() {
        for category in [
            "SyntaxError",
            "SemanticError",
            "TypeError",
            "ArgumentError",
            "ParameterMissing",
            "ProcedureNotFound",
            "ProcedureError",
            "EntityNotFound",
            "ConstraintVerificationFailed",
            "ArithmeticError",
            "FutureQueryCategory",
        ] {
            let mut error = crate::Error::new(-4, "unrelated rendered diagnostic");
            error.diagnostic = Some(ErrorDiagnostic {
                kind: Some("query".into()),
                category: Some(category.into()),
                ..Default::default()
            });
            assert!(error.is_query_error(), "{category}");
            for status in [-5, -6] {
                error.code = status;
                assert!(!error.is_query_error(), "query stop overrides {category}");
            }
        }

        for (kind, category, detail, phase) in [
            ("argument", "ArgumentError", "InvalidArgumentValue", ""),
            ("state", "DatabaseBusy", "DatabaseBusy", "runtime"),
            ("state", "ReadOnlyError", "ReadOnlyConnection", "runtime"),
            ("state", "DatabaseStateError", "MigrationRequired", ""),
            ("state", "HandleState", "", ""),
            ("io", "IOError", "DatabaseOpenFailed", ""),
            ("state", "QueryCancelled", "QueryCancelled", "runtime"),
            ("state", "DeadlineExceeded", "DeadlineExceeded", "runtime"),
            ("state", "FutureStateCategory", "", "runtime"),
        ] {
            let mut error = crate::Error::new(-4, "SyntaxError at compile time: UnknownFunction");
            error.diagnostic = Some(ErrorDiagnostic {
                kind: Some(kind.into()),
                category: Some(category.into()),
                detail: (!detail.is_empty()).then(|| detail.into()),
                phase: (!phase.is_empty()).then(|| phase.into()),
                ..Default::default()
            });
            assert!(!error.is_query_error(), "{kind}/{category}/{detail}");
        }

        assert!(
            !crate::Error::new(-4, "SyntaxError at compile time: UnknownFunction").is_query_error()
        );
        assert!(!crate::Error::application("a query-shaped application message").is_query_error());
        let mut unclassified = crate::Error::new(-4, "unclassified");
        unclassified.diagnostic = Some(ErrorDiagnostic::default());
        assert!(!unclassified.is_query_error());
    }

    #[test]
    fn query_kind_comes_from_typed_operation_identity() {
        use crate::{Error, ErrorKind};

        for (payload, expected) in [
            (
                br#"{"version":1,"kind":"argument","category":"ArgumentError","detail":"InvalidCreationOptions","message":"invalid options"}"#.as_slice(),
                ErrorKind::Other,
            ),
            (
                br#"{"version":1,"kind":"query","category":"ArgumentError","detail":"InvalidArgumentValue","message":"invalid query argument"}"#.as_slice(),
                ErrorKind::Parameter,
            ),
            (
                br#"{"version":1,"kind":"query","category":"FutureQueryCategory","message":"future query failure"}"#.as_slice(),
                ErrorKind::Query,
            ),
            (
                br#"{"version":1,"kind":"state","category":"FutureStateCategory","phase":"runtime","message":"runtime state failure"}"#.as_slice(),
                ErrorKind::Other,
            ),
        ] {
            let mut error = Error::new(-4, "original diagnostic");
            error.diagnostic = ErrorDiagnostic::from_json(payload);
            assert_eq!(error.kind(), expected, "{}", String::from_utf8_lossy(payload));
        }
    }
}
