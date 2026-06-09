use velr::{MigrationStatus, Velr};

#[test]
fn migration_api_reports_current_in_memory_database() -> velr::Result<()> {
    let db = Velr::open(None)?;

    let schema_version = match db.schema_version() {
        Ok(version) => version,
        Err(err) if err.message.contains("does not expose velr_schema_version") => {
            eprintln!("SKIP migration API smoke test: {}", err.message);
            return Ok(());
        }
        Err(err) => return Err(err),
    };

    let current_schema_version = db.current_schema_version()?;
    assert_eq!(schema_version, current_schema_version);
    assert!(!db.needs_migration()?);

    let report = db.migrate()?;
    assert_eq!(report.from_version, schema_version);
    assert_eq!(report.to_version, current_schema_version);
    assert_eq!(report.status, MigrationStatus::AlreadyCurrent);
    assert!(report.steps.is_empty());

    Ok(())
}
