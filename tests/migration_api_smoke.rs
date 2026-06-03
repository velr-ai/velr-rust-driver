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

    assert_eq!(schema_version, 4);
    assert_eq!(db.current_schema_version()?, 4);
    assert!(!db.needs_migration()?);

    let report = db.migrate()?;
    assert_eq!(report.from_version, 4);
    assert_eq!(report.to_version, 4);
    assert_eq!(report.status, MigrationStatus::AlreadyCurrent);
    assert!(report.steps.is_empty());

    Ok(())
}
