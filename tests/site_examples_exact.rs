// tests/site_examples_exact.rs

use std::time::{SystemTime, UNIX_EPOCH};

use velr::{CellRef, Velr};

fn tmp_db_path(tag: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    std::env::temp_dir()
        .join(format!("velr-site-docs-{tag}-{nanos}.db"))
        .to_string_lossy()
        .into_owned()
}

#[test]
fn site_opening_database_exact() -> velr::Result<()> {
    // In-memory database
    let _db = Velr::open(None)?;

    // Or on-disk
    let path = tmp_db_path("open");
    let _db = Velr::open(Some(&path))?;

    Ok(())
}

#[test]
fn site_run_exact() -> velr::Result<()> {
    let db = Velr::open(None)?;
    db.run("CREATE (:User {name:'Alice'})")?;
    Ok(())
}

#[test]
fn site_exec_exact() -> velr::Result<()> {
    let db = Velr::open(None)?;
    db.run("CREATE (:User {name:'Alice'}), (:User {name:'Bob'})")?;

    let mut stream = db.exec("MATCH (u:User) RETURN u.name AS name")?;

    while let Some(mut table) = stream.next_table()? {
        table.for_each_row(|row| {
            if let CellRef::Text(name) = row[0] {
                println!("User: {}", std::str::from_utf8(name).unwrap());
            }
            Ok(())
        })?;
    }

    Ok(())
}

#[test]
fn site_exec_one_exact() -> velr::Result<()> {
    let db = Velr::open(None)?;
    db.run("CREATE (:User {name:'Alice', age:30})")?;

    let mut table = db.exec_one("MATCH (u:User) RETURN u.name AS name, u.age AS age")?;

    table.for_each_row(|row| {
        let name = match row[0] {
            CellRef::Text(t) => std::str::from_utf8(t).unwrap(),
            _ => "<invalid>",
        };
        let age = match row[1] {
            CellRef::Integer(v) => v,
            _ => -1,
        };

        println!("{name} ({age})");
        Ok(())
    })?;

    Ok(())
}

#[test]
fn site_reading_results_exact() -> velr::Result<()> {
    let db = Velr::open(None)?;
    db.run("CREATE (:Movie {title:'Inception', released:2010})")?;

    let mut table = db.exec_one("MATCH (m:Movie) RETURN m.title AS title, m.released AS year")?;

    println!("{:?}", table.column_names());

    table.for_each_row(|row| {
        let title = match row[0] {
            CellRef::Text(t) => std::str::from_utf8(t).unwrap(),
            _ => "<invalid>",
        };

        let year = match row[1] {
            CellRef::Integer(y) => y,
            _ => -1,
        };

        println!("{title} ({year})");
        Ok(())
    })?;

    Ok(())
}

#[test]
fn site_cellref_as_str_utf8_exact() {
    fn print_cell(cell: CellRef<'_>) {
        if let Some(Ok(s)) = cell.as_str_utf8() {
            println!("{s}");
        }
    }

    print_cell(CellRef::Text(b"hello"));
}

#[test]
fn site_opening_transaction_exact() -> velr::Result<()> {
    let db = Velr::open(None)?;
    let tx = db.begin_tx()?;
    tx.rollback()?;
    Ok(())
}

#[test]
fn site_transactions_exact() -> velr::Result<()> {
    let db = Velr::open(None)?;
    let tx = db.begin_tx()?;

    tx.run("CREATE (:Person {name:'Neo'})")?;

    let mut table = tx.exec_one("MATCH (p:Person) RETURN count(p) AS c")?;

    table.for_each_row(|row| {
        let count = match row[0] {
            CellRef::Integer(v) => v,
            _ => -1,
        };
        println!("count = {count}");
        Ok(())
    })?;

    tx.commit()?;
    Ok(())
}

#[test]
fn site_commit_exact() -> velr::Result<()> {
    let db = Velr::open(None)?;
    let tx = db.begin_tx()?;
    tx.commit()?;
    Ok(())
}

#[test]
fn site_rollback_exact() -> velr::Result<()> {
    let db = Velr::open(None)?;
    let tx = db.begin_tx()?;
    tx.rollback()?;
    Ok(())
}

#[test]
fn site_scoped_savepoint_exact() -> velr::Result<()> {
    let db = Velr::open(None)?;
    let tx = db.begin_tx()?;
    let sp = tx.savepoint()?;

    tx.run("CREATE (:Temp {k:'x'})")?;

    // roll back only to the savepoint
    sp.rollback()?;

    tx.commit()?;
    Ok(())
}

#[test]
fn site_named_savepoint_exact() -> velr::Result<()> {
    let db = Velr::open(None)?;
    let tx = db.begin_tx()?;

    tx.savepoint_named("before_write1")?;
    tx.run("CREATE (:Temp {k:'a'})")?;

    tx.savepoint_named("before_write2")?;
    tx.run("CREATE (:Temp {k:'b'})")?;

    tx.rollback_to("before_write1")?;
    tx.run("CREATE (:Temp {k:'c'})")?;

    tx.release_savepoint("before_write1")?;
    tx.commit()?;

    Ok(())
}

#[test]
fn site_release_named_savepoint_exact() -> velr::Result<()> {
    let db = Velr::open(None)?;
    let tx = db.begin_tx()?;

    tx.savepoint_named("before_write1")?;
    tx.savepoint_named("before_write2")?;

    tx.release_savepoint("before_write2")?;
    tx.release_savepoint("before_write1")?;

    tx.commit()?;
    Ok(())
}

#[test]
fn site_explain_exact() -> velr::Result<()> {
    let db = Velr::open(None)?;

    let trace = db.explain("MATCH (n) RETURN n")?;

    println!("plans: {}", trace.plan_count()?);
    println!("{}", trace.to_compact_string()?);

    Ok(())
}

#[test]
fn site_explain_analyze_exact() -> velr::Result<()> {
    let db = Velr::open(None)?;
    db.run("CREATE (:Node {name:'n1'})")?;

    let trace = db.explain_analyze("MATCH (n) RETURN n")?;
    let snapshot = trace.snapshot()?;

    for plan in snapshot {
        println!("plan {}", plan.meta.plan_id);
        for step in plan.steps {
            println!("  step {}: {}", step.meta.step_no, step.meta.title);
            for stmt in step.statements {
                println!("    {} [{}]", stmt.meta.stmt_id, stmt.meta.kind);
            }
        }
    }

    Ok(())
}

#[cfg(feature = "arrow-ipc")]
#[test]
fn site_bind_arrow_exact() -> velr::Result<()> {
    use arrow2::array::{Array, BooleanArray, Utf8Array};

    let db = Velr::open(None)?;

    let col_names = vec!["name".to_string(), "active".to_string()];
    let arrays: Vec<Box<dyn Array>> = vec![
        Box::new(Utf8Array::<i64>::from(vec![Some("Alice"), Some("Bob")])),
        Box::new(BooleanArray::from(vec![Some(true), Some(false)])),
    ];

    db.bind_arrow("_users", col_names, arrays)?;

    db.run(
        "UNWIND BIND('_users') AS r
         CREATE (:User {name: r.name, active: r.active})",
    )?;

    Ok(())
}

#[cfg(feature = "arrow-ipc")]
#[test]
fn site_bind_arrow_chunks_exact() -> velr::Result<()> {
    use arrow2::array::{Array, PrimitiveArray};

    let db = Velr::open(None)?;

    let col_names = vec!["value".to_string()];
    let chunks_per_col: Vec<Vec<Box<dyn Array>>> = vec![vec![
        Box::new(PrimitiveArray::<i64>::from([Some(1), Some(2)])),
        Box::new(PrimitiveArray::<i64>::from([Some(3)])),
    ]];

    db.bind_arrow_chunks("_chunked", col_names, chunks_per_col)?;

    Ok(())
}

#[cfg(feature = "arrow-ipc")]
#[test]
fn site_bind_arrow_in_tx_exact() -> velr::Result<()> {
    use arrow2::array::{Array, Utf8Array};

    let db = Velr::open(None)?;
    let tx = db.begin_tx()?;

    let col_names = vec!["name".to_string()];
    let arrays: Vec<Box<dyn Array>> = vec![Box::new(Utf8Array::<i64>::from(vec![
        Some("Alice"),
        Some("Bob"),
    ]))];

    tx.bind_arrow("_people", col_names, arrays)?;

    tx.run(
        "UNWIND BIND('_people') AS r
         CREATE (:Person {name: r.name})",
    )?;

    tx.commit()?;
    Ok(())
}

#[cfg(feature = "arrow-ipc")]
#[test]
fn site_to_arrow_ipc_file_exact() -> velr::Result<()> {
    let db = Velr::open(None)?;
    db.run("CREATE (:Movie {title:'Inception', released:2010})")?;

    let mut table = db.exec_one("MATCH (m:Movie) RETURN m.title AS title, m.released AS year")?;

    let ipc_bytes = table.to_arrow_ipc_file()?;
    println!("arrow ipc bytes: {}", ipc_bytes.len());

    Ok(())
}

#[test]
fn site_complete_example_exact() -> velr::Result<()> {
    let db = Velr::open(None)?;

    db.run("CREATE (:Movie {title:'Inception', released:2010})")?;

    let mut table = db.exec_one("MATCH (m:Movie) RETURN m.title AS title, m.released AS year")?;

    table.for_each_row(|row| {
        let title = match row[0] {
            CellRef::Text(t) => std::str::from_utf8(t).unwrap(),
            _ => "<??>",
        };

        let year = match row[1] {
            CellRef::Integer(y) => y,
            _ => -1,
        };

        println!("{title} ({year})");
        Ok(())
    })?;

    Ok(())
}
