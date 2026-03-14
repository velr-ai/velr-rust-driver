// tests/readme_examples_exact.rs

use std::time::{SystemTime, UNIX_EPOCH};

use velr::{CellRef, Velr};

fn tmp_db_path(tag: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    std::env::temp_dir()
        .join(format!("velr-readme-{tag}-{nanos}.db"))
        .to_string_lossy()
        .into_owned()
}

#[test]
fn readme_opening_database_exact() -> velr::Result<()> {
    // In-memory database
    let _db = Velr::open(None)?;

    // File-backed database
    let path = tmp_db_path("open");
    let _db = Velr::open(Some(&path))?;

    Ok(())
}

#[test]
fn readme_quick_start_exact() -> velr::Result<()> {
    use velr::{CellRef, Velr};

    // Open in-memory DB (pass Some("path.db") for file-backed)
    let db = Velr::open(None)?;

    db.run("CREATE (:Person {name:'Keanu Reeves', born:1964})")?;

    let mut t = db.exec_one("MATCH (p:Person) RETURN p.name AS name, p.born AS born")?;

    println!("{:?}", t.column_names());

    t.for_each_row(|row| {
        match row[0] {
            CellRef::Text(bytes) => println!("name={}", std::str::from_utf8(bytes).unwrap()),
            _ => {}
        }
        match row[1] {
            CellRef::Integer(i) => println!("born={i}"),
            _ => {}
        }
        Ok(())
    })?;

    Ok(())
}

#[test]
fn readme_streaming_multiple_result_tables_exact() -> velr::Result<()> {
    let db = Velr::open(None)?;

    db.run(
        r#"
        CREATE
          (:Movie {title:'The Matrix', released:1999}),
          (:Movie {title:'Inception', released:2010})
        "#,
    )?;

    let mut stream = db.exec(
        "MATCH (m:Movie {title:'The Matrix'}) RETURN m.title AS title;
         MATCH (m:Movie {title:'Inception'})  RETURN m.released AS year",
    )?;

    while let Some(mut table) = stream.next_table()? {
        println!("{:?}", table.column_names());
        table.for_each_row(|row| {
            println!("{row:?}");
            Ok(())
        })?;
    }

    Ok(())
}

#[test]
fn readme_transactions_scoped_savepoint_exact() -> velr::Result<()> {
    let db = Velr::open(None)?;

    let tx = db.begin_tx()?;
    tx.run("CREATE (:Temp {k:'outer'})")?;

    {
        let sp = tx.savepoint()?;
        tx.run("CREATE (:Temp {k:'inner'})")?;
        sp.rollback()?; // rollback to the scoped savepoint
    }

    tx.commit()?;
    Ok(())
}

#[test]
fn readme_transactions_named_savepoints_exact() -> velr::Result<()> {
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

#[cfg(feature = "arrow-ipc")]
#[test]
fn readme_arrow_example_exact() -> velr::Result<()> {
    use arrow2::array::{Array, Utf8Array};

    let db = Velr::open(None)?;

    let cols = vec!["name".to_string()];
    let arrays: Vec<Box<dyn Array>> =
        vec![Utf8Array::<i64>::from(vec![Some("Alice"), Some("Bob")]).boxed()];

    db.bind_arrow("_people", cols, arrays)?;
    db.run("UNWIND BIND('_people') AS r CREATE (:Person {name:r.name})")?;

    let mut t = db.exec_one("MATCH (p:Person) RETURN p.name AS name ORDER BY name")?;
    let ipc = t.to_arrow_ipc_file()?;

    println!("IPC bytes: {}", ipc.len());
    Ok(())
}

#[test]
fn named_savepoint_rollback_to_earlier_marker_semantics() -> velr::Result<()> {
    let db = Velr::open(None)?;
    let tx = db.begin_tx()?;

    tx.savepoint_named("before_write1")?;
    tx.run("CREATE (:T {k:'a'})")?;

    tx.savepoint_named("before_write2")?;
    tx.run("CREATE (:T {k:'b'})")?;

    tx.rollback_to("before_write1")?;
    tx.run("CREATE (:T {k:'c'})")?;

    tx.release_savepoint("before_write1")?;
    tx.commit()?;

    let mut table = db.exec_one("MATCH (n:T) RETURN n.k AS k ORDER BY k")?;
    let mut values = Vec::new();

    table.for_each_row(|row| {
        if let CellRef::Text(bytes) = row[0] {
            values.push(std::str::from_utf8(bytes).unwrap().to_string());
        }
        Ok(())
    })?;

    assert_eq!(values, vec!["c"]);
    Ok(())
}
