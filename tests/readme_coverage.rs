// tests/readme_coverage.rs

use velr::{CellRef, Velr};

fn seed_movies_graph(db: &Velr) -> velr::Result<()> {
    db.run(
        r#"
        CREATE
          (keanu:Person:Actor {name:'Keanu Reeves', born:1964}),
          (laurence:Person:Actor {name:'Laurence Fishburne', born:1961}),
          (carrie:Person:Actor {name:'Carrie-Anne Moss', born:1960}),
          (matrix:Movie {title:'The Matrix', released:1999}),
          (inception:Movie {title:'Inception', released:2010}),
          (keanu)-[:ACTED_IN {roles:['Neo']}]->(matrix),
          (laurence)-[:ACTED_IN {roles:['Morpheus']}]->(matrix),
          (carrie)-[:ACTED_IN {roles:['Trinity']}]->(matrix);
        "#,
    )?;
    Ok(())
}

fn seed_people_for_aggregates(db: &Velr) -> velr::Result<()> {
    db.run(
        r#"
        CREATE
          (:Person {name:'A', born:1960}),
          (:Person {name:'B', born:1962}),
          (:Person {name:'C', born:1964});
        "#,
    )?;
    Ok(())
}

fn one_i64(db: &Velr, query: &str) -> velr::Result<i64> {
    let mut t = db.exec_one(query)?;
    let mut out = None;
    let mut rows = 0usize;

    t.for_each_row(|row| {
        rows += 1;
        assert_eq!(row.len(), 1, "expected 1 column for query: {query}");
        match row[0] {
            CellRef::Integer(i) => out = Some(i),
            other => panic!("expected integer cell, got {other:?} for query: {query}"),
        }
        Ok(())
    })?;

    assert_eq!(rows, 1, "expected 1 row for query: {query}");
    Ok(out.expect("missing integer result"))
}

fn one_text(db: &Velr, query: &str) -> velr::Result<String> {
    let mut t = db.exec_one(query)?;
    let mut out = None;
    let mut rows = 0usize;

    t.for_each_row(|row| {
        rows += 1;
        assert_eq!(row.len(), 1, "expected 1 column for query: {query}");
        match row[0] {
            CellRef::Text(bytes) => {
                out = Some(std::str::from_utf8(bytes).unwrap().to_owned());
            }
            other => panic!("expected text cell, got {other:?} for query: {query}"),
        }
        Ok(())
    })?;

    assert_eq!(rows, 1, "expected 1 row for query: {query}");
    Ok(out.expect("missing text result"))
}

#[test]
fn readme_quick_start_smoke() -> velr::Result<()> {
    let db = Velr::open(None)?;

    db.run("CREATE (:Person {name:'Keanu Reeves', born:1964})")?;

    let mut t = db.exec_one("MATCH (p:Person) RETURN p.name AS name, p.born AS born")?;

    let cols = t.column_names();
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0], "name");
    assert_eq!(cols[1], "born");

    let mut saw_row = false;

    t.for_each_row(|row| {
        saw_row = true;

        match row[0] {
            CellRef::Text(bytes) => {
                assert_eq!(std::str::from_utf8(bytes).unwrap(), "Keanu Reeves");
            }
            other => panic!("expected text in col 0, got {other:?}"),
        }

        match row[1] {
            CellRef::Integer(i) => assert_eq!(i, 1964),
            other => panic!("expected integer in col 1, got {other:?}"),
        }

        Ok(())
    })?;

    assert!(saw_row);
    Ok(())
}

#[test]
fn readme_streaming_multiple_result_tables() -> velr::Result<()> {
    let db = Velr::open(None)?;
    seed_movies_graph(&db)?;

    let mut stream = db.exec(
        r#"
        MATCH (m:Movie {title:'The Matrix'}) RETURN m.title AS title;
        MATCH (m:Movie {title:'Inception'})  RETURN m.released AS year
        "#,
    )?;

    let mut table_idx = 0usize;

    while let Some(mut table) = stream.next_table()? {
        table_idx += 1;

        match table_idx {
            1 => {
                let cols = table.column_names();
                assert_eq!(cols.len(), 1);
                assert_eq!(cols[0], "title");

                let mut saw = false;
                table.for_each_row(|row| {
                    saw = true;
                    match row[0] {
                        CellRef::Text(bytes) => {
                            assert_eq!(std::str::from_utf8(bytes).unwrap(), "The Matrix");
                        }
                        other => panic!("expected text, got {other:?}"),
                    }
                    Ok(())
                })?;
                assert!(saw);
            }
            2 => {
                let cols = table.column_names();
                assert_eq!(cols.len(), 1);
                assert_eq!(cols[0], "year");

                let mut saw = false;
                table.for_each_row(|row| {
                    saw = true;
                    match row[0] {
                        CellRef::Integer(i) => assert_eq!(i, 2010),
                        other => panic!("expected integer, got {other:?}"),
                    }
                    Ok(())
                })?;
                assert!(saw);
            }
            n => panic!("unexpected extra result table #{n}"),
        }
    }

    assert_eq!(table_idx, 2);
    Ok(())
}

#[test]
fn readme_supported_scalar_functions() -> velr::Result<()> {
    let db = Velr::open(None)?;
    seed_movies_graph(&db)?;

    // id()
    let id_val = one_i64(&db, "MATCH (p:Person {name:'Keanu Reeves'}) RETURN id(p)")?;
    assert!(id_val > 0);

    // type()
    assert_eq!(
        one_text(
            &db,
            "MATCH (:Person)-[r:ACTED_IN]->(:Movie) RETURN type(r) LIMIT 1",
        )?,
        "ACTED_IN"
    );

    // length()
    assert_eq!(
        one_i64(
            &db,
            "MATCH p=(:Person {name:'Keanu Reeves'})-[:ACTED_IN]->(:Movie {title:'The Matrix'}) RETURN length(p)",
        )?,
        1
    );

    // nodes()
    assert_has_one_value(
        &db,
        "MATCH p=(:Person {name:'Keanu Reeves'})-[:ACTED_IN]->(:Movie {title:'The Matrix'}) RETURN nodes(p)",
    )?;

    // relationships()
    assert_has_one_value(
        &db,
        "MATCH p=(:Person {name:'Keanu Reeves'})-[:ACTED_IN]->(:Movie {title:'The Matrix'}) RETURN relationships(p)",
    )?;

    // coalesce()
    assert_eq!(
        one_text(
            &db,
            "MATCH (p:Person {name:'Keanu Reeves'}) RETURN coalesce(p.alias, 'n/a')",
        )?,
        "n/a"
    );

    // labels()
    assert_has_one_value(
        &db,
        "MATCH (p:Person {name:'Keanu Reeves'}) RETURN labels(p)",
    )?;

    // properties()
    assert_has_one_value(
        &db,
        "MATCH (p:Person {name:'Keanu Reeves'}) RETURN properties(p)",
    )?;

    // keys()
    assert_has_one_value(&db, "MATCH (p:Person {name:'Keanu Reeves'}) RETURN keys(p)")?;

    Ok(())
}

fn assert_has_one_value(db: &Velr, query: &str) -> velr::Result<()> {
    let mut t = db.exec_one(query)?;
    assert_eq!(
        t.column_names().len(),
        1,
        "expected exactly 1 column for query: {query}"
    );

    let mut rows = 0usize;
    t.for_each_row(|row| {
        rows += 1;
        assert_eq!(row.len(), 1, "expected exactly 1 value for query: {query}");
        Ok(())
    })?;

    assert_eq!(rows, 1, "expected exactly 1 row for query: {query}");
    Ok(())
}
#[test]
fn readme_supported_aggregate_functions() -> velr::Result<()> {
    let db = Velr::open(None)?;
    seed_people_for_aggregates(&db)?;

    // count()
    assert_eq!(one_i64(&db, "MATCH (p:Person) RETURN count(p)")?, 3);

    // sum()
    assert_eq!(one_i64(&db, "MATCH (p:Person) RETURN sum(p.born)")?, 5886);

    // avg()
    assert_has_one_value(&db, "MATCH (p:Person) RETURN avg(p.born)")?;

    // min()
    assert_eq!(one_i64(&db, "MATCH (p:Person) RETURN min(p.born)")?, 1960);

    // max()
    assert_eq!(one_i64(&db, "MATCH (p:Person) RETURN max(p.born)")?, 1964);

    // collect()
    assert_has_one_value(&db, "MATCH (p:Person) RETURN collect(p.name)")?;

    Ok(())
}

#[cfg(feature = "arrow-ipc")]
#[test]
fn readme_arrow_ipc_example() -> velr::Result<()> {
    use arrow2::array::{Array, Utf8Array};

    let db = Velr::open(None)?;

    let cols = vec!["name".to_string()];
    let arrays: Vec<Box<dyn Array>> =
        vec![Utf8Array::<i64>::from(vec![Some("Alice"), Some("Bob")]).boxed()];

    db.bind_arrow("_people", cols, arrays)?;
    db.run("UNWIND BIND('_people') AS r CREATE (:Person {name:r.name})")?;

    let mut t = db.exec_one("MATCH (p:Person) RETURN p.name AS name ORDER BY name")?;
    let ipc = t.to_arrow_ipc_file()?;

    assert!(!ipc.is_empty());
    Ok(())
}
