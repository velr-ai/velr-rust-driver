use serde_json::Value;
use velr::{CellRef, Velr};

#[derive(Debug, PartialEq)]
enum Owned {
    Null,
    Bool(bool),
    Int(i64),
    F64(f64),
    Text(Vec<u8>),
    Json(Value),
}

fn own_cell(c: &CellRef<'_>) -> Owned {
    match c {
        CellRef::Null => Owned::Null,
        CellRef::Bool(b) => Owned::Bool(*b),
        CellRef::Integer(i) => Owned::Int(*i),
        CellRef::Float(f) => Owned::F64(*f),
        CellRef::Text(t) => Owned::Text(t.to_vec()),
        CellRef::Json(j) => Owned::Json(serde_json::from_slice(j).expect("valid json")),
    }
}

fn assert_f64(a: f64, b: f64) {
    let diff = (a - b).abs();
    assert!(diff < 1e-12, "expected {b}, got {a} (diff {diff})");
}

#[test]
fn roundtrip_all_types_non_tx() -> velr::Result<()> {
    let db = Velr::open(None)?;

    // Seed one node with all types.
    // NOTE: tb is TEXT "false" (must stay Text), while b is BOOL true.
    db.run(
        r#"
        CREATE (:Sample {
          n: null,
          b: true,
          i: 123,
          f: 3.75,
          ta: 'hello',
          tb: 'false',
          arr: ['a','b']
        });
        "#,
    )?;

    let mut t = db.exec_one(
        "MATCH (s:Sample)
         RETURN s.n AS n, s.b AS b, s.i AS i, s.f AS f, s.ta AS ta, s.tb AS tb, s.arr AS arr",
    )?;

    assert_eq!(
        t.column_names(),
        &["n", "b", "i", "f", "ta", "tb", "arr"]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
    );

    let mut rows = Vec::<Vec<Owned>>::new();
    t.for_each_row(|r| {
        rows.push(r.iter().map(own_cell).collect());
        Ok(())
    })?;

    assert_eq!(rows.len(), 1);
    let r = &rows[0];

    assert_eq!(r[0], Owned::Null);
    assert_eq!(r[1], Owned::Bool(true));
    assert_eq!(r[2], Owned::Int(123));

    match r[3] {
        Owned::F64(f) => assert_f64(f, 3.75),
        ref other => panic!("expected Float, got {other:?}"),
    }

    assert_eq!(r[4], Owned::Text(b"hello".to_vec()));
    assert_eq!(r[5], Owned::Text(b"false".to_vec())); // important: NOT Bool(false)

    // JSON checks (structure-based, not string-based)
    assert_eq!(r[6], Owned::Json(serde_json::json!(["a", "b"])));

    Ok(())
}

#[test]
fn exec_stream_two_tables() -> velr::Result<()> {
    let db = Velr::open(None)?;

    db.run(
        r#"
        CREATE
          (:Movie {title:'The Matrix', released:1999}),
          (:Movie {title:'Inception', released:2010});
        "#,
    )?;

    // Two statements => two result tables via streaming API
    let mut st = db.exec(
        "MATCH (m:Movie {title:'The Matrix'}) RETURN m.title AS title;
         MATCH (m:Movie {title:'Inception'})  RETURN m.released AS y",
    )?;

    // Table 1
    {
        let mut t1 = st.next_table()?.expect("table1");
        assert_eq!(t1.column_names(), &["title".to_string()]);
        let mut got = Vec::new();
        t1.for_each_row(|r| {
            got.push(own_cell(&r[0]));
            Ok(())
        })?;
        assert_eq!(got, vec![Owned::Text(b"The Matrix".to_vec())]);
    }

    // Table 2
    {
        let mut t2 = st.next_table()?.expect("table2");
        assert_eq!(t2.column_names(), &["y".to_string()]);
        let mut got = Vec::new();
        t2.for_each_row(|r| {
            got.push(own_cell(&r[0]));
            Ok(())
        })?;
        assert_eq!(got, vec![Owned::Int(2010)]);
    }

    // EOF
    assert!(st.next_table()?.is_none());
    Ok(())
}

fn count_label(db: &Velr, label: &str) -> velr::Result<i64> {
    let mut t = db.exec_one(&format!("MATCH (n:{label}) RETURN count(n) AS c"))?;
    let mut out = None;
    t.for_each_row(|r| {
        if let CellRef::Integer(i) = r[0] {
            out = Some(i);
            Ok(())
        } else {
            Err(velr::Error {
                code: -1,
                message: "expected integer".into(),
            })
        }
    })?;
    Ok(out.unwrap_or(0))
}

#[test]
fn tx_commit_rollback_savepoints_and_drop() -> velr::Result<()> {
    let db = Velr::open(None)?;

    // TX #1 commit path
    {
        let tx = db.begin_tx()?;
        tx.run("CREATE (:Temp {k:'t1'})")?;
        tx.commit()?;
    }
    assert_eq!(count_label(&db, "Temp")?, 1);

    // TX #2 rollback path
    {
        let tx = db.begin_tx()?;
        tx.run("CREATE (:Temp {k:'t2'})")?;
        tx.rollback()?;
    }
    assert_eq!(count_label(&db, "Temp")?, 1);

    // TX #3 savepoint drop rolls back inner work
    {
        let tx = db.begin_tx()?;
        tx.run("CREATE (:Temp {k:'outer'})")?;

        {
            let _sp = tx.savepoint()?;
            tx.run("CREATE (:Temp {k:'inner'})")?;
            // dropping _sp triggers savepoint rollback per your FFI contract
        }

        tx.commit()?;
    }
    // inner should have been rolled back; outer persisted
    // total now: previous 1 + outer = 2
    assert_eq!(count_label(&db, "Temp")?, 2);

    // TX #4 named savepoint + rollback_to
    {
        let tx = db.begin_tx()?;

        {
            // Savepoint handle borrows tx, so keep it in this inner scope.
            let _sp = tx.savepoint_named("spx")?;
            tx.run("CREATE (:Temp {k:'will_rollback_to_spx'})")?;

            // Exercise rollback_to API (this releases the savepoint in the engine)
            tx.rollback_to("spx")?;

            // _sp drops here (its Drop may attempt rollback/release again; your impl ignores errors)
        }

        tx.commit()?;
    }
    // no change because we rolled back inside tx
    assert_eq!(count_label(&db, "Temp")?, 2);

    // TX #5 drop without commit/rollback => velr_tx_close => RAII rollback
    {
        let tx = db.begin_tx()?;
        tx.run("CREATE (:Temp {k:'drop_rollback'})")?;
        drop(tx); // should rollback
    }
    assert_eq!(count_label(&db, "Temp")?, 2);

    Ok(())
}

#[test]
fn named_savepoints_can_rollback_to_earlier_marker() -> velr::Result<()> {
    let db = Velr::open(None)?;
    let tx = db.begin_tx()?;

    tx.savepoint_named("before_write1")?;
    tx.run("CREATE (:T {k:'a'})")?;

    tx.savepoint_named("before_write2")?;
    tx.run("CREATE (:T {k:'b'})")?;

    tx.rollback_to("before_write1")?;
    tx.run("CREATE (:T {k:'c'})")?;

    tx.commit()?;
    Ok(())
}

#[test]
fn named_savepoints_can_be_released_from_top_of_stack() -> velr::Result<()> {
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

#[test]
fn scoped_savepoint_rollback_semantics() -> velr::Result<()> {
    let db = Velr::open(None)?;
    let tx = db.begin_tx()?;

    tx.run("CREATE (:T {k:'outer'})")?;

    let sp = tx.savepoint()?;
    tx.run("CREATE (:T {k:'inner'})")?;
    sp.rollback()?;

    tx.commit()?;

    let mut table = db.exec_one("MATCH (n:T) RETURN n.k AS k ORDER BY k")?;
    let mut values = Vec::new();

    table.for_each_row(|row| {
        match row[0] {
            CellRef::Text(bytes) => {
                values.push(std::str::from_utf8(bytes).unwrap().to_string());
            }
            other => panic!("expected text, got {other:?}"),
        }
        Ok(())
    })?;

    assert_eq!(values, vec!["outer"]);
    Ok(())
}

#[cfg(feature = "arrow-ipc")]
mod arrow {
    use super::*;
    use arrow2::array::{Array, BooleanArray, PrimitiveArray, Utf8Array};

    #[test]
    fn arrow_bind_non_tx_roundtrip() -> velr::Result<()> {
        let db = Velr::open(None)?;

        let cols = vec!["name".into(), "b".into(), "i".into(), "f".into()];
        let arrays: Vec<Box<dyn Array>> = vec![
            Utf8Array::<i64>::from(vec![Some("Alice"), Some("Bob")]).boxed(),
            BooleanArray::from(vec![Some(true), Some(false)]).boxed(),
            PrimitiveArray::<i64>::from(vec![Some(123), Some(-7)]).boxed(),
            PrimitiveArray::<f64>::from(vec![Some(3.75), Some(2.1)]).boxed(),
        ];

        db.bind_arrow("_people", cols, arrays)?;

        db.run(
            "UNWIND BIND('_people') AS r
             CREATE (:Tmp { name: r.name, b: r.b, i: r.i, f: r.f });",
        )?;

        let mut t = db.exec_one(
            "MATCH (r:Tmp)
             RETURN r.name AS name, r.b AS b, r.i AS i, r.f AS f
             ORDER BY name",
        )?;

        let mut got = Vec::<(String, bool, i64, f64)>::new();
        t.for_each_row(|r| {
            let name = match r[0] {
                CellRef::Text(s) => std::str::from_utf8(s).unwrap().to_string(),
                _ => panic!("name"),
            };
            let b = matches!(r[1], CellRef::Bool(true));
            let i = match r[2] {
                CellRef::Integer(x) => x,
                _ => panic!("i"),
            };
            let f = match r[3] {
                CellRef::Float(x) => x,
                _ => panic!("f"),
            };
            got.push((name, b, i, f));
            Ok(())
        })?;

        assert_eq!(got.len(), 2);
        assert_eq!(got[0].0, "Alice");
        assert_eq!(got[0].1, true);
        assert_eq!(got[0].2, 123);
        assert_f64(got[0].3, 3.75);

        assert_eq!(got[1].0, "Bob");
        assert_eq!(got[1].1, false);
        assert_eq!(got[1].2, -7);
        assert_f64(got[1].3, 2.1);

        Ok(())
    }

    #[test]
    fn arrow_ipc_export_smoke() -> velr::Result<()> {
        let db = Velr::open(None)?;
        db.run("CREATE (:X {k: 1})")?;

        let mut t = db.exec_one("MATCH (x:X) RETURN x.k AS k")?;
        let bytes = t.to_arrow_ipc_file()?;

        // Arrow IPC file magic is "ARROW1" at the start
        assert!(bytes.len() > 8);
        assert_eq!(&bytes[..6], b"ARROW1");

        Ok(())
    }
}

#[cfg(test)]
mod explain_tests {
    use super::*;
    use velr::Velr;

    fn assert_trace_basic(
        trace: &velr::ExplainTrace,
        expected_cypher_fragment: &str,
    ) -> velr::Result<()> {
        let plan_count = trace.plan_count()?;
        assert!(plan_count >= 1, "expected at least one plan");

        let plan0 = trace.plan_meta(0)?;
        assert!(
            plan0.cypher.contains(expected_cypher_fragment),
            "expected cypher to contain {:?}, got {:?}",
            expected_cypher_fragment,
            plan0.cypher
        );

        let step_count_via_meta = plan0.step_count;
        let step_count_via_api = trace.step_count(0)?;
        assert_eq!(step_count_via_meta, step_count_via_api);
        assert!(step_count_via_api >= 1, "expected at least one step");

        let compact_len = trace.compact_len()?;
        let compact_bytes = trace.to_compact_bytes()?;
        let compact_string = trace.to_compact_string()?;

        assert_eq!(compact_len, compact_bytes.len());
        assert_eq!(compact_bytes, compact_string.as_bytes());
        assert!(
            !compact_string.is_empty(),
            "compact explain should not be empty"
        );

        // Snapshot should be internally consistent with the getter API.
        let snap = trace.snapshot()?;
        assert_eq!(snap.len(), plan_count);

        let total_steps: usize = snap.iter().map(|p| p.steps.len()).sum();
        assert!(
            total_steps >= 1,
            "snapshot should contain at least one step"
        );

        let total_statements: usize = snap
            .iter()
            .flat_map(|p| p.steps.iter())
            .map(|s| s.statements.len())
            .sum();
        assert!(
            total_statements >= 1,
            "snapshot should contain at least one statement"
        );

        // Walk the raw metadata API too, and verify counts match.
        for plan_idx in 0..plan_count {
            let pm = trace.plan_meta(plan_idx)?;
            assert_eq!(pm.step_count, trace.step_count(plan_idx)?);

            for step_idx in 0..pm.step_count {
                let sm = trace.step_meta(plan_idx, step_idx)?;
                assert_eq!(
                    sm.statement_count,
                    trace.statement_count(plan_idx, step_idx)?
                );

                for stmt_idx in 0..sm.statement_count {
                    let stm = trace.statement_meta(plan_idx, step_idx, stmt_idx)?;
                    assert_eq!(
                        stm.sqlite_plan_count,
                        trace.sqlite_plan_count(plan_idx, step_idx, stmt_idx)?
                    );
                    assert!(
                        !stm.sql.is_empty(),
                        "statement sql should not be empty at {plan_idx}/{step_idx}/{stmt_idx}"
                    );

                    let details = trace.sqlite_plan_details(plan_idx, step_idx, stmt_idx)?;
                    assert_eq!(details.len(), stm.sqlite_plan_count);

                    for (detail_idx, d) in details.iter().enumerate() {
                        let d2 =
                            trace.sqlite_plan_detail(plan_idx, step_idx, stmt_idx, detail_idx)?;
                        assert_eq!(&d2, d);
                    }
                }
            }
        }

        Ok(())
    }

    #[test]
    fn explain_non_tx_snapshot_and_compact() -> velr::Result<()> {
        let db = Velr::open(None)?;

        db.run(
            r#"
            CREATE
              (:Movie {title:'The Matrix', released:1999}),
              (:Movie {title:'Inception', released:2010});
            "#,
        )?;

        let q = "MATCH (m:Movie {title:'The Matrix'}) RETURN m.title AS title";
        let trace = db.explain(q)?;

        assert_trace_basic(&trace, "MATCH (m:Movie")?;

        // Make sure snapshot contains something useful.
        let snap = trace.snapshot()?;
        assert!(!snap.is_empty());

        let first_plan = &snap[0];
        assert!(first_plan.meta.step_count >= 1);
        assert!(first_plan.meta.cypher.contains("The Matrix"));

        Ok(())
    }

    #[test]
    fn explain_analyze_non_tx_smoke() -> velr::Result<()> {
        let db = Velr::open(None)?;

        db.run(
            r#"
            CREATE
              (:Movie {title:'The Matrix'}),
              (:Movie {title:'Inception'}),
              (:Movie {title:'Memento'});
            "#,
        )?;

        let q = "MATCH (m:Movie) RETURN count(m) AS c";
        let trace = db.explain_analyze(q)?;

        assert_trace_basic(&trace, "MATCH (m:Movie)")?;

        let compact = trace.to_compact_string()?;
        assert!(
            compact.contains("MATCH") || compact.contains("RETURN") || compact.contains("count"),
            "compact explain did not contain expected query-related text: {compact:?}"
        );

        Ok(())
    }

    #[test]
    fn explain_tx_smoke() -> velr::Result<()> {
        let db = Velr::open(None)?;

        let tx = db.begin_tx()?;
        tx.run("CREATE (:Temp {k:'inside_tx'})")?;

        let q = "MATCH (t:Temp) RETURN count(t) AS c";
        let trace = tx.explain(q)?;

        assert_trace_basic(&trace, "MATCH (t:Temp)")?;

        // The tx should still be usable after explain().
        let mut t = tx.exec_one(q)?;
        let mut got = None;
        t.for_each_row(|r| {
            match r[0] {
                CellRef::Integer(i) => got = Some(i),
                _ => panic!("expected integer count"),
            }
            Ok(())
        })?;
        assert_eq!(got, Some(1));

        tx.rollback()?;
        Ok(())
    }

    #[test]
    fn explain_analyze_tx_smoke() -> velr::Result<()> {
        let db = Velr::open(None)?;

        let tx = db.begin_tx()?;
        tx.run(
            r#"
            CREATE
              (:X {v: 1}),
              (:X {v: 2}),
              (:X {v: 3});
            "#,
        )?;

        let q = "MATCH (x:X) RETURN count(x) AS c";
        let trace = tx.explain_analyze(q)?;

        assert_trace_basic(&trace, "MATCH (x:X)")?;

        let snap = trace.snapshot()?;
        let stmt_count: usize = snap
            .iter()
            .flat_map(|p| p.steps.iter())
            .map(|s| s.statements.len())
            .sum();
        assert!(stmt_count >= 1);

        tx.rollback()?;
        Ok(())
    }

    #[test]
    fn explain_sqlite_detail_access_smoke() -> velr::Result<()> {
        let db = Velr::open(None)?;

        db.run(
            r#"
            CREATE
              (:Movie {title:'The Matrix', released:1999}),
              (:Movie {title:'Inception', released:2010});
            "#,
        )?;

        let trace = db
            .explain("MATCH (m:Movie) WHERE m.title = 'Inception' RETURN m.released AS released")?;

        let mut found_statement = false;

        for plan_idx in 0..trace.plan_count()? {
            let step_count = trace.step_count(plan_idx)?;
            for step_idx in 0..step_count {
                let stmt_count = trace.statement_count(plan_idx, step_idx)?;
                for stmt_idx in 0..stmt_count {
                    let stmt = trace.statement_meta(plan_idx, step_idx, stmt_idx)?;
                    if !stmt.sql.is_empty() {
                        found_statement = true;

                        let details = trace.sqlite_plan_details(plan_idx, step_idx, stmt_idx)?;
                        for d in details {
                            assert!(!d.is_empty(), "sqlite detail should not be empty");
                        }
                    }
                }
            }
        }

        assert!(
            found_statement,
            "expected at least one non-empty SQL statement"
        );
        Ok(())
    }
}
