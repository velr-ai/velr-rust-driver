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
