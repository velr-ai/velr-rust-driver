use std::{
    cell::RefCell,
    collections::BTreeMap,
    fs,
    path::PathBuf,
    rc::Rc,
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::Value;
use velr::{
    CellRef, PropertyValue, PropertyValueRef, QueryOptions, QueryParams, QueryValue,
    VectorEmbeddingInput, VectorEmbeddingPurpose, VectorEntityKind, Velr,
};

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

fn missing_options_symbol(e: &velr::Error) -> bool {
    e.message.contains("does not expose") && e.message.contains("_with_options")
}

fn vector_feature_unavailable(e: &velr::Error) -> bool {
    e.message.contains("requires the vector-usearch feature")
}

fn fulltext_feature_unavailable(e: &velr::Error) -> bool {
    e.message.contains("requires the fulltext-tantivy feature")
}

struct TempDbPath {
    path: PathBuf,
}

impl TempDbPath {
    fn new(name: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        Self {
            path: std::env::temp_dir().join(format!(
                "velr-rust-driver-{name}-{}-{nonce}.sqlite",
                std::process::id()
            )),
        }
    }
}

impl Drop for TempDbPath {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
        let _ = fs::remove_file(self.path.with_extension("sqlite-shm"));
        let _ = fs::remove_file(self.path.with_extension("sqlite-wal"));
        let _ = fs::remove_dir_all(PathBuf::from(format!(
            "{}.velr-vector",
            self.path.display()
        )));
        let _ = fs::remove_dir_all(PathBuf::from(format!("{}.velr-fts", self.path.display())));
    }
}

fn vector_text(input: &VectorEmbeddingInput) -> String {
    input
        .fields
        .iter()
        .filter_map(|field| match &field.value {
            PropertyValue::String(value) => Some(value.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[derive(Debug, Clone, PartialEq)]
struct VectorInputSnapshot {
    index_name: String,
    dimensions: usize,
    purpose: VectorEmbeddingPurpose,
    entity_kind: Option<VectorEntityKind>,
    entity_id: Option<i64>,
    fields: Vec<(Option<String>, String)>,
    text: String,
}

fn property_ref_summary(value: PropertyValueRef<'_>) -> String {
    match value {
        PropertyValueRef::Null => "null".to_string(),
        PropertyValueRef::Bool(value) => format!("bool:{value}"),
        PropertyValueRef::Integer(value) => format!("integer:{value}"),
        PropertyValueRef::Float(value) => format!("float:{value}"),
        PropertyValueRef::String(value) => format!("string:{value}"),
        PropertyValueRef::Date(value) => format!("date:{value}"),
        PropertyValueRef::LocalTime(value) => format!("local_time:{value}"),
        PropertyValueRef::ZonedTime(value) => format!("zoned_time:{value}"),
        PropertyValueRef::LocalDateTime(value) => format!("local_datetime:{value}"),
        PropertyValueRef::ZonedDateTime(value) => format!("zoned_datetime:{value}"),
        PropertyValueRef::Duration(value) => format!("duration:{value}"),
        PropertyValueRef::List(value) => {
            let values = value
                .iter()
                .map(property_ref_summary)
                .collect::<Vec<_>>()
                .join(",");
            format!("list:[{values}]")
        }
        PropertyValueRef::Vector(value) => format!("vector:{value:?}"),
        PropertyValueRef::Point(value) => format!("point:{value:?}"),
        PropertyValueRef::Geometry(value) => format!("geometry:{value:?}"),
        PropertyValueRef::Geography(value) => format!("geography:{value:?}"),
        PropertyValueRef::Bytes(value) => format!("bytes:{} bytes", value.len()),
    }
}

fn snapshot_vector_input(input: &VectorEmbeddingInput) -> VectorInputSnapshot {
    VectorInputSnapshot {
        index_name: input.index_name.clone(),
        dimensions: input.dimensions,
        purpose: input.purpose,
        entity_kind: input.entity_kind,
        entity_id: input.entity_id,
        fields: input
            .fields
            .iter()
            .map(|field| {
                (
                    field.name.clone(),
                    property_ref_summary(field.value.as_ref()),
                )
            })
            .collect(),
        text: vector_text(input),
    }
}

fn toy_vector(text: &str) -> Vec<f32> {
    let lower = text.to_ascii_lowercase();
    if lower.contains("alpha") {
        vec![1.0, 0.0, 0.0]
    } else if lower.contains("beta") {
        vec![0.0, 1.0, 0.0]
    } else {
        vec![0.0, 0.0, 1.0]
    }
}

fn title_from_node_cell(cell: &CellRef<'_>) -> String {
    let node = match cell {
        CellRef::Json(bytes) | CellRef::Text(bytes) => {
            serde_json::from_slice::<Value>(bytes).expect("node cell should be JSON")
        }
        other => panic!("expected node JSON cell, got {other:?}"),
    };
    node.get("properties")
        .and_then(|properties| properties.get("title"))
        .and_then(Value::as_str)
        .expect("node should contain properties.title")
        .to_string()
}

#[test]
fn query_options_cap_rows_non_tx() -> velr::Result<()> {
    let db = Velr::open(None)?;
    let mut t = match db.exec_one_with_options(
        "UNWIND [1,2,3,4,5,6] AS x RETURN x ORDER BY x LIMIT 10",
        QueryOptions::max_result_rows(5),
    ) {
        Ok(t) => t,
        Err(e) if missing_options_symbol(&e) => return Ok(()),
        Err(e) => return Err(e),
    };

    assert_eq!(t.column_names(), &["x".to_string()]);
    let rows = t.collect(|r| Ok(r.iter().map(own_cell).collect::<Vec<_>>()))?;
    assert_eq!(
        rows,
        vec![
            vec![Owned::Int(1)],
            vec![Owned::Int(2)],
            vec![Owned::Int(3)],
            vec![Owned::Int(4)],
            vec![Owned::Int(5)]
        ]
    );

    let mut t = db.exec_one_with_options(
        "UNWIND [1,2,3,4,5,6] AS x RETURN x ORDER BY x LIMIT 3",
        QueryOptions::max_result_rows(5),
    )?;

    assert_eq!(t.column_names(), &["x".to_string()]);
    let rows = t.collect(|r| Ok(r.iter().map(own_cell).collect::<Vec<_>>()))?;
    assert_eq!(
        rows,
        vec![
            vec![Owned::Int(1)],
            vec![Owned::Int(2)],
            vec![Owned::Int(3)]
        ]
    );
    Ok(())
}

#[test]
fn query_options_cap_rows_per_stream_table_non_tx() -> velr::Result<()> {
    let db = Velr::open(None)?;
    let mut st = match db.exec_with_options(
        "UNWIND [1,2,3] AS x RETURN x ORDER BY x;
         UNWIND [10,20,30] AS y RETURN y ORDER BY y",
        QueryOptions::max_result_rows(1),
    ) {
        Ok(st) => st,
        Err(e) if missing_options_symbol(&e) => return Ok(()),
        Err(e) => return Err(e),
    };

    let mut first = st.next_table()?.expect("first result table");
    assert_eq!(first.column_names(), &["x".to_string()]);
    let rows = first.collect(|r| Ok(r.iter().map(own_cell).collect::<Vec<_>>()))?;
    assert_eq!(rows, vec![vec![Owned::Int(1)]]);

    let mut second = st.next_table()?.expect("second result table");
    assert_eq!(second.column_names(), &["y".to_string()]);
    let rows = second.collect(|r| Ok(r.iter().map(own_cell).collect::<Vec<_>>()))?;
    assert_eq!(rows, vec![vec![Owned::Int(10)]]);

    assert!(st.next_table()?.is_none());
    Ok(())
}

#[test]
fn query_options_zero_preserves_columns_non_tx() -> velr::Result<()> {
    let db = Velr::open(None)?;
    let mut t = match db.exec_one_with_options(
        "RETURN 1 AS one, 2 AS two",
        QueryOptions::max_result_rows(0),
    ) {
        Ok(t) => t,
        Err(e) if missing_options_symbol(&e) => return Ok(()),
        Err(e) => return Err(e),
    };

    assert_eq!(t.column_names(), &["one".to_string(), "two".to_string()]);
    let rows = t.collect(|r| Ok(r.iter().map(own_cell).collect::<Vec<_>>()))?;
    assert!(rows.is_empty());
    Ok(())
}

#[test]
fn query_options_cap_rows_tx() -> velr::Result<()> {
    let db = Velr::open(None)?;
    let tx = db.begin_tx()?;
    let mut t = match tx.exec_one_with_options(
        "UNWIND [10,20,30] AS x RETURN x ORDER BY x",
        QueryOptions::max_result_rows(1),
    ) {
        Ok(t) => t,
        Err(e) if missing_options_symbol(&e) => return Ok(()),
        Err(e) => return Err(e),
    };

    assert_eq!(t.column_names(), &["x".to_string()]);
    let rows = t.collect(|r| Ok(r.iter().map(own_cell).collect::<Vec<_>>()))?;
    assert_eq!(rows, vec![vec![Owned::Int(10)]]);
    Ok(())
}

#[test]
fn query_params_and_row_cap_non_tx() -> velr::Result<()> {
    let db = Velr::open(None)?;
    let options = QueryOptions::max_result_rows(2).with_param("min", 2_i64)?;
    let mut t = db.exec_one_with_options(
        "UNWIND [1,2,3,4] AS x WITH x WHERE x >= $min RETURN x ORDER BY x",
        options,
    )?;

    assert_eq!(t.column_names(), &["x".to_string()]);
    let rows = t.collect(|r| Ok(r.iter().map(own_cell).collect::<Vec<_>>()))?;
    assert_eq!(rows, vec![vec![Owned::Int(2)], vec![Owned::Int(3)]]);
    Ok(())
}

#[test]
fn query_params_shortcut_preserves_string_values_non_tx() -> velr::Result<()> {
    let db = Velr::open(None)?;
    let params = QueryParams::new()
        .with("name", "Alice")?
        .with("literal", "MATCH (n) RETURN n")?;
    let mut t = db.exec_one_with_params("RETURN $name AS name, $literal AS literal", params)?;

    assert_eq!(
        t.column_names(),
        &["name".to_string(), "literal".to_string()]
    );
    let rows = t.collect(|r| Ok(r.iter().map(own_cell).collect::<Vec<_>>()))?;
    assert_eq!(
        rows,
        vec![vec![
            Owned::Text(b"Alice".to_vec()),
            Owned::Text(b"MATCH (n) RETURN n".to_vec())
        ]]
    );
    Ok(())
}

#[test]
fn query_params_shortcuts_work_in_tx() -> velr::Result<()> {
    let db = Velr::open(None)?;
    let tx = db.begin_tx()?;

    let mut props = BTreeMap::new();
    props.insert("name".to_string(), QueryValue::String("Alice".to_string()));
    props.insert("score".to_string(), QueryValue::Integer(7));
    tx.run_with_params(
        "CREATE (:Person $props)",
        QueryParams::new().with("props", QueryValue::Map(props))?,
    )?;

    let params = QueryParams::new().with("name", "Alice")?;
    let mut t = tx.exec_one_with_params(
        "MATCH (p:Person {name: $name}) RETURN p.score AS score",
        params,
    )?;
    assert_eq!(t.column_names(), &["score".to_string()]);
    let rows = t.collect(|r| Ok(r.iter().map(own_cell).collect::<Vec<_>>()))?;
    assert_eq!(rows, vec![vec![Owned::Int(7)]]);
    Ok(())
}

#[test]
fn query_params_macro_supports_identifier_and_literal_keys() -> velr::Result<()> {
    let db = Velr::open(None)?;

    let params = velr::params! {
        name: "Alice",
        age: 42_i64,
        "1" => 7_i64,
        "literal" => "MATCH (n) RETURN n",
    }?;
    let mut t = db.exec_one_with_params(
        "RETURN $name AS name, $age AS age, $1 AS one, $literal AS literal",
        params,
    )?;
    let rows = t.collect(|r| Ok(r.iter().map(own_cell).collect::<Vec<_>>()))?;
    assert_eq!(
        rows,
        vec![vec![
            Owned::Text(b"Alice".to_vec()),
            Owned::Int(42),
            Owned::Int(7),
            Owned::Text(b"MATCH (n) RETURN n".to_vec())
        ]]
    );
    Ok(())
}

#[test]
fn fulltext_public_api_indexes_and_queries_text() -> velr::Result<()> {
    let temp = TempDbPath::new("fulltext");
    let db = Velr::open(Some(
        temp.path
            .to_str()
            .expect("temp database path should be valid UTF-8"),
    ))?;

    db.run(
        "
        CREATE
          (:Paper {title: 'Vector Search', abstract: 'graph retrieval with embeddings'}),
          (:Paper {title: 'Planner Notes', abstract: 'query planning internals'})
        ",
    )?;
    match db.run(
        "
        CREATE FULLTEXT INDEX paperText IF NOT EXISTS
        FOR (n:Paper) ON EACH [n.title, n.abstract]
        ",
    ) {
        Ok(()) => {}
        Err(e) if fulltext_feature_unavailable(&e) => return Ok(()),
        Err(e) => return Err(e),
    }

    let mut table = match db.exec_one(
        "
        CALL db.index.fulltext.queryNodes('paperText', 'title:vector')
        YIELD node, score
        RETURN node, score
        ",
    ) {
        Ok(table) => table,
        Err(e) if fulltext_feature_unavailable(&e) => return Ok(()),
        Err(e) => return Err(e),
    };
    let rows = table.collect(|row| {
        Ok((
            title_from_node_cell(&row[0]),
            match row[1] {
                CellRef::Float(score) => score,
                ref other => panic!("expected fulltext score float, got {other:?}"),
            },
        ))
    })?;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "Vector Search");
    assert!(rows[0].1.is_finite(), "score should be finite");
    Ok(())
}

#[test]
fn vector_embedder_public_api_indexes_and_queries_text() -> velr::Result<()> {
    let temp = TempDbPath::new("vector");
    let db = Velr::open(Some(
        temp.path
            .to_str()
            .expect("temp database path should be valid UTF-8"),
    ))?;

    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen_for_callback = Rc::clone(&seen);
    db.register_vector_embedder("toy", move |inputs| {
        seen_for_callback
            .borrow_mut()
            .extend(inputs.iter().map(snapshot_vector_input));
        Ok(inputs
            .iter()
            .map(|input| toy_vector(&vector_text(input)))
            .collect())
    })?;

    db.run(
        "
        CREATE
          (:Paper {
            title: 'Alpha Paper',
            abstract: 'alpha graph',
            rank: 7,
            active: true,
            published: date('2024-05-01'),
            tags: ['graph', 'alpha']
          }),
          (:Paper {
            title: 'Beta Paper',
            abstract: 'beta graph',
            rank: 8,
            active: false,
            published: date('2024-05-02'),
            tags: ['graph', 'beta']
          })
        ",
    )?;
    match db.run(
        "
        CREATE VECTOR INDEX paperEmbedding IF NOT EXISTS
        FOR (n:Paper)
        ON EACH [n.title, n.abstract, n.rank, n.active, n.published, n.tags]
        OPTIONS {
          indexConfig: {
            dimensions: 3,
            metric: 'cosine',
            embedder: 'toy'
          }
        }
        ",
    ) {
        Ok(()) => {}
        Err(e) if vector_feature_unavailable(&e) => return Ok(()),
        Err(e) => return Err(e),
    }

    let mut table = match db.exec_one(
        "
        CALL db.index.vector.queryNodes('paperEmbedding', 1, 'alpha query')
        YIELD node, score
        RETURN node, score
        ",
    ) {
        Ok(table) => table,
        Err(e) if vector_feature_unavailable(&e) => return Ok(()),
        Err(e) => return Err(e),
    };
    let rows = table.collect(|row| Ok((own_cell(&row[0]), own_cell(&row[1]))))?;
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0].1, Owned::F64(score) if score.is_finite()));

    let seen = seen.borrow();
    assert!(
        seen.iter().any(|input| {
            input.index_name == "paperEmbedding"
                && input.dimensions == 3
                && input.purpose == VectorEmbeddingPurpose::IndexEntity
                && input.entity_kind == Some(VectorEntityKind::Node)
                && input.fields
                    == vec![
                        (Some("title".to_string()), "string:Alpha Paper".to_string()),
                        (
                            Some("abstract".to_string()),
                            "string:alpha graph".to_string(),
                        ),
                        (Some("rank".to_string()), "integer:7".to_string()),
                        (Some("active".to_string()), "bool:true".to_string()),
                        (Some("published".to_string()), "date:2024-05-01".to_string()),
                        (
                            Some("tags".to_string()),
                            "list:[string:graph,string:alpha]".to_string(),
                        ),
                    ]
                && input.text == "Alpha Paper\nalpha graph"
                && input.entity_id.is_some()
        }),
        "expected indexed Alpha input, got {seen:?}"
    );
    assert!(
        seen.iter().any(|input| {
            input.index_name == "paperEmbedding"
                && input.dimensions == 3
                && input.purpose == VectorEmbeddingPurpose::Query
                && input.entity_kind.is_none()
                && input.entity_id.is_none()
                && input.fields == vec![(None, "string:alpha query".to_string())]
                && input.text == "alpha query"
        }),
        "expected query input, got {seen:?}"
    );
    Ok(())
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
fn text_values_that_begin_with_brackets_stay_text() -> velr::Result<()> {
    let db = Velr::open(None)?;

    db.run(
        r#"
        CREATE (:Sample {
          plain: 'They are also historically associated with and used by search engines.',
          cited: '[3][4] They are also historically associated with and used by search engines.'
        });
        "#,
    )?;

    let mut table = db.exec_one(
        "MATCH (s:Sample)
         RETURN s.plain AS plain, s.cited AS cited",
    )?;

    let mut row_count = 0usize;
    table.for_each_row(|row| {
        row_count += 1;
        match row[0] {
            CellRef::Text(bytes) => assert_eq!(
                bytes,
                b"They are also historically associated with and used by search engines."
            ),
            other => panic!("expected plain text cell, got {other:?}"),
        }
        match row[1] {
            CellRef::Text(bytes) => assert_eq!(
                bytes,
                b"[3][4] They are also historically associated with and used by search engines."
            ),
            other => panic!("expected cited text cell, got {other:?}"),
        }
        Ok(())
    })?;

    assert_eq!(row_count, 1);

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

    #[test]
    fn arrow_ipc_bind_roundtrip() -> velr::Result<()> {
        let db = Velr::open(None)?;
        let mut source = db.exec_one("UNWIND [1,2,3] AS id RETURN id AS id ORDER BY id")?;
        let bytes = source.to_arrow_ipc_file()?;
        drop(source);

        db.bind_arrow_ipc("_ids_ipc", &bytes)?;

        let mut t = db.exec_one(
            "UNWIND BIND('_ids_ipc') AS row
             RETURN row.id AS id
             ORDER BY id",
        )?;
        let mut ids = Vec::new();
        t.for_each_row(|row| {
            match row[0] {
                CellRef::Integer(value) => ids.push(value),
                other => panic!("expected integer id, got {other:?}"),
            }
            Ok(())
        })?;

        assert_eq!(ids, vec![1, 2, 3]);
        Ok(())
    }

    #[test]
    fn arrow_ipc_bind_tx_roundtrip() -> velr::Result<()> {
        let db = Velr::open(None)?;
        let mut source = db.exec_one("UNWIND [4,5,6] AS id RETURN id AS id ORDER BY id")?;
        let bytes = source.to_arrow_ipc_file()?;
        drop(source);

        let tx = db.begin_tx()?;
        tx.bind_arrow_ipc("_ids_ipc_tx", &bytes)?;

        let mut t = tx.exec_one(
            "UNWIND BIND('_ids_ipc_tx') AS row
             RETURN row.id AS id
             ORDER BY id",
        )?;
        let mut ids = Vec::new();
        t.for_each_row(|row| {
            match row[0] {
                CellRef::Integer(value) => ids.push(value),
                other => panic!("expected integer id, got {other:?}"),
            }
            Ok(())
        })?;
        drop(t);
        tx.commit()?;

        assert_eq!(ids, vec![4, 5, 6]);
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
