# Velr 

Velr is an embedded property-graph database from Velr.ai, written in Rust, built on top of SQLite3 (persisting to a standard SQLite database file) and queried using the openCypher language.

Vector data and time-series support are actively in development and will ship after openCypher support has stabilized.

This crate provides the **Rust binding** for Velr. It links against a bundled native runtime with a C ABI, implemented in Rust.

**Questions, feedback, or commercial licensing:** tomas@velr.ai

---

## Release status

This release is **pre-beta**.

* The API and query support are still evolving.
* **Beta is expected in Q1 - 2026**.

If you hit a missing feature (see below), please reach out — it helps us prioritize.

---

## Installation

Add to `Cargo.toml`:

```toml
[dependencies]
velr = "0.1"
````

Enable Arrow IPC support (binding Arrow arrays + exporting result tables as Arrow IPC):

```toml
[dependencies]
velr = { version = "0.1", features = ["arrow-ipc"] }
```

---

## Quick start

```rust,no_run
use velr::{Velr, CellRef};

fn main() -> velr::Result<()> {
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
```

---

## Query language support

Velr supports **most of openCypher**, but some features are not yet implemented.

Notable missing features:

* Remove clause

* **Patterns in `WHERE` clauses** (pattern predicates)

---

## Streaming multiple result tables

A single `exec()` can yield multiple result tables (e.g. multiple statements):

```rust
let db = Velr::open(None)?;
let mut stream = db.exec(
    "MATCH (m:Movie {title:'The Matrix'}) RETURN m.title AS title;
     MATCH (m:Movie {title:'Inception'})  RETURN m.released AS year"
)?;

while let Some(mut table) = stream.next_table()? {
    println!("{:?}", table.column_names());
    table.for_each_row(|row| {
        println!("{row:?}");
        Ok(())
    })?;
}
```

---

## Transactions and savepoints

```rust,no_run
let db = Velr::open(None)?;

let tx = db.begin_tx()?;
tx.run("CREATE (:Temp {k:'t1'})")?;

{
    let sp = tx.savepoint_named("sp1")?;
    tx.run("CREATE (:Temp {k:'inner'})")?;
    sp.rollback()?; // rollback to savepoint, then release it
}

tx.commit()?;

```

Dropping an active transaction without `commit()` will roll it back (RAII).

---

## Arrow IPC (optional)

With `features = ["arrow-ipc"]` you can:

* Bind Arrow arrays as a logical table (`bind_arrow`, `bind_arrow_chunks`)
* Export a result table as an Arrow IPC file (`to_arrow_ipc_file()`)

```rust,no_run
#[cfg(feature = "arrow-ipc")]
fn arrow_example() -> velr::Result<()> {
    use arrow2::array::{Array, Utf8Array};

    let db = Velr::open(None)?;

    let cols = vec!["name".to_string()];
    let arrays: Vec<Box<dyn Array>> = vec![
        Utf8Array::<i64>::from(vec![Some("Alice"), Some("Bob")]).boxed(),
    ];

    db.bind_arrow("_people", cols, arrays)?;
    db.run("UNWIND BIND('_people') AS r CREATE (:Person {name:r.name})")?;

    let mut t = db.exec_one("MATCH (p:Person) RETURN p.name AS name ORDER BY name")?;
    let ipc = t.to_arrow_ipc_file()?;

    println!("IPC bytes: {}", ipc.len());
    Ok(())
}
```

---

## Supported functions

Velr currently supports these openCypher functions:

**Scalars**

* `id()`
* `type()`
* `length()`
* `nodes()`
* `relationships()`
* `coalesce()`
* `labels()`
* `properties()`
* `keys()`

**Aggregates**

* `count()`
* `sum()`
* `avg()`
* `min()`
* `max()`
* `collect()`

---

## Platform support

This crate links against a bundled native runtime.

Currently bundled targets:

* macOS universal (arm64 + x86_64)
* Linux x86_64
* Linux aarch64
* Windows x86_64


---

## License

This project is licensed under the **Velr Beta Test License v1.0**.

* Evaluation / beta use only
* No production use
* No redistribution

See [`LICENSE`](LICENSE) for the full terms.

