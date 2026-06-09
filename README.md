# Velr

Velr is an embedded property-graph database from Velr.ai, written in Rust, built on top of SQLite3 (persisting to a standard SQLite database file) and queried using the openCypher language.

This crate provides the **Rust binding** for Velr. It links against a bundled native runtime with a C ABI, implemented in Rust.

For the main Velr public entry point, see [velr-ai/velr](https://github.com/velr-ai/velr).  
For the Velr website, see [velr.ai](https://velr.ai/).

## Community

- **Community and questions:** [GitHub Discussions](https://github.com/velr-ai/velr/discussions)
- **Bug reports and feature requests:** [GitHub Issues](https://github.com/velr-ai/velr/issues)
- **Rust examples:** [velr-rust-examples](https://github.com/velr-ai/velr-rust-examples)

We’d love to have you join the Velr community.

---

## Release status

This release is **alpha**.

- The API and query support are still evolving.
- openCypher coverage is already substantial, but some features are still missing.
- During the `0.2.x` series, we do **not** guarantee database migration or on-disk database compatibility between releases.
- Velr 0.2.14 includes a breaking on-disk storage change; existing databases from earlier releases must be recreated by re-importing the source data.
- Starting with the `0.3.x` series, we intend to guarantee internal database compatibility within the branch.

### Schema version 5 compatibility

This release's current on-disk schema is version 5. Supported older databases
can be opened with `Velr::open` or `Velr::open_readonly` without changing the
file. Reads continue to work on those databases, but writes (`CREATE`, `MERGE`,
`SET`, `DELETE`, `DETACH DELETE`, and other mutating queries) are only available
after migrating to schema version 5. This is intentional: migration is an
explicit maintenance operation, not a side effect of opening a database.

Velr is already usable for real workflows and representative use cases, but rough edges remain and the API is not yet stable.

**Velr 1.0 is focused on strong openCypher compatibility.**  
**Vector search**, **time-series**, and **federation** are planned as post-1.0 capabilities.

---

## Installation

Add to `Cargo.toml`:

```toml
[dependencies]
velr = "0.2"
````

Enable Arrow IPC support (binding Arrow arrays + exporting result tables as Arrow IPC):

```toml
[dependencies]
velr = { version = "0.2", features = ["arrow-ipc"] }
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

## Opening an existing database read-only

Use `Velr::open_readonly(path)` for viewers, agents, and other read paths that
should not initialize or migrate the database:

```rust,no_run
use velr::Velr;

fn main() -> velr::Result<()> {
    let db = Velr::open_readonly("mygraph.db")?;
    let mut table = db.exec_one("MATCH (n) RETURN count(n) AS count")?;

    table.for_each_row(|row| {
        println!("{:?}", row[0]);
        Ok(())
    })?;

    Ok(())
}
```

`open_readonly` requires an existing file-backed database at a supported Velr
schema version. It does not create files, run schema DDL, or migrate older
databases. Older supported databases, such as schema version 3 or 4 databases
opened by a schema version 5 runtime, remain available for reads. Writes and
features that require schema version 5 fail with a normal query error until the
database is explicitly migrated.

---

## Schema migration

Velr does not migrate supported older databases automatically on open. Use the
driver migration API, or run `MIGRATE DATABASE`, from maintenance code when you
intend to update the on-disk schema. See the release-status note above for the
schema version 5 read/write compatibility behavior.

```rust,no_run
use velr::{MigrationStatus, Velr};

fn main() -> velr::Result<()> {
    let db = Velr::open(Some("mygraph.db"))?;

    if db.needs_migration()? {
        let report = db.migrate()?;
        match report.status {
            MigrationStatus::Migrated => {
                println!(
                    "migrated schema {} -> {} via {:?}",
                    report.from_version, report.to_version, report.steps
                );
            }
            MigrationStatus::AlreadyCurrent => {}
        }
    }

    Ok(())
}
```

The equivalent Cypher command is useful for scripts and tools that already work
through query execution:

```rust,no_run
let db = Velr::open(Some("mygraph.db"))?;
let mut report = db.exec_one("MIGRATE DATABASE")?;
println!("{:?}", report.column_names());
```

---

## Introspection

Use `SHOW CURRENT GRAPH SHAPE` to inspect the observed schema of the graph. It
reports the shape present in stored data: node labels, relationship types,
properties, observed value types, and counts. It is an observed shape surface,
not a declared GQL graph type.

`SHOW CURRENT GRAPH SHAPE` is available on schema version 5 databases. Older
supported databases can still be opened for reads, but must be migrated
explicitly before this command is valid. Schema version 5 maintains this
inventory through the write planner instead of persistent graph-shape triggers.

The default projection returns `element_kind`, `element_name`, `property_name`,
`observed_type`, `owner_count`, `present_count`, and `missing_count`.
`YIELD *` exposes the full row shape, including `surface`, `source_label`,
`target_label`, `required`, `storage_class`, and `tag`.

```rust,no_run
let db = Velr::open(Some("mygraph.db"))?;

let mut shape = db.exec_one(
    "SHOW CURRENT GRAPH SHAPE
     YIELD element_kind, element_name, property_name, observed_type, owner_count
     WHERE element_kind = 'node_property'
     RETURN element_name, property_name, observed_type, owner_count",
)?;

shape.for_each_row(|row| {
    println!("{row:?}");
    Ok(())
})?;
```

Use `YIELD` to compose the command with `WHERE` and `RETURN`. Plain
`SHOW CURRENT GRAPH SHAPE` returns the default projection; `YIELD *` exposes the
full current row shape.

---

## Query language support

Velr supports **most of openCypher**, but some features are not yet implemented.

Notable current limitations:

* Driver-level query parameters (for example `$name`)
* The query planner does not yet use indexes in all cases where expected.

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

Velr supports transactions together with two kinds of savepoints:

* **Scoped savepoints** via `savepoint()`, which return a guard
* **Named savepoints** via `savepoint_named(name)`, which remain active in the transaction until released or the transaction ends

Calling `rollback_to(name)` rolls back to the named savepoint, discards any newer named savepoints, and keeps the target savepoint active.

### Scoped savepoint

```rust,no_run
let db = Velr::open(None)?;

let tx = db.begin_tx()?;
tx.run("CREATE (:Temp {k:'outer'})")?;

{
    let sp = tx.savepoint()?;
    tx.run("CREATE (:Temp {k:'inner'})")?;
    sp.rollback()?; // rollback to the scoped savepoint
}

tx.commit()?;
````

### Named savepoints

```rust,no_run
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
```

`release_savepoint(name)` currently releases the most recent active named savepoint.

Dropping an active transaction without `commit()` will roll it back automatically.

---
## Explain plans

Velr can produce an explain trace for a query, which is useful when you want to inspect how a openCypher query is planned and translated internally.

Use `Velr::explain` to build a trace without executing the query:

```rust,no_run
use velr::Velr;

fn main() -> velr::Result<()> {
    let db = Velr::open(None)?;

    let trace = db.explain("MATCH (n) RETURN n")?;

    println!("plans: {}", trace.plan_count()?);
    println!("{}", trace.to_compact_string()?);

    Ok(())
}
```

The returned `ExplainTrace` can be inspected programmatically or rendered as a compact string for logging, debugging, tests, or documentation.

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

Velr currently supports these openCypher functions and constructors:

**Graph and path**

* `id()`
* `type()`
* `labels()`
* `keys()`
* `properties()`
* `length()`
* `nodes()`
* `relationships()`

**Lists and predicates**

* `size()`
* `head()`
* `last()`
* `tail()`
* `reverse()`
* `range()`
* `all()`
* `any()`
* `none()`
* `single()`

**Strings and conversion**

* `coalesce()`
* `toInteger()`
* `toString()`
* `toLower()`
* `trim()`
* `substring()`
* `split()`

**Numeric**

* `abs()`
* `ceil()`
* `rand()`
* `sign()`
* `sqrt()`

**Temporal**

* `date()`
* `time()`
* `localtime()`
* `datetime()`
* `localdatetime()`
* `duration()`
* `datetime.fromepoch()`
* `datetime.fromepochmillis()`
* `date.realtime()`, `date.transaction()`, `date.statement()`
* `time.realtime()`, `time.transaction()`, `time.statement()`
* `localtime.realtime()`, `localtime.transaction()`, `localtime.statement()`
* `datetime.realtime()`, `datetime.transaction()`, `datetime.statement()`
* `localdatetime.realtime()`, `localdatetime.transaction()`, `localdatetime.statement()`

**Aggregates**

* `count()`
* `sum()`
* `avg()`
* `min()`
* `max()`
* `collect()`
* `percentileDisc()`
* `percentileCont()`

---

## Platform support

This crate links against a bundled native runtime. Cargo selects one platform-specific
`velr-runtime-*` crate for the current build target, so user installation stays:

```toml
[dependencies]
velr = "0.2"
```

Currently bundled targets:

* macOS universal (arm64 + x86_64)
* Linux x86_64
* Linux aarch64
* Windows x86_64


---

### Licensing 

* The **Rust binding source code** in this package is licensed under **MIT**.
* The **bundled native runtime binaries** may be **used and freely redistributed in unmodified form** under the terms of **`LICENSE.runtime`**.

See [`LICENSE`](LICENSE) and [`LICENSE.runtime`](LICENSE.runtime) for the full license texts.
