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

- The Rust API is still evolving.
- Velr supports openCypher and passes all positive openCypher TCK tests. Exact
  error semantics are not guaranteed to match other openCypher implementations.
- Velr 0.2.14 includes a breaking on-disk storage change; existing databases from earlier releases must be recreated by re-importing the source data.
- Starting with the `0.3.x` series, we intend to guarantee internal database compatibility within the branch.

### Schema version 8 compatibility

This release's current on-disk schema is version 8. Supported older databases
can be opened with `Velr::open` or `Velr::open_readonly` without changing the
file. Reads continue to work on those databases, but writes (`CREATE`, `MERGE`,
`SET`, `DELETE`, `DETACH DELETE`, and other mutating queries) are only available
after migrating to the current schema version. This is intentional: migration
is an explicit maintenance operation, not a side effect of opening a database.

Velr is already usable for real workflows and representative use cases, but rough edges remain and the API is not yet stable.

Fulltext search and vector search are available today through Cypher DDL and
`CALL` syntax. API details may still evolve while Velr remains alpha.

---

## Installation

Add to `Cargo.toml`:

```toml
[dependencies]
velr = "0.2"
```

Arrow IPC support is enabled by default. To build without Arrow IPC support, opt out of default features:

```toml
[dependencies]
velr = { version = "0.2", default-features = false }
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
databases. Older supported databases, such as schema version 3, 4, 5, 6, or 7
databases opened by a schema version 8 runtime, remain available for reads.
Writes and features that require the current schema fail with a normal query
error until the database is explicitly migrated.

---

## Schema migration

Velr does not migrate supported older databases automatically on open. Use the
driver migration API, or run `MIGRATE DATABASE`, from maintenance code when you
intend to update the on-disk schema. See the release-status note above for the
schema version 8 read/write compatibility behavior.

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

`SHOW CURRENT GRAPH SHAPE` is available on schema version 5 or newer databases.
Older supported databases can still be opened for reads, but must be migrated
explicitly before this command is valid. Schema version 5 introduced this
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

## Fulltext Search

Fulltext search is available through normal Cypher execution. Define indexes
with `CREATE FULLTEXT INDEX` and query them with
`CALL db.index.fulltext.queryNodes(...)`.

```rust,no_run
let db = Velr::open(Some("mygraph.db"))?;

db.run(
    "CREATE FULLTEXT INDEX paperText
     FOR (n:Paper) ON EACH [n.title, n.abstract]",
)?;

let mut rows = db.exec_one(
    "CALL db.index.fulltext.queryNodes('paperText', 'abstract:vector')
     YIELD node, score
     RETURN node, score",
)?;
```

The query string supports this fulltext grammar:

- Terms: `vector search`
- Phrases: `"vector search"`
- Field scoping by indexed property: `title:graph`, `abstract:"vector search"`
- Boolean operators and grouping: `graph AND (vector OR semantic)`
- Default `OR` between adjacent terms: `vector search`
- Required and excluded terms: `+vector -draft`
- Phrase slop: `"vector search"~2`
- Phrase prefix on the last phrase term: `"vector sea"*`
- Boosts: `title:graph^2.0`
- Match all indexed nodes: `*`

Field scoping applies to the next term or phrase only. For example,
`title:graph search` searches `graph` in `title` and `search` in the default
fulltext field.

`score` is a non-normalized relevance score. Higher scores are better within a
single query result set; scores are not guaranteed to be in `0..1` or
comparable across different queries.

Fulltext indexes are kept up to date by writes. For file-backed databases,
Velr repairs missing or corrupt fulltext index data automatically when the
database is opened.

---

## Vector Search

Velr supports two vector index shapes.

If your application already computes embeddings, store them as a vector/list
property and index that property. No embedder is needed, and queries pass a
numeric vector/list:

```rust,no_run
use velr::Velr;

fn main() -> velr::Result<()> {
    let db = Velr::open(Some("mygraph.db"))?;

    db.run(
        "CREATE (:Chunk {
           id: 'chunk-1',
           text: 'Graph databases store connected data',
           embedding: [0.12, 0.34, 0.56]
         })",
    )?;

    db.run(
        "CREATE VECTOR INDEX chunkEmbedding IF NOT EXISTS
         FOR (n:Chunk)
         ON (n.embedding)
         OPTIONS { indexConfig: { dimensions: 3, metric: 'cosine' } }",
    )?;

    let mut _rows = db.exec_one(
        "CALL db.index.vector.queryNodes('chunkEmbedding', 10, [0.10, 0.30, 0.50])
         YIELD node, score
         RETURN node, score",
    )?;

    Ok(())
}
```

For text-to-vector search, register an embedding callback and reference it from
`CREATE VECTOR INDEX`. Velr invokes the callback for index maintenance when
indexed source values change and for text queries passed to
`CALL db.index.vector.queryNodes(...)`.

`OPTIONS { indexConfig: ... }` configures Velr vector indexes. Prefer the
Neo4j-style names where they exist; shorter aliases are accepted for existing
queries and examples.

```cypher
OPTIONS {
  indexConfig: {
    `vector.dimensions`: 384,
    `vector.similarity_function`: 'cosine',
    embedder: 'text',
    cache_policy: 'required',
    `vector.hnsw.ef_search`: 128
  }
}
```

Core options:

| Option | Accepted aliases | Meaning |
|---|---|---|
| `` `vector.dimensions` `` | `dimensions` | Required vector width. Every stored vector, embedder output, and query vector must contain exactly this many values. |
| `` `vector.similarity_function` `` | `metric`, `similarity_function` | Distance function. Use `cosine` for most semantic embeddings, `l2`/`euclidean` for geometric distance, and `dot`/`inner_product`/`max_inner_product` for inner-product retrieval. Default: `cosine`. |
| `embedder` | `velr.embedder` | Name of a registered callback for `ON EACH [...]` indexes. Omit it for stored-vector indexes such as `ON (n.embedding)`. |
| `embedder_fingerprint` | `embedderFingerprint`, `velr.embedder_fingerprint` | Optional model/version metadata stored with the index definition. Velr does not use it to call the embedder. |
| `cache_policy` | `cachePolicy` | Embedder-backed indexes only. `required` stores generated embeddings in the database so index files can be rebuilt without recomputing them; `best_effort` skips cache write errors; `none` stores no generated embedding cache. Defaults: `required` with `embedder`, `none` without. Stored-vector indexes require `none` because the vector property is already the durable source. |

HNSW tuning options:

| Option | Integer count | Meaning |
|---|---:|---|
| `` `vector.hnsw.m` `` | `1..512` neighbors | Maximum graph connectivity per vector. For example, `16` means each vector keeps roughly 16 HNSW neighbor links. Higher values can improve recall on difficult datasets, with more memory, larger index files, and slower builds. |
| `` `vector.hnsw.ef_construction` `` | `1..3200` candidates | Build-time candidate pool per inserted vector. For example, `320` means the builder considers a working set of 320 candidate neighbors while placing each vector. Higher values can improve index quality and recall, usually with slower index creation. |
| `` `vector.hnsw.ef_search` `` | `1..3200` candidates | Query-time candidate pool. For example, `128` means each search keeps a working set of 128 candidate vectors while traversing the HNSW graph. Higher values can improve recall, usually with slower searches. Velr's default is `64`. |

Velr manages the vector scalar format and storage/cache engines internally. Use
the options above for application configuration.

```rust,no_run
use velr::{PropertyValue, VectorEmbeddingPurpose, Velr};

fn embed_text(_text: &str, dimensions: usize) -> Vec<f32> {
    // Call your embedding model here.
    vec![0.0; dimensions]
}

fn main() -> velr::Result<()> {
    let db = Velr::open(Some("mygraph.db"))?;

    db.register_vector_embedder("text", |inputs| {
        Ok(inputs
            .iter()
            .map(|input| {
                let text = input
                    .fields
                    .iter()
                    .filter_map(|field| match &field.value {
                        PropertyValue::String(value) => Some(value.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n");

                let prefix = match input.purpose {
                    VectorEmbeddingPurpose::IndexEntity => "passage: ",
                    VectorEmbeddingPurpose::Query => "query: ",
                };
                embed_text(&format!("{prefix}{text}"), input.dimensions)
            })
            .collect())
    })?;

    db.run(
        "CREATE VECTOR INDEX paperEmbedding IF NOT EXISTS
         FOR (n:Paper)
         ON EACH [n.title, n.abstract]
         OPTIONS { indexConfig: { dimensions: 384, metric: 'cosine', embedder: 'text' } }",
    )?;

    let mut _rows = db.exec_one(
        "CALL db.index.vector.queryNodes('paperEmbedding', 10, 'paper about greek letters')
         YIELD node, score
         RETURN node, score",
    )?;

    Ok(())
}
```

`ON EACH [n.title, n.abstract]` passes both property values to the callback in
that order. Query text is passed as one unnamed string field. Vector `score` is
metric-dependent and non-normalized; higher scores are better within a single
query result set.

For file-backed databases, Velr can repair missing or corrupt vector index data
from stored vector properties or from registered embedders, depending on how
the index was created.

---

## Query language support

Velr supports the openCypher query language and passes all positive openCypher
TCK tests. Exact error semantics, including error messages, categories, and
timing, are not guaranteed to match other openCypher implementations.

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

## Bounded result previews

Use `QueryOptions::max_result_rows` when a host needs projected column names and
a small row sample without rewriting the Cypher text:

```rust,no_run
use velr::{QueryOptions, Velr};

fn main() -> velr::Result<()> {
    let db = Velr::open_readonly("mygraph.db")?;
    let mut table = db.exec_one_with_options(
        "MATCH (n) RETURN labels(n) AS labels, n.name AS name ORDER BY name",
        QueryOptions::max_result_rows(20),
    )?;

    let columns = table.column_names().to_vec();
    let sample = table.collect(|row| {
        Ok(row.iter().map(|cell| format!("{cell:?}")).collect::<Vec<_>>())
    })?;

    println!("{columns:?}");
    println!("{sample:?}");
    Ok(())
}
```

`max_result_rows = 0` preserves the returned column metadata and opens row
cursors at EOF. The cap is enforced by Velr during result emission, not by
appending or injecting Cypher `LIMIT`, and applies independently to each result
table produced by `exec_with_options`. Existing Cypher `LIMIT` clauses still
apply, so a query with `LIMIT 3` and `max_result_rows = 5` emits at most three
rows, while `LIMIT 10` with `max_result_rows = 5` emits at most five rows. It is
not a timeout or cancellation
mechanism; hosts that need read-only validation or execution deadlines should
keep those checks separate.

## Query parameter binding

Use `params!`, `QueryParams`, or `QueryOptions::with_param` to bind openCypher parameters
out of band. Query text uses `$name`; API parameter names omit the leading `$`.
Values are passed as Cypher values, not interpolated into query text, so a Rust
`String` is always a Cypher string value.

```rust,no_run
use velr::{QueryOptions, Velr};

fn main() -> velr::Result<()> {
    let db = Velr::open(None)?;

    db.run_with_params(
        "CREATE (:Person {name: $name, age: $age})",
        velr::params! {
            name: "Alice",
            age: 42_i64,
        }?,
    )?;

    let mut table = db.exec_one_with_options(
        "MATCH (p:Person) WHERE p.age >= $min_age RETURN p.name AS name ORDER BY name",
        QueryOptions::max_result_rows(20).with_param("min_age", 18_i64)?,
    )?;

    println!("{:?}", table.column_names());
    table.for_each_row(|row| {
        println!("{row:?}");
        Ok(())
    })?;
    Ok(())
}
```

Supported parameter values are `null`, booleans, signed 64-bit integers, finite
floats, strings, lists, and maps. Lists and maps can be supplied as
`QueryValue::List` / `QueryValue::Map`, Rust `Vec`s, `BTreeMap<String, _>`,
`HashMap<String, _>`, or `serde_json::Value`.

Use string-literal keys in `params!` for parameter names that are not Rust
identifiers, such as `$1`: `velr::params! { name: "Alice", "1" => 42_i64 }?`.

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

## Arrow IPC

Arrow IPC support is enabled by default. With the default feature set you can:

* Bind Arrow arrays as a logical table (`bind_arrow`, `bind_arrow_chunks`)
* Bind Arrow IPC file / Feather v2 bytes as a logical table (`bind_arrow_ipc`)
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

## OpenCypher functions

The following openCypher functions and constructors are available:

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
