# Velr

Velr is an embedded property-graph database with openCypher query support.

This crate provides the **Rust binding** for Velr and bundles the native runtime
for supported platforms.

Create fulltext and vector indexes with Cypher DDL and query them with `CALL`.

For the main Velr public entry point, see [velr-ai/velr](https://github.com/velr-ai/velr).  
For the Velr website, see [velr.ai](https://velr.ai/).

The [Rust API reference](https://docs.rs/velr/latest/velr/) documents every public
type and method, including parameters, errors, feature requirements, and lifetimes.

## Community

- **Community and questions:** [GitHub Discussions](https://github.com/velr-ai/velr/discussions)
- **Bug reports and feature requests:** [GitHub Issues](https://github.com/velr-ai/velr/issues)
- **Rust examples:** [velr-rust-examples](https://github.com/velr-ai/velr-rust-examples)

---

## Database compatibility

Velr 0.2.14 changed the database format. Recreate databases from earlier
releases by re-importing the source data.

New databases use schema version 11. Databases at schema versions 3 through 9
require an [explicit migration](#schema-migration) before graph queries can run.

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

Velr bundles the native runtime for your platform. On macOS, Cargo automatically
selects the Apple Silicon (`aarch64`) or Intel (`x86_64`) runtime matching your
build target.

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

## Choosing an API

| Application task | API |
|---|---|
| Open a database | `Velr::open`, `Velr::open_readonly` |
| Create a database with an inline relationship property | `Velr::create`, `CreateOptions`, `inline_edge_properties` |
| Execute a write and discard results | `run`, `run_with_params`, `run_with_options` |
| Read exactly one result table | `exec_one`, `exec_one_with_params`, `exec_one_with_options` |
| Read multiple result tables | `exec`, `exec_with_params`, `exec_with_options`, then `next_table` |
| Read rows | `TableResult::for_each_row`, `collect`, or `rows` and `RowIter::next` |
| Bind parameters or limit emitted rows | `params!`, `QueryParams`, `QueryValue`, `QueryOptions` |
| Group work in a transaction | `begin_tx`, then the query methods on `VelrTx`, followed by `commit` or `rollback` |
| Inspect query behavior | `explain`, `explain_with_params`, `explain_analyze`, `ExplainTrace` |
| Check and migrate an existing database | `schema_version`, `current_schema_version`, `needs_migration`, `migrate` |
| Import or export Arrow data | `bind_arrow`, `bind_arrow_chunks`, `bind_arrow_ipc`, `TableResult::to_arrow_ipc_file` |
| Provide embeddings for vector search | `register_vector_embedder`, `VectorEmbeddingInput`, `VectorEmbeddingBatchResult` |
| Compact a database or inspect temporary storage | `vacuum`, `temp_storage_bytes` |

`exec_one` returns an error when a query produces zero or multiple result tables.
Use `run` when no result is needed and `exec` when processing multiple statements.
The transaction handle provides matching execution and Arrow binding methods.

## Results, lifetimes, and threads

`CellRef` represents `Null`, `Bool`, `Integer`, `Float`, borrowed `Text` bytes,
or borrowed `Json` bytes for structured values such as lists, maps, and graph
entities. Text and JSON are valid only inside the row callback; copy or parse
them there to retain values. `collect` retains every mapped value in memory;
use `for_each_row` for results that should be processed incrementally.

```rust,no_run
use velr::{CellRef, Velr};

fn main() -> velr::Result<()> {
    let db = Velr::open(None)?;
    let mut table = db.exec_one("RETURN 'Alice' AS name")?;
    let names: Vec<String> = table.collect(|row| match row[0] {
        CellRef::Text(bytes) => Ok(String::from_utf8_lossy(bytes).into_owned()),
        _ => Ok(String::new()),
    })?;
    println!("{names:?}");
    Ok(())
}
```

Process each result table before requesting the next one. Finish tables and row
cursors while their connection and any transaction are active. Streams, tables,
cursors, transactions, savepoints, and explain traces release their resources
when dropped. Dropping an active transaction rolls back its writes.

`Velr` is `Send` but not `Sync`: an idle connection can be moved to another
thread, and separate threads can use separate connections. Result handles and
transaction handles are neither `Send` nor `Sync`; keep them on their creating
thread. An `Arc<Velr>` does not permit concurrent use of one connection.

Most operations return `velr::Result<T>`. `Error` contains a numeric `code` and a
human-readable `message`. The numeric code describes the API failure, such as
an invalid argument or execution failure. Cypher diagnostics in `message`
identify the error category and its compile-time or runtime phase, with an
explanation of the problem and guidance for correcting it. Source-aware
diagnostics show the query excerpt, location, and affected expression. Where
several expressions could explain an error, labels mark possible locations.

| Cypher error category | Meaning |
|---|---|
| `SyntaxError` | Invalid syntax or a statically invalid query. |
| `TypeError` | A value has an incompatible type. |
| `ArgumentError` | An argument is outside the operation's supported domain. |
| `ParameterMissing` | A query parameter was not supplied. |
| `ProcedureNotFound` | A `CALL` names a procedure unavailable on this connection. |
| `ProcedureError` | In the openCypher profile, a `CALL` names an unavailable procedure. |
| `ArithmeticError` | Arithmetic is undefined or overflows. |
| `EntityNotFound` | A query accesses a deleted graph entity. |
| `ConstraintVerificationFailed` | A write violates a graph constraint. |
| `SemanticError` | A query violates a graph semantic rule. |

Parameter construction returns `QueryParamError`, which converts to `Error`
through `?`. A query that references an unbound parameter returns `Error` with
a `ParameterMissing` diagnostic. Query execution can fail when starting a query,
advancing result tables, or fetching rows, so
propagate errors at each stage. Returning an error from a row callback stops
iteration and returns that error to the caller.

A runtime error can be reported before a result table is returned. The phase
describes the Cypher error independently of which driver call reports it.

---

## Creating a database with an inline relationship property

Use `Velr::create` to choose zero or one case-sensitive relationship property
name for a new database. The default selects no property. The choice applies
across all relationship types and persists when you reopen the database. Pass
`None` to create an in-memory database. Creation fails if the file already
exists. Use `Velr::open` or `Velr::open_readonly` to reopen an existing file.

Eligible values include integers, floating-point numbers, and strings up to 128
UTF-8 bytes. Values that exceed the inline size limit remain available through
the same property name. Use ordinary Cypher to create, update, and read the
property.

```rust,no_run
use velr::{CreateOptions, Velr};

fn main() -> velr::Result<()> {
    let options = CreateOptions::new().with_inline_edge_properties(["timestamp"]);
    let db = Velr::create(Some("events.db"), options)?;
    db.run("CREATE (:Event)-[:FOLLOWED_BY {timestamp: 1}]->(:Event)")?;
    db.run("MATCH ()-[r:FOLLOWED_BY]->() SET r.timestamp = 2")?;
    drop(db);

    let db = Velr::open_readonly("events.db")?;
    println!("{:?}", db.inline_edge_properties()?);
    let mut table = db.exec_one("MATCH ()-[r:FOLLOWED_BY]->() RETURN r.timestamp")?;
    table.for_each_row(|row| {
        println!("{:?}", row[0]);
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

`open_readonly` requires an existing Velr database at a supported version and
does not modify the file. After migrating an older database on a writable
connection, reopen any read-only connections before querying it.

---

## Schema migration

Open the database on a writable connection and call `migrate()` or run
`MIGRATE DATABASE` outside an active transaction.

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

Run the equivalent Cypher command from scripts and query-based tools:

```rust,no_run
let db = Velr::open(Some("mygraph.db"))?;
let mut report = db.exec_one("MIGRATE DATABASE")?;
println!("{:?}", report.column_names());
```

## Maintenance

After large deletes or prune operations on a file-backed database, call
`db.vacuum()` from maintenance code when you want to compact the database file:

```rust,no_run
use velr::Velr;

fn main() -> velr::Result<()> {
    let db = Velr::open(Some("mygraph.db"))?;
    db.run("MATCH (n:Expired) DETACH DELETE n")?;
    db.vacuum()?;
    Ok(())
}
```

`vacuum()` must run on a writable current-schema connection outside an open
transaction. The equivalent command for scripts is `VACUUM DATABASE`.

---

## Introspection

Use `SHOW CURRENT GRAPH SHAPE` to inspect the observed schema of the graph. It
reports the shape present in stored data: node labels, relationship types,
properties, observed value types, and counts. It is an observed shape surface,
not a declared GQL graph type.

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

Create fulltext indexes with `CREATE FULLTEXT INDEX` and query them with
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

`score` is an unnormalized, query-specific relevance score. Higher scores rank
results higher. Compare scores within the same query's result set.

Fulltext queries return up to 1,000 matches by default. Pass a third
argument such as `{limit: 5000}` to change the cap.

Fulltext queries see changes made earlier in the current transaction.
Opening a file-backed database for writing applies pending index updates
and repairs missing or corrupt index data. Term, phrase, phrase-prefix,
and Boolean searches can see small sets of indexed changes without
scanning the graph. A search can still scan the indexed graph after
large writes, when its snapshot predates the shared index, or with tied
scores. Broad prefix expansions and some combinations of Boolean scores
can also require a scan.
Repeated searches can reuse the same view of the indexed documents
while the transaction makes no further changes.

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

For exact query-time scoring without a native vector index, use the
Neo4j-compatible scalar function `vector.similarity.cosine(left, right)` inside
`WITH`, `WHERE`, `RETURN`, and `ORDER BY`.

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

For file-backed databases, Velr repairs missing or corrupt vector index data
using stored vector properties or registered embedders, according to the index
configuration.

---

## Query language support

Velr supports the openCypher clauses and expressions described here, along with
Cypher extensions for subqueries, fulltext search, and vector search.

Use `MATCH`, `OPTIONAL MATCH`, `WHERE`, `WITH`, `UNWIND`, aggregation,
ordering, pagination, `CREATE`, `MERGE`, `SET`, `REMOVE`, and deletion. Variable
length paths, `shortestPath`, `allShortestPaths`, list and pattern comprehensions,
`reduce`, `UNION`, and `UNION ALL` are also supported.

`UNWIND` emits one row per list element, including null elements. A non-null
scalar or map yields one row, so `UNWIND 5 AS x RETURN x` returns `5`. A null
source or empty list yields zero rows.

Use a Boolean condition in `WHERE`. Evaluating an integer property as the
condition raises a runtime `TypeError`. To filter on a number, write a comparison
such as `WHERE p.age > 0`.

List comprehensions transform or filter a list; `reduce` folds its elements into
an accumulator, including lists stored as properties:

```cypher
RETURN [x IN range(1, 5) WHERE x % 2 = 1 | x * x] AS oddSquares,
       reduce(total = 0, x IN [1, 2, 3] | total + x) AS sum
```

`reduce` processes elements in list order. An empty list returns the initial
accumulator; a `null` source returns `null`.

`range(start, end[, step])` accepts integer and finite floating-point arguments.
Floating-point arguments are truncated toward zero; the resulting step must be
nonzero. Strings, booleans, and `null` arguments raise an argument error.
List element access (`list[index]`) requires an integer index, and map element
access (`map[key]`) requires a string key. A `null` index or key returns `null`.
`trim(value)` accepts a string or `null`; other argument types raise a type error.
Convert other values explicitly with `toString()` before trimming them.

`sqrt()`, `floor()`, and `round()` accept numeric values or `null`; incompatible
argument types raise a runtime `TypeError`. `round(value, precision)` accepts
integer and finite floating-point precision values, truncating FLOAT precision
toward zero. The resulting precision must be non-negative. Negative precision,
non-finite precision, and a NaN value passed to `round()` raise `ArgumentError`.
`sqrt()` and `floor()` propagate NaN values. Integer overflow and integer
division or modulo by zero raise `ArithmeticError`.

`percentileDisc()` and `percentileCont()` require finite numeric inputs and a
percentile between `0.0` and `1.0`, inclusive, constant within each aggregation
group. Null inputs are skipped. Incompatible types raise `TypeError`; invalid
percentile values raise `ArgumentError`. Diagnostics identify the relevant
function argument and provide source guidance.

In a `CREATE` or `MERGE` relationship pattern, reference a node already in scope
as `(n)`; apply labels and properties with `SET`. Repeating the same relationship
variable in one `MATCH` makes that pattern produce no matches. Use separate
`MATCH` clauses to reuse the same relationship.

Use a pattern comprehension to return related values as a list:

```cypher
MATCH (p:Person)
RETURN p.name, [(p)-[:KNOWS]->(friend) | friend.name] AS friends
```

Use `EXISTS { ... }` and `COUNT { ... }` with read-only subqueries:

```cypher
RETURN EXISTS { MATCH (n) } AS anyNodes, COUNT { MATCH (n) } AS nodeCount
```

`CALL { ... }` supports subqueries that return rows and unit subqueries that
perform writes without returning rows. Import outer variables with `WITH`.
For a returning subquery, also include a final outer `RETURN`:

```cypher
UNWIND [1, 2] AS x
CALL { WITH x RETURN x * 2 AS doubled }
RETURN x, doubled
```

Use `collect(expression)` aggregation or a pattern comprehension to collect
values into a list. `COLLECT { ... }` subquery syntax is not supported.

---

## Streaming multiple result tables

Use `exec()` to read result tables from multiple statements:

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
table produced by `exec_with_options`. Existing Cypher `LIMIT` clauses
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

`Option::None` and `()` represent Cypher `null`. Integer values must fit in a
signed 64-bit integer, and floating-point values must be finite. Invalid nested
values are rejected too. A parameter name must be nonempty and omit `$`;
inserting an existing name replaces its value.

Use string-literal keys in `params!` for parameter names that are not Rust
identifiers, such as `$1`: `velr::params! { name: "Alice", "1" => 42_i64 }?`.

---

## Transactions and savepoints

Velr supports transactions together with two kinds of savepoints:

* **Scoped savepoints** via `savepoint()`, which return a guard
* **Named savepoints** via `savepoint_named(name)`, which remain active in the transaction until released or the transaction ends

Calling `rollback_to(name)` rolls back to the named savepoint, discards any newer named savepoints, and keeps the target savepoint active.
`commit()` releases any still-active named savepoints from newest to oldest so
that all surviving transaction changes are committed.

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
```

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

`release_savepoint(name)` requires the name of the most recent active named
savepoint. Names must be unique among active savepoints. Releasing a savepoint
keeps its changes pending in the outer transaction; commit the transaction to
persist them. Drop or finalize scoped savepoints before finalizing the transaction.

Dropping an active transaction without `commit()` will roll it back automatically.

---
## Explain plans

Use an explain trace to diagnose a query's behavior.

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

Inspect the returned `ExplainTrace` programmatically or render it as a compact
string for logging and debugging.

Use `explain_with_params(query, params)` for queries with bound parameters.
`explain_analyze` executes the query, including writes; use it inside a transaction
that you roll back when those changes should be discarded. The trace exposes
zero-based plan, step, and statement accessors, plus `snapshot()` for owned data
and `write_compact()` for writing a readable trace. Generated plan details can
change between releases.

---

## Arrow IPC

Arrow IPC support is enabled by default. With the default feature set you can:

* Bind Arrow arrays as a logical table (`bind_arrow`, `bind_arrow_chunks`)
* Bind Arrow IPC file / Feather v2 bytes as a logical table (`bind_arrow_ipc`)
* Export a result table as an Arrow IPC file (`to_arrow_ipc_file()`)

Add `arrow2 = "0.18"` to your application's dependencies when constructing
arrays with `arrow2`. `bind_arrow` takes one equal-length array per column name;
`bind_arrow_chunks` accepts one nonempty chunk list per column with matching
total row counts. Both consume the arrays, so the application need not retain
their buffers. `bind_arrow_ipc` accepts an Arrow IPC **file** (Feather v2),
not an IPC stream, and only borrows the input bytes during the call.

```rust,no_run
use arrow2::array::{Array, Utf8Array};
use velr::Velr;

fn main() -> velr::Result<()> {
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

The returned IPC bytes are owned and can be saved or read independently of the
table. Export reads the result and can use memory proportional to its size.

---

## Query functions and expressions

| Category | Available functions and forms |
|---|---|
| Graph and path | `id`, `type`, `labels`, `keys`, `properties`, `startNode`, `endNode`, `length`, `nodes`, `relationships`, `shortestPath`, `allShortestPaths` |
| Maps | Map projections such as `p {.name, .age}` and `p {.*, active: true}` |
| Lists | `size`, `head`, `last`, `tail`, `reverse`, `range`, list comprehensions, pattern comprehensions, `reduce(acc = initial, x IN list \| expression)` |
| Predicates and nulls | `all`, `any`, `none`, `single`, `exists(n.property)`, `IN`, `IS NULL`, `IS NOT NULL`, `coalesce`, `EXISTS { ... }` |
| Strings and conversions | `toInteger`, `toFloat`, `toBoolean`, `toString`, `toLower` / `lower`, `toUpper`, `replace`, `left`, `right`, `trim`, `lTrim`, `rTrim`, `substring(text, start [, length])`, `split`, `=~` |
| Numbers | `abs`, `ceil`, `floor`, `round(value)` / `round(value, precision)`, `rand`, `sign`, `sqrt`, `e`, `exp`, `log`, `log10` |
| Trigonometry | `acos`, `asin`, `atan`, `atan2`, `cos`, `cot`, `degrees`, `haversin`, `pi`, `radians`, `sin`, `tan` |
| Aggregates | `count`, `sum`, `avg`, `min`, `max`, `collect`, `percentileDisc`, `percentileCont`, `stDev`, `stDevP` |
| Temporal constructors | `date`, `time`, `localtime`, `datetime`, `localdatetime`, `duration`, `datetime.fromepoch`, `datetime.fromepochmillis` |
| Temporal clocks | `timestamp()`, plus `.realtime()`, `.transaction()`, and `.statement()` on `date`, `time`, `localtime`, `datetime`, and `localdatetime` |
| Temporal operations | `duration.between`, `duration.inMonths`, `duration.inDays`, `duration.inSeconds`; `.truncate(unit, value, fields)` on `date`, `time`, `localtime`, `datetime`, and `localdatetime` |
| Vector similarity | `vector.similarity.cosine(left, right)` |

`toLower(text)`, `lower(text)`, and `toUpper(text)` convert Unicode case.

Map projections accept node, relationship, and map variables. Use `.key` to
select a field, `.*` to include all fields, `key: expression` to add or override
a field, or a bare variable to include it under its name. A null source produces
null.

Name a path inside a pattern comprehension to use it in the projection,
as in `[p = (n)-[:LINK]->() | length(p)]`. A variable-length interval such
as `*2..1` produces no matches. In `MATCH`, `<-[:LINK]->` matches the
relationship in either direction.

Deeply nested expressions or long chains of operators can raise
`SyntaxError` with detail `QueryTooComplex`. Split nested computations into
`WITH` clauses or use `IN` for a long chain of equality comparisons.

Use `CREATE p = (:A)-[:LINK]->(:B) RETURN length(p)` to create a path and
return its length. `startNode(r)` and `endNode(r)` return the relationship's
stored endpoints. Both return `null` when the relationship is `null`.

When the host uses the strict `OpenCypher` query profile, a list assigned to a
graph property must contain non-null values of one property type. Integers and
floats can be combined in a list of floats.

`left(text, length)` and `right(text, length)` count Unicode code points and
require a nonnegative integer length when the text is present.

Use the openCypher operator `=~` to match an entire string against a regular
expression. Matching is case-sensitive by default. For example,
`RETURN 'alice' =~ '(?i)A.*' AS matched` returns `true`.

The pattern string uses the Rust regex dialect. Lookaround and backreferences
are unsupported. A null or non-string operand yields `null`. An invalid
pattern raises a query error when both operands are strings.
See [supported pattern syntax](https://docs.rs/regex/latest/regex/#syntax).

`timestamp()` returns Unix epoch milliseconds and has the same value throughout
a statement.

For temporal truncation, pass the third map argument even when no fields need
overriding, for example `date.truncate('month', date('2026-11-05'), {})`.

For two durations with matching months, days, seconds, and nanoseconds, `<=`
and `>=` return `true`. For unequal durations, both return `null`. Strict
comparisons (`<` and `>`) between durations always return `null`.

---

## Platform support

This crate links against a bundled native runtime. Cargo selects one platform-specific
`velr-runtime-*` crate for the current build target, so user installation stays:

```toml
[dependencies]
velr = "0.2"
```

Supported platforms:

* macOS aarch64 (Apple Silicon)
* macOS x86_64 (Intel)
* Linux x86_64
* Linux aarch64
* Windows x86_64


---

### Licensing 

* The **Rust binding source code** in this package is licensed under **MIT**.
* The **bundled native runtime binaries** may be **used and freely redistributed in unmodified form** under the terms of **`LICENSE.runtime`**.

See [`LICENSE`](LICENSE) and [`LICENSE.runtime`](LICENSE.runtime) for the full license texts.
