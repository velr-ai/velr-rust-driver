# Query language reference

Use this reference with the Velr runtime supplied with your driver. It describes
Cypher queries, functions, and Velr extensions. Driver method names, host-language
value conversions, resource ownership, and optional integrations are documented
in each driver's API reference. A server's configuration determines which
optional extensions it enables.

## Parameters and values

Bind application values through the driver's parameter API and refer to them as
`$name` in Cypher. Parameter keys omit `$`. Parameters represent values; they do
not substitute labels, relationship types, property names, or query clauses.

```cypher
UNWIND $people AS person
CREATE (:Person {name: person.name, age: person.age})
```

Drivers accept nulls, booleans, signed 64-bit integers, finite floating-point
numbers, strings, lists, and maps with string keys. Consult the driver reference
for the corresponding host-language types and validation errors. Query results
can additionally contain graph entities, paths, and temporal values; use the
driver's typed or decoded result APIs to read them.

## Clauses and expressions

| Task | Syntax |
|---|---|
| Read a graph | `MATCH`, `OPTIONAL MATCH`, `WHERE` |
| Search indexed entities | `MATCH ... SEARCH ... IN (VECTOR INDEX ... FOR ... LIMIT ...)` or `FULLTEXT INDEX`, with optional `SCORE AS` |
| Project and transform rows | `WITH`, `RETURN`, `DISTINCT`, `UNWIND` |
| Order and paginate | `ORDER BY`, `SKIP`, `LIMIT` |
| Write a graph | `CREATE`, `MERGE`, `SET`, `REMOVE`, `DELETE`, `DETACH DELETE`, `FOREACH` |
| Combine results | `UNION`, duplicate-preserving `UNION ALL` |
| Match paths | Variable-length relationships, `shortestPath`, `allShortestPaths` |
| Build lists | List literals, list comprehensions, pattern comprehensions, `reduce` |
| Build maps | Map literals and map projections |
| Branch on values | `CASE`, comparisons, arithmetic, boolean operators, null tests |
| Test, count, and collect subquery results | `EXISTS { ... }`, `COUNT { ... }`, `COLLECT { ... }` |
| Execute subqueries | Returning and unit `CALL { ... }` subqueries |

`WHERE` keeps a row only when its predicate is `true`; `false` and `null` do not
match. Compare text explicitly, such as `WHERE n.status = 'active'`. Test list
length explicitly with `WHERE size(values) > 0`.

`OPTIONAL MATCH` retains its input row and returns `null` for unmatched variables.
In `MATCH` and `MERGE`, `(a)<-[:REL]->(b)` matches either relationship direction,
as does `(a)-[:REL]-(b)`. `CREATE` requires one arrowhead to choose a direction.
A reversed path range such as `*3..1` produces no matching paths.

Give result columns unique names, using `AS` to distinguish projected
expressions. Duplicate result aliases fail query validation.

In a `CREATE` or `MERGE` relationship pattern, reference a node already in scope
as `(n)` and use `SET` to apply labels or properties. Repeating a relationship
variable in one `MATCH` produces no matches; use separate `MATCH` clauses to
reuse that relationship.

Use `FOREACH` to run updates for each item of a list. Each iteration completes
its updates in order before the next begins. The iterator and variables created
in the body are local to the loop. An empty or null list leaves the input row in
scope for later clauses.

```cypher
MATCH (p:Person {name: 'Alice'})
FOREACH (label IN ['graph', 'cypher'] | CREATE (p)-[:INTERESTED_IN]->(:Topic {name: label}))
RETURN p.name
```

### Lists, maps, and subqueries

```cypher
RETURN [x IN range(1, 5) WHERE x % 2 = 1 | x * x] AS oddSquares,
       reduce(total = 0, x IN [1, 2, 3] | total + x) AS total
```

`reduce` visits list elements in order. An empty list returns its initial
accumulator; a null list returns null. A pattern comprehension such as
`[(p)-[:KNOWS]->(friend) | friend.name]` returns matching values as a list.

Cypher lists stored as properties require a single element type. A list of
integers and floats is stored as floats. Lists used as intermediate query values
or returned directly may mix element types.

List access requires an integer index; map access requires a string key.
A null index or key returns null. List slices use integer bounds, for example
`values[1..3]`. An omitted bound extends to the beginning or end; an explicit
null bound returns null.

Map projections accept node, relationship, and map variables. Use `.key` to
select a property, `.*` to include all properties, `key: expression` to add or
replace a field, and a bare variable to include it under its name. A null source
returns null.

```cypher
WITH {name: 'Alice', age: 42} AS person
RETURN person {.name, .age, active: true} AS person
```

Read-only `EXISTS` and `COUNT` subqueries return a boolean and a row count.

```cypher
RETURN EXISTS { MATCH (n) } AS anyNodes,
       COUNT { MATCH (n) } AS nodeCount
```

`COLLECT` subqueries return one list per outer row and can read variables from
the outer query. Each branch must return one explicit column. Use `ORDER BY`
inside the subquery to order the members. Returned nulls remain in the list,
and a subquery with no rows returns an empty list.

```cypher
MATCH (p:Person)
RETURN p.name AS person,
       COLLECT {
           MATCH (p)-[:KNOWS]->(friend)
           RETURN friend.name AS name ORDER BY name
       } AS friends
```

`COLLECT` bodies support read pipelines built from `MATCH`, `OPTIONAL MATCH`,
`UNWIND`, and `WITH`, with `UNION` or `UNION ALL` between branches. Use
an explicit `MATCH` before a pattern.

A correlated `CALL` body imports outer variables explicitly with `WITH`.
Returning subqueries provide columns to an outer query; include the final outer
`RETURN`. Unit subqueries perform writes without returning rows.

```cypher
UNWIND [1, 2] AS x
CALL { WITH x RETURN x * 2 AS doubled }
RETURN x, doubled
```

APOC functions are not supplied by Velr.
Use numeric `id(entity)` when an entity identifier is needed; `elementId()` is
not supported.

## Functions

The tables below describe query-language functions, not driver methods.
Square brackets in a signature denote optional arguments.

### Graph and path

| Function | Result |
|---|---|
| `id(entity)` | Numeric node or relationship identifier. |
| `type(relationship)` | Relationship type name. |
| `labels(node)` | List of node label names. |
| `keys(value)` | Property or map key names. |
| `properties(value)` | Properties of a node, relationship, or map. |
| `length(path)` | Number of relationships in a path. |
| `nodes(path)` | Nodes of a path, in order. |
| `relationships(path)` | Relationships of a path, in order. |
| `shortestPath(pattern)` | A shortest path matching the relationship pattern. |
| `allShortestPaths(pattern)` | All shortest paths matching the relationship pattern. |

### Lists, predicates, and nulls

| Function or form | Result |
|---|---|
| `size(value)` | Size of a string or list. |
| `head(list)`, `last(list)` | First or last element. |
| `tail(list)` | List without its first element. |
| `reverse(value)` | Reversed string or list. |
| `range(start, end[, step])` | Inclusive sequence of integers. |
| `all(x IN list WHERE predicate)` | Whether every element satisfies the predicate. |
| `any(x IN list WHERE predicate)` | Whether any element satisfies the predicate. |
| `none(x IN list WHERE predicate)` | Whether no element satisfies the predicate. |
| `single(x IN list WHERE predicate)` | Whether exactly one element satisfies the predicate. |
| `reduce(acc = initial, x IN list \| expression)` | Accumulated result. |
| `coalesce(value, ...)` | First non-null value. |
| `value IN list` | List membership. |
| `value IS NULL`, `value IS NOT NULL` | Explicit null tests. |

`range` accepts integer and finite floating-point arguments, truncating floats
toward zero. The resulting step must be nonzero. Strings, booleans, and null
arguments raise `ArgumentError`.

### Strings and conversions

| Function | Result |
|---|---|
| `toInteger(value)` | Integer conversion. |
| `toFloat(value)` | Floating-point conversion. |
| `toBoolean(value)` | Boolean conversion. |
| `toBooleanOrNull(value)` | Boolean conversion with null for unsupported inputs. |
| `toString(value)` | String conversion. |
| `toLower(text)`, `lower(text)` | Lowercase string. |
| `toUpper(text)` | Uppercase string. |
| `trim(text)` | String with surrounding whitespace removed. |
| `substring(text, start[, length])` | Substring from a zero-based start. |
| `replace(text, search, replacement)` | String with matching text replaced. |
| `left(text, length)`, `right(text, length)` | Prefix or suffix counted in Unicode code points. |
| `split(text, delimiter)` | List of substrings. |

`trim` and `toLower` accept a string or null; incompatible values raise
`TypeError`. Convert non-string scalars with `toString` explicitly.
`left` and `right` require nonnegative integer lengths when text is present.

`toInteger` accepts booleans (`0`/`1`), numbers (truncated toward zero), strings,
and null. Invalid or unrepresentable numeric/string conversions return null;
lists and maps raise `TypeError`. `toFloat` accepts numbers, strings, and null;
invalid or non-finite string conversions return null, while booleans, lists,
and maps raise `TypeError`. `toBoolean` accepts booleans, integers, strings,
and null. Zero returns `false` and other integers return `true`. The strings
`'true'` and `'false'` are recognized case-insensitively after trimming
surrounding whitespace. Other strings return null. Unsupported input types
raise `TypeError` with `toBoolean` and return null with `toBooleanOrNull`.

`text =~ pattern` matches the entire string using
[Rust regex syntax](https://docs.rs/regex/latest/regex/#syntax). Inline flags
such as `(?i)` are supported; lookaround and backreferences are unsupported.
A null or non-string operand returns null. Invalid patterns raise a query error.

```cypher
RETURN toFloat('1.5') AS number, toBoolean('true') AS enabled,
       toUpper('hello') AS upper, replace('hello', 'll', 'y') AS replaced,
       left('hello', 2) AS prefix, right('hello', 2) AS suffix
```

### Spatial functions

| Function | Result |
|---|---|
| `point(coordinates)` | A Cartesian or WGS 84 point from a map or an entity's properties. |
| `point.distance(from, to)` | Distance between points in the same coordinate reference system. |

Use `x` and `y` for Cartesian coordinates or `longitude` and `latitude` for
WGS 84. Add `z` or `height`, respectively, for a 3D point. To select a coordinate
system explicitly, set `crs` to `cartesian`, `cartesian-3d`, `wgs-84`, or
`wgs-84-3d`, or set `srid` to `7203`, `9157`, `4326`, or `4979`, respectively.
Read point fields with expressions such as `p.x`, `p.srid`, and `p.crs`.
Geographic points also expose `longitude` and `latitude`, and geographic 3D
points expose `height`.

Cartesian distance uses the coordinate units. WGS 84 distance is in meters.
Distance returns null for null inputs or points in different coordinate systems.
Store points as individual node or relationship properties. Lists of points are
available as query expressions and results.

```cypher
WITH point({x: 0, y: 0}) AS origin, point({x: 3, y: 4}) AS endPoint
RETURN point.distance(origin, endPoint) AS distance
```

### Numbers and aggregation

| Function | Result |
|---|---|
| `abs(number)` | Absolute value. |
| `ceil(number)`, `floor(number)` | Rounded up or down. |
| `round(number[, precision])` | Rounded value; precision defaults to zero. |
| `rand()` | Random floating-point value. |
| `sign(number)` | Sign of a number. |
| `sqrt(number)` | Square root. |
| `count(*)`, `count(expression)` | Row count or count of non-null values. |
| `sum(expression)`, `avg(expression)` | Sum or average of numeric values. |
| `min(expression)`, `max(expression)` | Minimum or maximum value. |
| `collect(expression)` | Values collected into a list. |
| `percentileDisc(expression, percentile)` | Discrete percentile. |
| `percentileCont(expression, percentile)` | Interpolated percentile. |

Use `DISTINCT` inside an aggregate to remove repeated inputs.
`count()` is accepted as an alias of `count(*)`; prefer `count(*)` for portable
queries. `sum` and `avg` skip nulls and require numeric inputs. Convert numeric
strings explicitly, for example `sum(toFloat(n.amount))`. With no non-null
inputs, `sum` returns `0` and `avg` returns null.

`sqrt`, `floor`, and `round` accept numbers or null; incompatible values raise
`TypeError`. `round` accepts integer or finite floating-point precision,
truncating floats toward zero. A negative resulting integer precision, non-finite precision, and a NaN input
to `round` raise `ArgumentError`. `sqrt` and `floor` propagate NaN.
Integer overflow and integer division or modulo by zero raise `ArithmeticError`.

Percentiles require finite numeric inputs and a percentile in `0.0..1.0`,
inclusive, constant within each group. Null inputs are skipped. Incompatible
types raise `TypeError`; invalid percentiles raise `ArgumentError`.

```cypher
UNWIND [1, 2, 3] AS value
RETURN count(*) AS count, sum(value) AS total, avg(value) AS average,
       percentileDisc(value, 0.5) AS median, floor(1.8) AS floor,
       round(1.25, 1) AS rounded
```

### Temporal values

| Function | Arguments and result |
|---|---|
| `date`, `time`, `localtime`, `datetime`, `localdatetime` | Construct a value from supported text, a component map, or a compatible temporal value; no arguments selects the current transaction clock. |
| `duration(value)` | Construct a duration from duration text or a component map. |
| `datetime.fromepoch(seconds, nanoseconds)` | Datetime from Unix epoch seconds plus nanoseconds. |
| `datetime.fromepochmillis(milliseconds)` | Datetime from Unix epoch milliseconds. |
| `timestamp()` | Unix epoch milliseconds, constant throughout a statement. |
| `date.realtime()`, `time.realtime()`, `localtime.realtime()`, `datetime.realtime()`, `localdatetime.realtime()` | Current value from the real-time clock. |
| `date.transaction()`, `time.transaction()`, `localtime.transaction()`, `datetime.transaction()`, `localdatetime.transaction()` | Value fixed for the transaction. |
| `date.statement()`, `time.statement()`, `localtime.statement()`, `datetime.statement()`, `localdatetime.statement()` | Value fixed for the statement. |
| `duration.between(a, b)` | Calendar duration between compatible temporal values. |
| `duration.inMonths(a, b)`, `duration.inDays(a, b)`, `duration.inSeconds(a, b)` | Difference expressed in the requested unit. |
| `date.truncate(unit, value, fields)`, `time.truncate(unit, value, fields)`, `localtime.truncate(unit, value, fields)`, `datetime.truncate(unit, value, fields)`, `localdatetime.truncate(unit, value, fields)` | Truncated value with optional component overrides in the required third map argument. |

Temporal component names are case-insensitive: `epochseconds` and `epochSeconds`
refer to the same component. For truncation, supply `{}` when no overrides are
needed. Named timezones can be passed through a map such as
`datetime({timezone: 'Etc/UTC'})`.

Calendar months and days preserve local calendar behavior, including daylight
saving offset changes. Hours, minutes, and seconds shift an instant by elapsed
time. For a cutoff 30 calendar days ago, use
`datetime({timezone: 'Etc/UTC'}) - duration('P30D')`.

```cypher
RETURN date.truncate('month', date('2026-11-05'), {}) AS month,
       datetime.fromepoch(1, 0).epochSeconds AS seconds
```

## Fulltext and vector search

Fulltext and vector indexes require a file-backed database. Run index DDL and
searches through the driver's normal query APIs. Indexed searches support
`SEARCH` clauses and the `CALL` procedures described below.

### SEARCH clauses

Use `SEARCH` with `MATCH` or `OPTIONAL MATCH` to search a fulltext or vector
index. These forms require Velr 0.2.62 or a later runtime that supports them.
Create the index before querying it. For example, create and query a fulltext
index over a text property:

```cypher
CREATE (:SearchPaper {title: 'Graph databases', category: 'guides'});
CREATE FULLTEXT INDEX searchText IF NOT EXISTS
FOR (p:SearchPaper) ON EACH [p.title];
MATCH (p:SearchPaper)
SEARCH p IN (FULLTEXT INDEX searchText FOR 'graph'
             WITH ANALYZER 'standard' OFFSET 0 LIMIT 10)
SCORE AS score
RETURN p.title AS title, score ORDER BY score DESC
```

For a stored-vector index, pass a numeric list or typed vector with the index's
dimensions. Declare properties used by an inner filter with the index DDL's
`WITH [...]` clause:

```cypher
CREATE (:SearchChunk {text: 'Connected data', category: 'guides', embedding: [1.0, 0.0]});
CREATE VECTOR INDEX searchVector IF NOT EXISTS
FOR (c:SearchChunk) ON (c.embedding) WITH [c.category]
OPTIONS {indexConfig: {dimensions: 2, metric: 'cosine'}};
MATCH (c:SearchChunk)
SEARCH c IN (VECTOR INDEX searchVector FOR [1.0, 0.0]
             WHERE c.category = 'guides' LIMIT 5)
SCORE AS score
RETURN c.text AS text, score ORDER BY score DESC
```

`SCORE AS name` is optional. Its variable must be new in the current scope.
Use `ORDER BY score DESC` when the final output needs score order. `LIMIT` inside
the search bounds candidates; an outer `RETURN ... LIMIT` bounds final rows.

| Option | Contract |
|---|---|
| `FOR expression` | Fulltext takes a query string; vector takes a numeric list or typed vector. It can reference earlier input variables or parameters, but not the searched entity. |
| Index name | Vector uses an identifier, with backticks when needed. Fulltext also accepts a string parameter, as in `FULLTEXT INDEX $index`. |
| `LIMIT expression` | Required integer in `0..2147483647`. Use constants, static expressions, or parameters; it cannot depend on query variables. Zero produces no matches. |
| `SKIP` / `OFFSET expression` | Fulltext only; integer in `0..9223372036854775807`, with the same constant/parameter restriction. Applied before the inner limit. |
| `WITH ANALYZER expression` | Fulltext only. Supports `standard` (default), `standard-no-stop-words`, `default`, `keyword`, `whitespace`, `simple`, and `english`. |
| Inner `WHERE` | Vector only; filters eligible candidates before selecting nearest neighbors. Declare every filtered property with the index's `WITH [...]` clause. |

Inner vector filters support comparisons (`=`, `<`, `<=`, `>`, `>=`), `IN`,
boolean property tests, and `IS NULL` / `IS NOT NULL`, combined with `AND`.
Each property can have one lower and one upper bound, or one equality, `IN`, or
null test. An enclosing `MATCH WHERE`, placed before or after `SEARCH`, filters
the selected candidates afterward and can leave fewer than the requested count.
Fulltext filtering also uses the enclosing `WHERE`.

Cosine `SEARCH` and vector procedure scores are `(1 + cosine) / 2`, clamped to
`0..1`. Euclidean/L2 `SEARCH` scores are `1 / (1 + squaredDistance)`, while
the procedures return `-squaredDistance` for Euclidean/L2. Higher is better.
Dot-product scores are unbounded. Fulltext scores are query-specific and
unnormalized.

A null query value produces no matches. `OPTIONAL MATCH ... SEARCH` preserves
an unmatched input row with null entity and score values. Earlier input rows can
provide query vectors, text, and filter values. Search also works inside
correlated `EXISTS`, `COUNT`, and `CALL` subqueries.

A search pattern contains one node or one relationship hop. Only the searched
element may be named or constrained. Multiple patterns, named paths, shortest
path selectors, and variable-length search patterns are unsupported. Use
additional query clauses to expand or filter the search results.

Query errors identify the invalid index, vector, filter, analyzer, or pagination
value and provide repair guidance. Use `SHOW INDEXES` to check an index's name
and family; choose `VECTOR INDEX` or `FULLTEXT INDEX` accordingly.

### Fulltext search

```cypher
CREATE (:Paper {title: 'Graph databases', abstract: 'Connected data'});
CREATE FULLTEXT INDEX paperText IF NOT EXISTS
FOR (n:Paper) ON EACH [n.title, n.abstract];
CALL db.index.fulltext.queryNodes('paperText', 'title:graph')
YIELD node, score
RETURN node, score
```

Search text supports terms, quoted phrases, property scoping (`title:graph`),
`AND`/`OR` and grouping, required/excluded terms (`+graph -draft`), phrase slop
(`"vector search"~2`), term prefixes (`vec*`), phrase prefixes
(`"vector sea"*`), boosts (`title:graph^2.0`), and `*` to match all indexed nodes.
Adjacent terms use `OR`. A field scope applies to the next term or phrase only.
Apostrophes within words can be entered directly; bind search text as a string
parameter. `?` wildcards and term-level fuzzy matching are unsupported.

Fulltext scores are query-specific, unnormalized relevance values. Higher scores
rank results within one result set. Do not compare scores across queries.

### Vector search

```cypher
CREATE (:Chunk {text: 'Connected data', embedding: [0.12, 0.34, 0.56]});
CREATE VECTOR INDEX chunkEmbedding IF NOT EXISTS
FOR (n:Chunk) ON (n.embedding)
OPTIONS {indexConfig: {dimensions: 3, metric: 'cosine'}};
CALL db.index.vector.queryNodes('chunkEmbedding', 10, [0.10, 0.30, 0.50])
YIELD node, score
RETURN node, score
```

Stored-vector indexes use `ON (n.embedding)` and accept numeric query vectors.
For text-to-vector search, register an embedder through the driver and use
`ON EACH [n.title, n.abstract]` with `embedder: 'name'`. The callback receives
selected property values in that order and query text as one unnamed field.
Return one finite vector of the requested dimensions per input, in input order.
Consult the driver reference for callback types and reentrancy restrictions.

| Index option | Aliases | Contract |
|---|---|---|
| `vector.dimensions` | `dimensions` | Required vector width, shared by stored values, embedder outputs, and query vectors. |
| `vector.similarity_function` | `metric`, `similarity_function` | `cosine` (default), `l2`/`euclidean`, or `dot`/`inner_product`/`max_inner_product`. |
| `embedder` | `velr.embedder` | Registered callback name for `ON EACH [...]`; omit for stored-vector indexes. |
| `embedder_fingerprint` | `embedderFingerprint`, `velr.embedder_fingerprint` | Application model/version metadata; does not select or invoke a model. |
| `cache_policy` | `cachePolicy` | `required` retains generated embeddings for recovery; `best_effort` tolerates failures to retain them; `none` requires recomputation for recovery. Defaults to `required` with an embedder and `none` without one. Stored-vector indexes require `none`. |
| `vector.hnsw.m` | — | Integer in `1..512`. Increasing it can improve recall with higher memory, disk usage, and build cost. |
| `vector.hnsw.ef_construction` | — | Integer in `1..3200`. Increasing it can improve index quality with slower builds. |
| `vector.hnsw.ef_search` | — | Integer in `1..3200`, default `64`. Increasing it can improve recall with slower queries. |

Quote dotted option names with backticks inside `indexConfig` maps.
Vector scores depend on the metric. Higher scores are better within each
result set. For cosine indexes, `SEARCH`, `db.index.vector.queryNodes`, and
`db.index.vector.queryRelationships` return scores on the same `0..1` scale as
`vector.similarity.cosine(left, right)`. The function computes exact similarity
without an index. Cosine query vectors require a nonzero component. To migrate a
threshold from a Velr version that returned raw cosine in the procedures,
convert it with `(1 + threshold) / 2`.

A neighbor count of zero returns no candidates. `WHERE` after `YIELD` filters
the selected candidates and can return fewer rows than requested. Searches over
stored-vector indexes see a transaction's own writes. For an embedder-backed
index with pending changes, commit or roll back before searching it.

## Data integration, diagnostics, and maintenance

| Operation | Syntax and prerequisites |
|---|---|
| Query bound Arrow data | `UNWIND BIND('name') AS row`; bind data first through a driver with Arrow support. |
| Inspect indexes | `SHOW INDEXES`; returns index names, families, properties, and options. |
| List node labels | `CALL db.labels() YIELD label RETURN label` |
| List relationship types | `CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType` |
| List property keys | `CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey` |
| Inspect observed graph shape | `SHOW CURRENT GRAPH SHAPE`; use `YIELD`, `WHERE`, and `RETURN` to select fields, or `YIELD *` for all fields. |
| Inspect a query | `EXPLAIN` or the driver's explain API. `EXPLAIN ANALYZE` executes the query, including writes. |
| Apply database migrations | `MIGRATE DATABASE` or the driver's migration API; use a writable connection. Opening a supported older database does not migrate it. |
| Compact a database | `VACUUM DATABASE` or the driver's maintenance API; requires a writable current-schema database outside a transaction. |

Driver row caps limit returned rows per result table. They preserve column
metadata when zero, honor a query's `LIMIT`, and do not prevent write effects or
limit execution time. Use query deadlines or cancellation where supported.
Queries can fail when execution starts, a stream advances, or rows are consumed;
handle errors around the complete operation and release results afterward.

With the current runtime, supported older database formats must be explicitly
migrated before graph queries or writes. Opening never migrates automatically.
Use `schema_version()`, `current_schema_version()`, and `needs_migration()` (or
their language equivalents) to inspect compatibility, then migrate on a writable
connection. Reopen read-only connections after migration. Catalog introspection
describes supported catalogs; it does not imply `SHOW FUNCTIONS` or
`SHOW PROCEDURES` support.
