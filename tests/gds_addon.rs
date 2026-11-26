use std::{env, fs, path::PathBuf, sync::OnceLock};
use velr::{CellRef, Extension, Result, Velr};

const SEED: &str = "CREATE (a:Point {rank:0, lon:0.0}), (b:Point {rank:1, lon:0.005}),
                            (c:Point {rank:2, lon:0.015}), (:Point {rank:3}),
                            (a)-[:ROUTE {cost:0.5}]->(b),
                            (b)-[:ROUTE {cost:1.25}]->(c),
                            (a)-[:ROUTE {cost:5.0}]->(c)";
const PROJECT: &str = "CALL gds.graph.project('routes', {Point:{properties:{
                          latitude:{defaultValue:0},
                          longitude:{property:'lon', defaultValue:0.0}
                      }}}, {ROUTE:{properties:'cost'}})";
const EXISTS: &str = "CALL gds.graph.exists('routes') YIELD exists RETURN exists";
const MEMORY_LIMIT: usize = 512 * 1024 * 1024;

#[cfg(target_os = "linux")]
const FILENAME: &str = "libvelr_gds.so";
#[cfg(target_os = "macos")]
const FILENAME: &str = "libvelr_gds.dylib";
#[cfg(target_os = "windows")]
const FILENAME: &str = "velr_gds.dll";

fn native_plugin() -> Option<&'static [u8]> {
    static PLUGIN: OnceLock<Vec<u8>> = OnceLock::new();
    let Some(path) = env::var_os("VELR_GDS_TEST_PLUGIN") else {
        assert_ne!(
            env::var("VELR_GDS_TEST_REQUIRED").as_deref(),
            Ok("1"),
            "VELR_GDS_TEST_PLUGIN must name the built native GDS add-on",
        );
        eprintln!("Skipping GDS add-on smoke test. Set VELR_GDS_TEST_PLUGIN to enable it.");
        return None;
    };
    let path = PathBuf::from(path);
    Some(PLUGIN.get_or_init(|| {
        fs::read(&path).unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()))
    }))
}

fn extension(native: &'static [u8], memory_limit_bytes: usize) -> Extension {
    // The test fixture is the trusted release build of the Velr GDS add-on.
    unsafe { Extension::from_bundled_native(FILENAME, native, memory_limit_bytes) }
}

fn graph_exists(db: &Velr) -> Result<bool> {
    let exists = db.exec_one(EXISTS)?.collect(|row| match row {
        [CellRef::Bool(value)] => Ok(*value),
        _ => panic!("expected graph existence boolean, got {row:?}"),
    })?;
    assert_eq!(exists.len(), 1);
    Ok(exists[0])
}

#[test]
fn loads_gds_and_reuses_only_the_opted_in_connection() -> Result<()> {
    let Some(native) = native_plugin() else {
        return Ok(());
    };
    let plain = Velr::open(None)?;
    assert!(plain.run("RETURN gds.version()").is_err());
    let db = Velr::builder()
        .with_extension(extension(native, MEMORY_LIMIT))
        .open(None)?;
    let other = Velr::builder()
        .with_extension(extension(native, MEMORY_LIMIT))
        .open(None)?;

    db.run(SEED)?;
    db.run(PROJECT)?;
    assert!(graph_exists(&db)?);
    assert!(!graph_exists(&other)?);

    let ids: Vec<i64> = db
        .exec_one("MATCH (p:Point) RETURN id(p) ORDER BY p.rank")?
        .collect(|row| match row {
            [CellRef::Integer(id)] => Ok(*id),
            _ => panic!("expected one node ID, got {row:?}"),
        })?;
    assert_eq!(ids.len(), 4);
    let shortest = (1.75, 2, vec![0.5, 1.25]);
    for (call, expected) in [
        (
            "CALL gds.shortestPath.dijkstra.stream('routes', {
                sourceNode:$source, targetNodes:[$target], relationshipWeightProperty:'cost'
            })",
            vec![shortest.clone()],
        ),
        (
            "CALL gds.shortestPath.astar.stream('routes', {
                sourceNode:$source, targetNode:$target, relationshipWeightProperty:'cost',
                latitudeProperty:'latitude', longitudeProperty:'longitude'
            })",
            vec![shortest.clone()],
        ),
        (
            "CALL gds.shortestPath.yens.stream('routes', {
                sourceNode:$source, targetNode:$target, relationshipWeightProperty:'cost', k:3
            })",
            vec![shortest, (5.0, 1, vec![5.0])],
        ),
    ] {
        let query = format!(
            "{call} YIELD index, totalCost, path
             RETURN totalCost, length(path) AS hops,
                    [r IN relationships(path) | r.cost] AS edgeCosts ORDER BY index"
        );
        let paths = db
            .exec_one_with_params(&query, velr::params! { source: ids[0], target: ids[2] }?)?
            .collect(|row| match row {
                [CellRef::Float(cost), CellRef::Integer(hops), CellRef::Json(edge_costs)] => {
                    let edge_costs = serde_json::from_slice::<Vec<f64>>(edge_costs)
                        .map_err(velr::Error::application)?;
                    Ok((*cost, *hops, edge_costs))
                }
                _ => panic!("expected costs and hop count, got {row:?}"),
            })?;
        assert_eq!(paths, expected, "{call}");
    }

    let single_source = |algorithm: &str| -> Result<Vec<(i64, f64, i64)>> {
        let query = format!(
            "CALL gds.allShortestPaths.{algorithm}.stream('routes', {{
                sourceNode:$source, relationshipWeightProperty:'cost'
             }}) YIELD targetNode, totalCost, path
             RETURN targetNode, totalCost, length(path) AS hops ORDER BY targetNode"
        );
        db.exec_one_with_params(&query, velr::params! { source: ids[0] }?)?
            .collect(|row| match row {
                [CellRef::Integer(target), CellRef::Float(cost), CellRef::Integer(hops)] => {
                    Ok((*target, *cost, *hops))
                }
                _ => panic!("expected target ID, cost, and hop count, got {row:?}"),
            })
    };
    let expected = single_source("dijkstra")?;
    assert_eq!(
        expected,
        [(ids[0], 0.0, 0), (ids[1], 0.5, 1), (ids[2], 1.75, 2)]
    );
    assert_eq!(single_source("delta")?, expected);

    let traversal = db
        .exec_one_with_params(
            "CALL gds.dfs.stream('routes', {sourceNode:$source}) YIELD nodeIds, path
             RETURN nodeIds, length(path) AS hops",
            velr::params! { source: ids[0] }?,
        )?
        .collect(|row| match row {
            [CellRef::Json(nodes), CellRef::Integer(hops)] => {
                let nodes =
                    serde_json::from_slice::<Vec<i64>>(nodes).map_err(velr::Error::application)?;
                Ok((nodes, *hops))
            }
            _ => panic!("expected DFS visit order and hop count, got {row:?}"),
        })?;
    assert_eq!(traversal, [(vec![ids[0], ids[2], ids[1]], 2)]);

    let similarities = db
        .exec_one(
            "CALL gds.nodeSimilarity.stream('routes') YIELD node1, node2, similarity
             RETURN node1, node2, similarity ORDER BY node1, node2",
        )?
        .collect(|row| match row {
            [CellRef::Integer(node1), CellRef::Integer(node2), CellRef::Float(similarity)] => {
                Ok((*node1, *node2, *similarity))
            }
            _ => panic!("expected node IDs and Jaccard score, got {row:?}"),
        })?;
    assert_eq!(similarities, [(ids[0], ids[1], 0.5), (ids[1], ids[0], 0.5)]);

    let stored = db
        .exec_one(
            "CALL gds.fastRP.write('routes', {
                embeddingDimension:16, randomSeed:7, writeProperty:'savedEmbedding'
             })
             YIELD nodePropertiesWritten RETURN nodePropertiesWritten",
        )?
        .collect(|row| match row {
            [CellRef::Integer(written)] => Ok(*written),
            _ => panic!("expected the embedding count, got {row:?}"),
        })?;
    assert_eq!(stored, [4]);
    db.run(
        "CALL gds.graph.project('saved-routes','Point','ROUTE',{
            nodeProperties:{embedding:'savedEmbedding'}
         })",
    )?;
    let neighbors = db
        .exec_one(
            "CALL gds.knn.stream('saved-routes', {nodeProperties:'embedding', topK:1})
             YIELD node1, node2, similarity RETURN node1, node2, similarity",
        )?
        .collect(|row| match row {
            [CellRef::Integer(node1), CellRef::Integer(node2), CellRef::Float(similarity)] => {
                Ok((*node1, *node2, *similarity))
            }
            _ => panic!("expected projected neighbors and a Float score, got {row:?}"),
        })?;
    assert_eq!(neighbors.len(), 4);
    assert!(neighbors
        .iter()
        .all(|(left, right, similarity)| { left != right && (0.0..=1.0).contains(similarity) }));

    db.run("CALL gds.graph.drop('saved-routes')")?;
    let components = db
        .exec_one(
            "CALL gds.wcc.stream('routes') YIELD nodeId, componentId
             RETURN nodeId, componentId ORDER BY nodeId",
        )?
        .collect(|row| match row {
            [CellRef::Integer(id), CellRef::Integer(component)] => Ok((*id, *component)),
            _ => panic!("expected node and component IDs, got {row:?}"),
        })?;
    assert_eq!(components.len(), 4);
    assert_eq!(components[0].1, components[1].1);
    assert_eq!(components[1].1, components[2].1);
    assert_ne!(components[2].1, components[3].1);

    db.run("CALL gds.graph.drop('routes')")?;
    assert!(!graph_exists(&db)?);
    assert!(plain.run("RETURN gds.version()").is_err());
    Ok(())
}

#[test]
fn projects_stored_seeds_and_writes_communities_through_the_native_addon() -> Result<()> {
    let Some(native) = native_plugin() else {
        return Ok(());
    };
    let db = Velr::builder()
        .with_extension(extension(native, MEMORY_LIMIT))
        .open(None)?;
    // More than one native property batch, with an Integer that would lose
    // precision if either bridge treated it as a Float.
    db.run("CREATE (:Person {storedSeed:9007199254740993, storedWeight:1.25})")?;
    db.run("UNWIND range(1, 1025) AS i CREATE (:Person {i:i})")?;
    db.run(
        "CALL gds.graph.project('seeded', {Person:{properties:{
           seed:{property:'storedSeed', defaultValue:42},
           weight:{property:'storedWeight', defaultValue:0.5}
         }}}, 'KNOWS')",
    )?;
    let weights = db
        .exec_one(
            "CALL gds.graph.nodeProperty.stream('seeded', 'weight')
             YIELD propertyValue RETURN propertyValue ORDER BY propertyValue",
        )?
        .collect(|row| match row {
            [CellRef::Float(weight)] => Ok(*weight),
            _ => panic!("expected projected Float weight, got {row:?}"),
        })?;
    assert_eq!(weights.len(), 1026);
    assert!(weights[..1025].iter().all(|weight| *weight == 0.5));
    assert_eq!(weights[1025], 1.25);
    db.run(
        "CALL gds.labelPropagation.write('seeded', {
           seedProperty:'seed', writeProperty:'community'
         })",
    )?;
    let rows = db
        .exec_one(
            "MATCH (p:Person)
             RETURN p.community AS community, count(*) AS n ORDER BY community",
        )?
        .collect(|row| match row {
            [CellRef::Integer(seed), CellRef::Integer(count)] => Ok((*seed, *count)),
            _ => panic!("expected exact community seed and count, got {row:?}"),
        })?;
    assert_eq!(rows, [(42, 1025), (9_007_199_254_740_993, 1)]);
    Ok(())
}

#[test]
fn rejects_duplicate_extensions_and_does_not_publish_over_budget_graphs() -> Result<()> {
    let Some(native) = native_plugin() else {
        return Ok(());
    };
    assert!(Velr::builder()
        .with_extension(extension(native, MEMORY_LIMIT))
        .with_extension(extension(native, MEMORY_LIMIT))
        .open(None)
        .is_err());

    let db = Velr::builder()
        .with_extension(extension(native, 1))
        .open(None)?;
    db.run(SEED)?;
    let error = db.run(PROJECT).expect_err("projection must exceed 1 byte");
    assert!(error.message.contains("analytics memory limit"), "{error}");
    assert!(!graph_exists(&db)?);
    Ok(())
}

#[test]
fn writes_and_reprojects_embeddings_across_native_batches() -> Result<()> {
    let Some(native) = native_plugin() else {
        return Ok(());
    };
    let db = Velr::builder()
        .with_extension(extension(native, MEMORY_LIMIT))
        .open(None)?;
    // The native property transport fills bounded groups of 1024 rows.
    db.run("UNWIND range(0,1024) AS i CREATE (:BatchItem {rank:i})")?;
    db.run("CALL gds.graph.project('batch','BatchItem','*')")?;
    let written = db
        .exec_one(
            "CALL gds.fastRP.write('batch',{
                    embeddingDimension:2, iterationWeights:[], nodeSelfInfluence:1,
                    randomSeed:7, writeProperty:'savedEmbedding'
                   })
                   YIELD nodePropertiesWritten RETURN nodePropertiesWritten",
        )?
        .collect(|row| match row {
            [CellRef::Integer(count)] => Ok(*count),
            _ => panic!("expected embedding count, got {row:?}"),
        })?;
    assert_eq!(written, [1025]);
    db.run(
        "CALL gds.graph.project('loaded','BatchItem','*',{
              nodeProperties:{embedding:'savedEmbedding'}
            })",
    )?;
    let loaded = db
        .exec_one(
            "CALL gds.graph.nodeProperty.stream('loaded','embedding') YIELD propertyValue
                   RETURN count(propertyValue) AS stored",
        )?
        .collect(|row| match row {
            [CellRef::Integer(count)] => Ok(*count),
            _ => panic!("expected projected vector count, got {row:?}"),
        })?;
    assert_eq!(loaded, [1025]);
    Ok(())
}

#[test]
fn writes_knn_scores_across_native_batches() -> Result<()> {
    let Some(native) = native_plugin() else {
        return Ok(());
    };
    let db = Velr::builder()
        .with_extension(extension(native, MEMORY_LIMIT))
        .open(None)?;
    // A node outside the projection shifts the database IDs. A top-1 result
    // for 1025 nodes crosses the native transport's 1024-row batch boundary.
    db.run("CREATE (:OutsideProjection)")?;
    db.run("UNWIND range(0,1024) AS i CREATE (:Item {rank:i})")?;
    db.run("CALL gds.graph.project('items','Item','*',{nodeProperties:{ranking:'rank'}})")?;
    let scores = |statement: &str| -> Result<Vec<(i64, i64, f64)>> {
        db.exec_one(statement)?.collect(|row| match row {
            [CellRef::Integer(source), CellRef::Integer(target), CellRef::Float(score)] => {
                Ok((*source, *target, *score))
            }
            _ => panic!("expected two node IDs and a Float score, got {row:?}"),
        })
    };
    let streamed = scores(
        "CALL gds.knn.stream('items',{nodeProperties:'ranking',topK:1})
         YIELD node1,node2,similarity RETURN node1,node2,similarity ORDER BY node1,node2",
    )?;
    assert_eq!(streamed.len(), 1025);
    let written = db
        .exec_one(
            "CALL gds.knn.write('items',{
                nodeProperties:'ranking',topK:1,writeRelationshipType:'SIMILAR',writeProperty:'score'
             }) YIELD relationshipsWritten RETURN relationshipsWritten",
        )?
        .collect(|row| match row {
            [CellRef::Integer(count)] => Ok(*count),
            _ => panic!("expected relationship count, got {row:?}"),
        })?;
    assert_eq!(written, [1025]);
    assert_eq!(
        scores(
            "MATCH (a:Item)-[r:SIMILAR]->(b:Item)
             RETURN id(a) AS node1,id(b) AS node2,r.score AS similarity ORDER BY node1,node2",
        )?,
        streamed
    );
    Ok(())
}
