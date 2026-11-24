use std::{env, fs, path::PathBuf, sync::OnceLock};
use velr::{CellRef, Extension, Result, Velr};

const SEED: &str = "CREATE (a:Point {rank:0}), (b:Point {rank:1}), (c:Point {rank:2}),
                            (:Point {rank:3}),
                            (a)-[:ROUTE {cost:0.5}]->(b),
                            (b)-[:ROUTE {cost:1.25}]->(c),
                            (a)-[:ROUTE {cost:5.0}]->(c)";
const PROJECT: &str = "CALL gds.graph.project('routes', 'Point', {ROUTE:{properties:'cost'}})";
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
    let paths = db
        .exec_one_with_params(
            "CALL gds.shortestPath.dijkstra.stream('routes', {
                 sourceNode:$source, targetNodes:[$target], relationshipWeightProperty:'cost'
             }) YIELD totalCost, path
             RETURN totalCost, length(path) AS hops,
                    [r IN relationships(path) | r.cost] AS edgeCosts",
            velr::params! { source: ids[0], target: ids[2] }?,
        )?
        .collect(|row| match row {
            [CellRef::Float(cost), CellRef::Integer(hops), CellRef::Json(edge_costs)] => {
                let edge_costs = serde_json::from_slice::<Vec<f64>>(edge_costs)
                    .map_err(velr::Error::application)?;
                Ok((*cost, *hops, edge_costs))
            }
            _ => panic!("expected costs and hop count, got {row:?}"),
        })?;
    assert_eq!(paths, [(1.75, 2, vec![0.5, 1.25])]);

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
