use std::{
    env, fs,
    path::PathBuf,
    process::{self, Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

use velr::{
    CellRef, ExecTables, ExecTablesTx, ExplainTrace, RowIter, TableResult, Velr, VelrSavepoint,
    VelrTx,
};

const CHILD_ENV: &str = "VELR_RUNTIME_STARTUP_CHILD";
const WORKERS_ENV: &str = "VELR_RUNTIME_STARTUP_WORKERS";

#[test]
fn public_driver_threading_contract_is_send_not_sync() -> velr::Result<()> {
    static_assertions::assert_impl_all!(Velr: Send);
    static_assertions::assert_not_impl_any!(Velr: Sync);
    static_assertions::assert_not_impl_any!(ExecTables<'static>: Send, Sync);
    static_assertions::assert_not_impl_any!(TableResult: Send, Sync);
    static_assertions::assert_not_impl_any!(RowIter<'static>: Send, Sync);
    static_assertions::assert_not_impl_any!(VelrTx<'static>: Send, Sync);
    static_assertions::assert_not_impl_any!(ExecTablesTx<'static>: Send, Sync);
    static_assertions::assert_not_impl_any!(VelrSavepoint<'static>: Send, Sync);
    static_assertions::assert_not_impl_any!(ExplainTrace: Send, Sync);

    let db = Velr::open(None)?;
    let handle = std::thread::spawn(move || -> velr::Result<i64> {
        let mut table = db.exec_one("RETURN 1 AS n")?;
        let mut value = 0;
        table.for_each_row(|row| {
            value = match row[0] {
                CellRef::Integer(n) => n,
                _ => panic!("expected integer row"),
            };
            Ok(())
        })?;
        Ok(value)
    });
    assert_eq!(handle.join().expect("connection worker joins")?, 1);
    Ok(())
}

fn temp_cache_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    env::temp_dir().join(format!(
        "velr-runtime-startup-{name}-{}-{nanos}",
        process::id()
    ))
}

#[test]
fn parallel_cold_cache_child() -> velr::Result<()> {
    if env::var_os(CHILD_ENV).is_none() {
        return Ok(());
    }

    let db = Velr::open(None)?;
    let mut table = db.exec_one("RETURN 1 AS n")?;
    let mut values = Vec::new();
    table.for_each_row(|row| {
        let n = match row[0] {
            CellRef::Integer(n) => n,
            _ => panic!("expected integer row"),
        };
        values.push(n);
        Ok(())
    })?;
    assert_eq!(values, vec![1]);

    Ok(())
}

#[test]
fn parallel_cold_cache_runtime_startup_succeeds() {
    let cache_dir = temp_cache_dir("processes");
    let exe = env::current_exe().expect("current test executable");
    let workers = env::var(WORKERS_ENV)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(16);

    let mut children = Vec::new();
    for index in 0..workers {
        let child = Command::new(&exe)
            .arg("parallel_cold_cache_child")
            .arg("--exact")
            .env(CHILD_ENV, "1")
            .env("VELR_CACHE_DIR", &cache_dir)
            .env("RUST_BACKTRACE", "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|err| panic!("spawn child {index}: {err}"));
        children.push((index, child));
    }

    let mut failures = Vec::new();
    for (index, child) in children {
        let output = child
            .wait_with_output()
            .unwrap_or_else(|err| panic!("wait for child {index}: {err}"));
        if !output.status.success() {
            failures.push(format!(
                "child {index} failed with status {}\nstdout:\n{}\nstderr:\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ));
        }
    }

    let _ = fs::remove_dir_all(&cache_dir);
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}
