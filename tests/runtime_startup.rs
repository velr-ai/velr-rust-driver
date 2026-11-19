use std::{
    env, fs,
    path::PathBuf,
    process::{self, Command, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use velr::{
    CellRef, ExecTables, ExecTablesTx, ExplainTrace, RowIter, TableResult, Velr, VelrSavepoint,
    VelrTx,
};

const CHILD_ENV: &str = "VELR_RUNTIME_STARTUP_CHILD";
const WORKERS_ENV: &str = "VELR_RUNTIME_STARTUP_WORKERS";
const LOCK_READY_ENV: &str = "VELR_RUNTIME_STARTUP_LOCK_READY";

#[cfg(all(target_os = "linux", target_arch = "aarch64", target_env = "gnu"))]
use velr_runtime_linux_aarch64 as selected_runtime;
#[cfg(all(target_os = "linux", target_arch = "x86_64", target_env = "gnu"))]
use velr_runtime_linux_x86_64 as selected_runtime;
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
use velr_runtime_macos_aarch64 as selected_runtime;
#[cfg(all(target_os = "macos", target_arch = "x86_64"))]
use velr_runtime_macos_x86_64 as selected_runtime;
#[cfg(all(target_os = "windows", target_arch = "x86_64", target_env = "msvc"))]
use velr_runtime_windows_x86_64 as selected_runtime;

struct CacheCleanup(PathBuf);

impl Drop for CacheCleanup {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct ChildGuard(Option<process::Child>);

impl ChildGuard {
    fn wait_with_output(mut self) -> process::Output {
        let deadline = Instant::now() + Duration::from_secs(60);
        while self
            .0
            .as_mut()
            .expect("child")
            .try_wait()
            .expect("inspect child")
            .is_none()
        {
            assert!(Instant::now() < deadline, "runtime startup child timed out");
            std::thread::sleep(Duration::from_millis(10));
        }
        self.0
            .take()
            .expect("child")
            .wait_with_output()
            .expect("wait for child")
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn startup_child() -> Command {
    let exe = env::current_exe().expect("current test executable");
    let mut command = Command::new(exe);
    command
        .arg("parallel_cold_cache_child")
        .arg("--exact")
        .env(CHILD_ENV, "1")
        .env_remove("VELR_RUNTIME_PATH")
        .env_remove(LOCK_READY_ENV)
        .env("RUST_BACKTRACE", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

#[test]
fn public_driver_threading_contract_is_send_not_sync() -> velr::Result<()> {
    static_assertions::assert_impl_all!(Velr: Send);
    static_assertions::assert_not_impl_any!(Velr: Sync);
    static_assertions::assert_not_impl_any!(ExecTables<'static>: Send, Sync);
    static_assertions::assert_not_impl_any!(TableResult<'static>: Send, Sync);
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

    if let Some(ready) = env::var_os(LOCK_READY_ENV) {
        // Simulate a process exiting during a protected cold-cache write. The
        // parent kills this process, so no Rust destructor releases the lock.
        let (bytes, filename) = selected_runtime::bytes_and_name();
        let hash = blake3::hash(bytes).to_hex();
        let dir =
            PathBuf::from(env::var_os("VELR_CACHE_DIR").expect("child cache")).join("velr/runtime");
        fs::create_dir_all(&dir).expect("create child cache");
        let lock_path = dir.join(format!("{hash}-{filename}.lock"));
        let lock_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)
            .expect("open child cache lock");
        let mut lock = fd_lock::RwLock::new(lock_file);
        let _writer = lock.write().expect("hold child cache lock");
        fs::write(
            dir.join(format!("{hash}-{filename}.materializing.tmp")),
            b"partial",
        )
        .expect("write interrupted runtime");
        fs::write(ready, b"ready").expect("signal held lock");
        loop {
            std::thread::park();
        }
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
    let _cleanup = CacheCleanup(cache_dir.clone());
    let workers = env::var(WORKERS_ENV)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(16);
    assert!(
        workers >= 2,
        "cold-cache startup requires at least two workers"
    );

    let mut children = Vec::new();
    for index in 0..workers {
        let child = startup_child()
            .env("VELR_CACHE_DIR", &cache_dir)
            .spawn()
            .unwrap_or_else(|err| panic!("spawn child {index}: {err}"));
        children.push((index, ChildGuard(Some(child))));
    }

    let mut failures = Vec::new();
    for (index, child) in children {
        let output = child.wait_with_output();
        if !output.status.success() {
            failures.push(format!(
                "child {index} failed with status {}\nstdout:\n{}\nstderr:\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ));
        }
    }

    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}

#[test]
fn cold_cache_recovers_after_writer_process_is_killed() {
    let cache_dir = temp_cache_dir("killed-writer");
    let _cleanup = CacheCleanup(cache_dir.clone());
    let ready = cache_dir.join("writer-ready");
    let child = startup_child()
        .env("VELR_CACHE_DIR", &cache_dir)
        .env(LOCK_READY_ENV, &ready)
        .spawn()
        .expect("spawn lock holder");
    let mut holder = ChildGuard(Some(child));
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready.is_file() {
        if holder
            .0
            .as_mut()
            .expect("holder")
            .try_wait()
            .expect("inspect holder")
            .is_some()
        {
            let output = holder.wait_with_output();
            panic!(
                "lock holder exited before readiness: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        assert!(
            Instant::now() < deadline,
            "lock holder did not signal readiness"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    holder
        .0
        .as_mut()
        .expect("holder")
        .kill()
        .expect("kill lock holder");
    holder
        .0
        .as_mut()
        .expect("holder")
        .wait()
        .expect("reap lock holder");

    let child = startup_child()
        .env("VELR_CACHE_DIR", &cache_dir)
        .spawn()
        .expect("spawn recovering writer");
    let output = ChildGuard(Some(child)).wait_with_output();
    assert!(
        output.status.success(),
        "startup after killed writer failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let temporary_files = fs::read_dir(cache_dir.join("velr/runtime"))
        .expect("read recovered cache")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().map_or(false, |ext| ext == "tmp"))
        .count();
    assert_eq!(
        temporary_files, 0,
        "interrupted runtime copy was not removed"
    );
}
