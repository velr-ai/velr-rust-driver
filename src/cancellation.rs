use std::fmt;
use std::ptr::NonNull;
use std::sync::{Arc, Mutex, Weak};

use crate::{api, ffi, Error, QueryOptions, Result};

/// A thread-safe signal that requests cancellation of queries.
///
/// Clones share the same signal. Canceling the token also cancels future uses
/// of that token. Give independent operations separate tokens.
#[derive(Clone, Default)]
pub struct CancellationToken {
    state: Arc<Mutex<CancellationState>>,
}

#[derive(Default)]
struct CancellationState {
    cancelled: bool,
    active: Vec<Weak<NativeQueryControl>>,
}

impl fmt::Debug for CancellationToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancellationToken").finish_non_exhaustive()
    }
}

impl PartialEq for CancellationToken {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }
}

impl Eq for CancellationToken {}

impl CancellationToken {
    /// Create a fresh, unsignaled token.
    pub fn new() -> Self {
        Self::default()
    }

    /// Request cancellation of queries using this token.
    ///
    /// Call this from any thread. The running operation reports whether it
    /// stopped or finished before observing the signal. Check errors from both
    /// execution and result fetching. Every clone stays canceled after this call.
    pub fn cancel(&self) {
        let active = {
            let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
            state.cancelled = true;
            std::mem::take(&mut state.active)
        };
        for query in active.iter().filter_map(Weak::upgrade) {
            query.cancel();
        }
    }

    fn attach(&self, query: &Arc<NativeQueryControl>) {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        if state.cancelled {
            query.cancel();
        } else {
            state.active.retain(|query| query.strong_count() > 0);
            state.active.push(Arc::downgrade(query));
        }
    }
}

pub(crate) struct NativeQueryControl {
    ptr: NonNull<ffi::velr_query_control>,
    cancel: unsafe extern "C" fn(*const ffi::velr_query_control),
    free: unsafe extern "C" fn(*mut ffi::velr_query_control),
}

// The native control can be signaled from any thread. The Arc keeps its handle
// alive throughout a call to cancel, including while a result is being dropped.
unsafe impl Send for NativeQueryControl {}
unsafe impl Sync for NativeQueryControl {}

impl NativeQueryControl {
    pub(crate) fn prepare(a: &api::Api, options: &QueryOptions) -> Result<Option<Arc<Self>>> {
        if options.timeout.is_none() && options.cancellation.is_none() {
            return Ok(None);
        }

        let new = require_control_api(a.velr_query_control_new)?;
        let cancel = require_control_api(a.velr_query_control_cancel)?;
        let free = require_control_api(a.velr_query_control_free)?;
        // Zero means "no deadline" in the ABI. A zero-duration timeout expires
        // immediately.
        let timeout_ns = options.timeout.map_or(0, |timeout| {
            timeout.as_nanos().clamp(1, u64::MAX as u128) as u64
        });
        let ptr = NonNull::new(unsafe { new(timeout_ns) }).ok_or_else(|| {
            Error::new(
                ffi::velr_code::VELR_EERR as i32,
                "could not create query cancellation control",
            )
        })?;
        let query = Arc::new(Self { ptr, cancel, free });
        if let Some(token) = &options.cancellation {
            token.attach(&query);
        }
        Ok(Some(query))
    }

    pub(crate) fn as_ptr(&self) -> *const ffi::velr_query_control {
        self.ptr.as_ptr()
    }

    fn cancel(&self) {
        unsafe { (self.cancel)(self.ptr.as_ptr()) };
    }
}

impl Drop for NativeQueryControl {
    fn drop(&mut self) {
        unsafe { (self.free)(self.ptr.as_ptr()) };
    }
}

pub(crate) fn require_control_api<T>(api: Option<T>) -> Result<T> {
    api.ok_or_else(|| {
        Error::new(
            ffi::velr_code::VELR_ESTATE as i32,
            "loaded Velr runtime does not support query cancellation",
        )
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::CancellationToken;
    use crate::{velr_api, CellRef, ErrorKind, QueryOptions, Velr};

    fn runtime_supports_query_control() -> bool {
        let supported = velr_api().unwrap().velr_query_control_new.is_some();
        let required = std::env::var_os("VELR_REQUIRE_QUERY_CONTROL").as_deref()
            == Some(std::ffi::OsStr::new("1"));
        if required || std::env::var_os("VELR_RUNTIME_PATH").is_some() {
            assert!(supported, "selected runtime lacks query cancellation");
        }
        // Published driver bundles may load a runtime from before query control.
        supported
    }

    #[test]
    fn stops_work_during_execution() {
        if !runtime_supports_query_control() {
            return;
        }
        let db = Velr::open(None).unwrap();
        let query =
            "UNWIND range(1, 10000) AS a UNWIND range(1, 10000) AS b RETURN sum(a * b) AS n";
        let error = db
            .run_with_options(
                query,
                QueryOptions::new().with_timeout(Duration::from_millis(40)),
            )
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DeadlineExceeded, "{error}");

        let token = CancellationToken::new();
        std::thread::scope(|scope| {
            let worker_token = token.clone();
            let cancel = scope.spawn(move || {
                std::thread::sleep(Duration::from_millis(40));
                worker_token.cancel();
            });
            let error = db
                .run_with_options(
                    query,
                    QueryOptions::new()
                        .with_cancellation(token)
                        .with_timeout(Duration::from_secs(5)),
                )
                .unwrap_err();
            cancel.join().unwrap();
            assert_eq!(error.kind(), ErrorKind::Cancelled, "{error}");
        });

        db.run("RETURN 1").unwrap();
    }

    #[test]
    fn preserves_control_through_deferred_rows_and_transaction_tables() {
        if !runtime_supports_query_control() {
            return;
        }
        let db = Velr::open(None).unwrap();
        let query = "UNWIND [1, 2, 3] AS n RETURN n";
        {
            let mut stream = db
                .exec_with_options(
                    query,
                    QueryOptions::new().with_timeout(Duration::from_millis(300)),
                )
                .unwrap();
            let mut table = stream.next_table().unwrap().unwrap();
            let mut rows = table.rows().unwrap();
            assert!(rows.next(|_| Ok(())).unwrap());
            std::thread::sleep(Duration::from_millis(300));
            let error = rows.next(|_| Ok(())).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::DeadlineExceeded, "{error}");
        }

        let tx = db.begin_tx().unwrap();
        let token = CancellationToken::new();
        {
            let mut table = tx
                .exec_one_with_options(query, QueryOptions::new().with_cancellation(token.clone()))
                .unwrap();
            let mut rows = table.rows().unwrap();
            assert!(rows.next(|_| Ok(())).unwrap());
            std::thread::spawn(move || token.cancel()).join().unwrap();
            let error = rows.next(|_| Ok(())).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Cancelled, "{error}");
        }
        tx.rollback().unwrap();
        db.run("RETURN 1").unwrap();
    }

    #[test]
    fn already_expired_or_cancelled_options_stop_writes() {
        if !runtime_supports_query_control() {
            return;
        }
        let db = Velr::open(None).unwrap();
        let token = CancellationToken::new();
        token.cancel();
        let tx = db.begin_tx().unwrap();
        let error = tx
            .run_with_options(
                "CREATE (:Interrupted)",
                QueryOptions::new().with_cancellation(token),
            )
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Cancelled, "{error}");

        let error = tx
            .run_with_options(
                "CREATE (:Interrupted)",
                QueryOptions::new().with_timeout(Duration::ZERO),
            )
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DeadlineExceeded, "{error}");
        tx.commit().unwrap();

        let mut result = db
            .exec_one("MATCH (n:Interrupted) RETURN count(n)")
            .unwrap();
        let counts = result
            .collect(|row| match row[0] {
                CellRef::Integer(value) => Ok(value),
                ref cell => panic!("expected integer count, got {cell:?}"),
            })
            .unwrap();
        assert_eq!(counts, [0]);
    }
}
