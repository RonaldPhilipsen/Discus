use std::io;
use std::sync::OnceLock;

static FIRST_FAILURE: OnceLock<io::Error> = OnceLock::new();

/// The nice value for the data runtime threads: the process value plus `nice`, capped at 19.
/// `None` when `nice` is 0 or the process value cannot be read; the read failure is recorded.
///
/// Call it on the main thread before the runtime starts. A Linux thread inherits the nice value of
/// the thread that creates it, and tokio creates blocking threads from worker threads, so a value
/// relative to the current thread would compound.
pub fn data_thread_target(nice: i32) -> Option<i32> {
    if nice == 0 {
        return None;
    }
    match current_thread_nice() {
        Ok(base) => Some(base.saturating_add(nice).min(19)),
        Err(e) => {
            record(&FIRST_FAILURE, Err(e));
            None
        }
    }
}

/// Set the calling thread to `target` and remember the first failure of the process. The runtime
/// starts its workers before the tracing subscriber exists, so `main` reads the failure later with
/// `first_failure` and logs it once.
pub fn set_current_thread_nice_recorded(target: i32) {
    record(&FIRST_FAILURE, set_current_thread_nice(target));
}

pub fn first_failure() -> Option<&'static io::Error> {
    FIRST_FAILURE.get()
}

fn record(slot: &OnceLock<io::Error>, result: io::Result<()>) {
    if let Err(e) = result {
        let _ = slot.set(e);
    }
}

/// The nice value of the calling thread. Nice is per thread on Linux. Elsewhere this is 0.
#[cfg(target_os = "linux")]
pub fn current_thread_nice() -> io::Result<i32> {
    // SAFETY: gettid takes no arguments and cannot fail.
    let tid = unsafe { libc::gettid() } as libc::id_t;
    // getpriority can return -1 as a valid value, so errno tells the two cases apart.
    // SAFETY: __errno_location returns a valid pointer to this thread's errno.
    unsafe { *libc::__errno_location() = 0 };
    // SAFETY: getpriority reads only its integer arguments.
    let current = unsafe { libc::getpriority(libc::PRIO_PROCESS, tid) };
    let err = io::Error::last_os_error();
    if current == -1 && err.raw_os_error() != Some(0) {
        return Err(err);
    }
    Ok(current)
}

#[cfg(not(target_os = "linux"))]
pub fn current_thread_nice() -> io::Result<i32> {
    Ok(0)
}

/// Set the nice value of the calling thread to `target`, capped at 19. A thread that already runs
/// at `target` or lower priority keeps its value: raising the priority again needs a privilege.
/// The other threads of the process keep their priority. Elsewhere than Linux this does nothing.
#[cfg(target_os = "linux")]
pub fn set_current_thread_nice(target: i32) -> io::Result<()> {
    let target = target.min(19);
    if current_thread_nice()? >= target {
        return Ok(());
    }
    // SAFETY: gettid takes no arguments and cannot fail.
    let tid = unsafe { libc::gettid() } as libc::id_t;
    // SAFETY: setpriority reads only its integer arguments.
    if unsafe { libc::setpriority(libc::PRIO_PROCESS, tid, target) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn set_current_thread_nice(_target: i32) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod record_tests {
    use super::*;

    #[test]
    fn keeps_the_first_failure_only() {
        let slot = OnceLock::new();
        record(&slot, Ok(()));
        assert!(slot.get().is_none());
        record(&slot, Err(io::Error::other("first")));
        record(&slot, Err(io::Error::other("second")));
        assert_eq!(slot.get().unwrap().to_string(), "first");
    }

    #[test]
    fn zero_nice_has_no_target() {
        assert_eq!(data_thread_target(0), None);
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn lowers_only_the_calling_thread() {
        let before = current_thread_nice().unwrap();
        let target = data_thread_target(5).unwrap();
        assert_eq!(target, (before + 5).min(19));
        let t = std::thread::spawn(move || {
            set_current_thread_nice(target).unwrap();
            current_thread_nice().unwrap()
        });
        assert_eq!(t.join().unwrap(), target);
        assert_eq!(current_thread_nice().unwrap(), before);
    }

    #[test]
    fn a_thread_created_by_a_lowered_thread_ends_at_base_plus_nice() {
        let base = current_thread_nice().unwrap();
        let target = data_thread_target(5).unwrap();
        let t = std::thread::spawn(move || {
            set_current_thread_nice(target).unwrap();
            std::thread::spawn(move || {
                set_current_thread_nice(target).unwrap();
                current_thread_nice().unwrap()
            })
            .join()
            .unwrap()
        });
        assert_eq!(t.join().unwrap(), (base + 5).min(19));
    }

    #[test]
    fn never_raises_the_priority_of_a_lower_thread() {
        let base = current_thread_nice().unwrap();
        let t = std::thread::spawn(move || {
            set_current_thread_nice(base + 3).unwrap();
            set_current_thread_nice(base + 1).unwrap();
            current_thread_nice().unwrap()
        });
        assert_eq!(t.join().unwrap(), (base + 3).min(19));
    }
}
