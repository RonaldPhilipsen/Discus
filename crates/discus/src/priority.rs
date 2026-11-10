use std::io;

/// Raise the nice value of the calling thread by `nice`, capped at 19. Nice is per thread on
/// Linux, so the other threads of the process keep their priority. Elsewhere this does nothing.
#[cfg(target_os = "linux")]
pub fn lower_current_thread(nice: i32) -> io::Result<()> {
    if nice == 0 {
        return Ok(());
    }
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
    let target = current.saturating_add(nice).min(19);
    // SAFETY: setpriority reads only its integer arguments.
    if unsafe { libc::setpriority(libc::PRIO_PROCESS, tid, target) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn lower_current_thread(_nice: i32) -> io::Result<()> {
    Ok(())
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn lowers_only_the_calling_thread() {
        let before = unsafe { libc::getpriority(libc::PRIO_PROCESS, 0) };
        let t = std::thread::spawn(|| {
            lower_current_thread(5).unwrap();
            unsafe { libc::getpriority(libc::PRIO_PROCESS, libc::gettid() as _) }
        });
        assert_eq!(t.join().unwrap(), (before + 5).min(19));
        assert_eq!(unsafe { libc::getpriority(libc::PRIO_PROCESS, 0) }, before);
    }
}
