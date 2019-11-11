use crate::Result;

use super::pool::ThreadPool;

/// The naïve thread pool implementation.
/// when `ThreadPool::spawn` called, it just simply spawn a new thread.
/// and never reuse them.
///
/// It's just a thread factory!
pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn spawn<R>(&self, runnable: R)
        where
            R: 'static + Send + FnOnce(),
    {
        std::thread::spawn(runnable);
    }

    fn new(_n: usize) -> Result<Self> {
        Ok(NaiveThreadPool)
    }
}
