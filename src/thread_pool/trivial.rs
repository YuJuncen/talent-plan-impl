use crate::Result;

use super::pool::ThreadPool;

pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn spawn<R>(&self, runnable: R) where
        R: 'static + Send + FnOnce() {
        std::thread::spawn(runnable);
    }

    fn new(_n: usize) -> Result<Self> {
        Ok(NaiveThreadPool)
    }
}