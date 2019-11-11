use rayon::ThreadPool;

use crate::Result;

/// the `ThreadPool` implementation using the `rayon` thread pool.
pub struct RayonThreadPool(ThreadPool);

impl crate::thread_pool::ThreadPool for RayonThreadPool {
    fn spawn<R>(&self, runnable: R)
        where
            R: 'static + Send + FnOnce(),
    {
        self.0.spawn(runnable)
    }

    fn new(size: usize) -> Result<Self> {
        let inner = rayon::ThreadPoolBuilder::new().num_threads(size).build()?;
        Ok(RayonThreadPool(inner))
    }
}
