use crate::Result;

/// the common abstraction of a thread pool.
pub trait ThreadPool: Sized {
    /// like `thread::spawn`, spawn an thread into this pool.
    fn spawn<R>(&self, runnable: R) where
        R: 'static + Send + FnOnce();
    /// create an new thread pool with specified size.
    fn new(size: usize) -> Result<Self>;
}