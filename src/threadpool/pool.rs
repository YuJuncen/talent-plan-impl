/// the common abstraction of a thread pool.
pub trait ThreadPool {
    fn spawn<R>(runnable: R) where
        R: 'static + Send + FnOnce();
}