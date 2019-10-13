use super::pool::ThreadPool;

pub struct TrivialThreadPool;

impl TrivialThreadPool {
    fn new() -> Self {
        TrivialThreadPool
    }
}

impl ThreadPool for TrivialThreadPool {
    fn spawn<R>(runnable: R) where
        R: 'static + Send + FnOnce() {
        std::thread::spawn(runnable);
    }
}