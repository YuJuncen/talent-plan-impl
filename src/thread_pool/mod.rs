pub use pool::ThreadPool;
pub use shared_queue::SharedQueueThreadPool;
pub use trivial::NaiveThreadPool;

pub use self::rayon::RayonThreadPool;

mod pool;
mod trivial;
mod shared_queue;
mod rayon;