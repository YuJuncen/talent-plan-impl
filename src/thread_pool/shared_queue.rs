use std::collections::VecDeque;
use std::thread;

use crossbeam_channel::{Receiver, Sender, unbounded};
use log::error;

use crate::Result;
use crate::thread_pool::pool::ThreadPool;
use crate::thread_pool::shared_queue::MasterMessage::TaskDone;

struct Delayed(Option<Box<dyn FnOnce()>>);

impl Drop for Delayed {
    fn drop(&mut self) {
        (self.0.take().unwrap())()
    }
}

impl Delayed {
    fn new(f: Box<dyn FnOnce()>) -> Self {
        Delayed(Some(f))
    }
}

macro_rules! delay {
    ($($fun: stmt)*) => {
        let __delayed__ = Delayed::new(Box::new(move || {
            $($fun)*
        }));
    }
}

type Task = Box<dyn FnOnce() + 'static + Send>;

enum MasterMessage {
    NewTask(Task),
    Terminate(Sender<()>),
    TaskDone(WorkerBroker),
    GracefulShutdown(Sender<()>),
    Panicked,
}

enum WorkerMessage {
    RunTask(Task),
    Terminate,
}

#[derive(Clone)]
struct WorkerBroker(Sender<WorkerMessage>);

#[derive(Debug, Clone, Eq, PartialEq, Copy)]
enum PoolState {
    Running,
    Terminating {
        ended_workers: usize
    },
    GracefulShutdown,
}

impl PoolState {
    fn incr_ended_workers(&mut self) {
        match self {
            PoolState::Terminating { ended_workers } => {
                *ended_workers += 1;
            }
            _ => panic!("Fetal: incr_ended_workers on illegal state.")
        }
    }

    fn get_ended_workers(&self) -> usize {
        match self {
            PoolState::Terminating { ended_workers } => *ended_workers,
            _ => panic!("Fetal: incr_ended_workers on illegal state.")
        }
    }

    fn is_terminating(&self) -> bool {
        match self {
            PoolState::Terminating { .. } | PoolState::GracefulShutdown => true,
            _ => false
        }
    }
}

struct ThreadMaster {
    waiting: VecDeque<Task>,
    idle_workers: VecDeque<WorkerBroker>,
    pool_size: usize,
    state: PoolState,
    terminate_hook: Option<Sender<()>>,
}

#[derive(Clone)]
/// An implementation of `ThreadPool`,
/// It maintains a `Sender` to the real worker -- `ThreadMaster`.
/// This thread pool uses two shared queues: (task)waiting queue and idle worker queue,
/// every worker will send back a message when they finish their work, so that we can schedule a new work to it,
/// or push it into idle worker queue.
pub struct SharedQueueThreadPool(Sender<MasterMessage>);

impl SharedQueueThreadPool {
    /// Shutdown the pool asynchronously, all pending task won't be executed.
    /// But running tasks will keep on going, when they are done,
    /// we will send the `Terminate` message to its worker.
    pub fn shutdown(&self) -> Receiver<()> {
        let (s, r) = unbounded();
        self.0.send(MasterMessage::Terminate(s)).unwrap();
        r
    }

    /// Shutdown the pool asynchronously, the pool will not receive task more.
    /// But, all tasks submitted, even queuing, will be executed.
    /// When the waiting task queue becoming empty, our state will transform to `Terminating`.
    pub fn graceful_shutdown(&self) -> Receiver<()> {
        let (s, r) = unbounded();
        self.0.send(MasterMessage::GracefulShutdown(s)).unwrap();
        r
    }
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        self.graceful_shutdown();
    }
}

impl ThreadPool for SharedQueueThreadPool {
    fn spawn<R>(&self, runnable: R) where
        R: 'static + Send + FnOnce() {
        self.0.send(MasterMessage::NewTask(Box::new(runnable))).unwrap();
    }

    fn new(size: usize) -> Result<Self> {
        Ok(ThreadMaster::new(size).start_work())
    }
}

impl ThreadMaster {
    fn new(pool_size: usize) -> Self {
        ThreadMaster {
            waiting: VecDeque::new(),
            idle_workers: VecDeque::new(),
            state: PoolState::Running,
            terminate_hook: None,
            pool_size,
        }
    }

    fn send_terminate(&mut self) {
        if let Some(hook) = self.terminate_hook.take() {
            hook.send(()).unwrap();
        }
    }

    fn start_work(mut self) -> SharedQueueThreadPool {
        let (this, mail_box) = unbounded();
        (0..self.pool_size).for_each(|_| { self.idle_workers.push_back(WorkerBroker::new(this.clone())) });
        let this2 = this.clone();
        thread::Builder::new()
            .name("shared-queue-thread-pool-master".to_owned())
            .spawn(move || {
                for message in mail_box.iter() {
                    if !self.handle_message(message, this2.clone()) { break; }
                }
            })
            .unwrap();
        SharedQueueThreadPool(this)
    }

    fn handle_message(&mut self, message: MasterMessage, this: Sender<MasterMessage>) -> bool {
        use MasterMessage::*;
        match message {
            NewTask(task) => {
                if self.state.is_terminating() {
                    error!(target: "app::error", "Trying to spawn a work to a terminated executor.");
                    // 比起导致可能的线程泄漏，我们在此处选择更加稳妥的解决方案——继续等待直到它自己结束。
                    return false;
                }
                self.new_task(task)
            }
            TaskDone(broker) => {
                match self.state {
                    PoolState::GracefulShutdown => {
                        self.new_broker(broker);
                        if self.waiting.is_empty() {
                            this.send(MasterMessage::Terminate(self.terminate_hook.take().unwrap())).unwrap();
                        }
                    }
                    PoolState::Terminating { .. } => {
                        broker.unsafe_terminate();
                        self.state.incr_ended_workers();
                        if self.state.get_ended_workers() == self.pool_size {
                            self.send_terminate();
                            return true;
                        }
                    }
                    PoolState::Running => {
                        self.new_broker(broker)
                    }
                }
            }
            Terminate(ret) => {
                // 让这个操作成为幂等的，防止意外的状态转移。
                if let PoolState::Terminating { .. } = self.state {
                    return false;
                }

                self.state = PoolState::Terminating { ended_workers: 0 };
                while let Some(worker) = self.idle_workers.pop_front() {
                    worker.unsafe_terminate();
                    self.state.incr_ended_workers();
                }
                self.terminate_hook = Some(ret);
                if self.pool_size == self.state.get_ended_workers() {
                    self.send_terminate();
                    return true;
                }
            }
            Panicked => {
                if !self.state.is_terminating() {
                    error!("One worker panicked, we are recruiting a new now!");
                    let broker = WorkerBroker::new(this.clone());
                    self.new_broker(broker);
                } else {
                    error!("One worker panicked in the dying executor, what to do...?");
                    match self.state {
                        PoolState::GracefulShutdown => {
                            let broker = WorkerBroker::new(this.clone());
                            self.new_broker(broker);
                            if self.waiting.is_empty() {
                                this.send(MasterMessage::Terminate(self.terminate_hook.take().unwrap())).unwrap();
                            }
                        }
                        PoolState::Terminating { .. } => {
                            self.state.incr_ended_workers();
                            if self.state.get_ended_workers() == self.pool_size {
                                self.send_terminate();
                                return true;
                            }
                        }
                        _ => unreachable!()
                    }
                }
            }
            GracefulShutdown(ret) => {
                if self.state.is_terminating() {
                    return false;
                }

                self.state = PoolState::GracefulShutdown;
                self.terminate_hook = Some(ret);

                if self.waiting.is_empty() {
                    this.send(MasterMessage::Terminate(self.terminate_hook.take().unwrap())).unwrap();
                }
            }
        }
        true
    }


    #[inline]
    fn new_task(&mut self, task: Task) -> () {
        if let Some(worker) = self.idle_workers.pop_front() {
            worker.unsafe_send_task(task);
        } else {
            self.waiting.push_back(task);
        }
    }

    #[inline]
    fn new_broker(&mut self, broker: WorkerBroker) {
        if let Some(task) = self.waiting.pop_front() {
            broker.unsafe_send_task(task);
        } else {
            self.idle_workers.push_back(broker);
        }
    }
}

impl WorkerBroker {
    fn new(master: Sender<MasterMessage>) -> Self {
        let (s, r) = unbounded::<WorkerMessage>();
        let worker = WorkerBroker(s);
        let abroad_worker = worker.clone();
        thread::Builder::new()
            .name("shared-queue-thread-pool-worker".to_owned())
            .spawn(move || {
                let master2 = master.clone();
                delay! {
                    if thread::panicking() {
                        master2.send(MasterMessage::Panicked).unwrap();
                    }
                }
                ;
                // 这儿我们可以放心地让这个线程 Panic。
                loop {
                    let message = r.recv().unwrap();
                    match message {
                        WorkerMessage::RunTask(task) => {
                            task();
                            master.send(TaskDone(abroad_worker.clone())).unwrap();
                        }
                        WorkerMessage::Terminate => break
                    };
                }
            })
            .unwrap();
        worker
    }

    #[inline]
    fn unsafe_send_task(&self, task: Task) {
        self.0.send(WorkerMessage::RunTask(task)).unwrap();
    }

    #[inline]
    fn unsafe_terminate(&self) {
        self.0.send(WorkerMessage::Terminate).unwrap();
    }
}
