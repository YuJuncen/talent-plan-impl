# PingCAP Talent Plan implementation

这是我在 `TiKV` 方向的一个实现。  

我曾经用 `actix` 和 `tokio` 半娱乐地做过一个小型的 `Rust`项目：我的博客后端，因此对 `Rust` 还算有些基础；我很喜欢这门有些奇特的语言——它有着非常深厚的命令式血脉、精致的内存管理、许多前沿的语言特性、用 `Prelude` 做常用模块的名字（因为……我个人对编程世界的向往大抵滥觞于 `Haskell`……）……

## 项目

这是一个键值数据库。

它抽象了存储引擎，所以可以运行在多个底层实现上。

### 快速上手
这个项目提供两个控制台接口： `kvs-server ` 和  `kvs-client` ，其用途恰如其名。
#### 服务端

在项目中，可以使用 `cargo` 直接运行：

```bash
# to start server
cargo run --bin kvs-server
```
这个命令会用默认的 `kvs` 引擎来启动  `kvs-server` ，跑在默认的 `localhost:4000` 上。

你可以使用 `--help` 命令了解更多。

#### 客户端
```bash
# to get value of $KEY_NAME.
cargo run --bin kvs-client -- get $KEY_NAME
# to set key $KEY_NAME as $KEY_VALUE.
cargo run --bin kvs-client -- set $KEY_NAME $VALUE
# to remove key $KEY_NAME.
cargo run --bin kvs-client -- rm $KEY_NAME
```
以上所有操作都会试着连接默认的服务器地址 `localhost:4000`。

你可以使用 `--help` 命令了解更多。

## 实现时的笔记

### project 1

工作非常简单——仅仅是温习了一下 `rust` 的模块机制还有相关的工具链。顺便感慨一下 `async/await` 终于提上日程（我上一个项目的异步编程完全就是灾难！）。  

### project 2

一些新的问题开始呈现。  

最显著的问题是文件 IO 相关的多重难题：我开始疑惑为何 `Read` 会需要可变的借用：这样 `get` 方法也必须获得自身的可变引用，这在多线程的环境下可以说是灾难性的——因为只能有一个线程获得可变引用。  

我尝试使用内部可变性还有分离读写的句柄来解决这个问题；然后很快发现事情比想象的更加复杂：而且因为可变借用的唯一性，连仅有的 race condition 都不存在了，索性放飞自我了——在压缩的时候，按照索引创建新的文件，然后直接 `fs::copy` 了事，反正没有人可以在我们写文件的时候读。  

### project 3

后来，我逐渐发现 `rust` 的安全性比我想象的要高许多——数据结构即便在线程之间被共享，也会需要遵循最基本的借用规则；同时通过借用来在线程之间共享数据比想象的会艰难不少——因为线程的生存时间并不确定，所以我们几乎无法向 `rustc` 证明我们的借用是安全的（即便在它消失之前调用 `join` 也不行，`rustc` 就是这样，不是 `'static` 的都不可以！）。

于是实现 `Clone` 成为了在线程之间共享数据的最好方法——熟悉 `Java` 的人可能很快就会开始嘲讽我们“重新发明了 `ThreadLocal`”——看起来性质差不多，但是还是有些许不同，至少我们可以用 `Arc` 来实现类似 `Java` 中的变量共享风格。

使用 `Arc` 之后还有更加棘手的事情：`Arc` 仅仅实现了 `Deref`，它无法改变被共享的值——最终，我们还是要面对内部可变性（哎！），此处我们使用锁（而非 `RefCell`）解决。  

接下来呢？即便使用内部可变性和 `Arc` 共享句柄，我们仍旧没有办法同时读多个文件——可变借用说到底还是只能有一个。这个问题的解决方案相当平凡——不再使用 `Arc` 或者内部可变性来共享文件，而是实现 `Clone`，为每一个线程中的实例重新打开一次文件，剩下的交给操作系统还有硬件去调度吧（说不定还可以利用 SSD 的并行机制）。

另一件非常有趣的事情是，在单线程的 benchmark 中，我们乱拳打死老师傅了：

```
kvs                     time:   [1.4274 s 1.4436 s 1.4634 s]                 
                        change: [-3.0782% -0.1901% +2.6514%] (p = 0.91 > 0.05)
                        No change in performance detected.

sled                    time:   [1.5484 s 1.5963 s 1.6223 s]  
```

原因（我猜的）大概是因为 `sled` 使用了某种 `LSM Tree` 作为索引（因为它要支持高效的区间查询），所以具有更大的写放大，同时若读取的键不在 L0 缓存中，或者根本不存在，则会拥有很大的读放大；而我们使用的哈希索引，在内存中足以存放索引、又不需要支持区间查询的前提下，能够拥有还算不错的性能；同时由于测试是单线程的，所以我们的实现那灾难般的并发劣势也没有显现出来。

还有值得一提的是，我们在服务器端使用了 `Tokio` 实现——这事实上并没有什么意义，因为硬盘的 IO 会阻塞掉协程；所以事实上，我们的工作性质接近于单纯地重用了 `Tokio` 中实现的线程池。

接下来我们还要做更多——暂时让把 `Tokio` 放一放，来实现我们的线程池吧。

### project 4 其一

我们现在需要用到两件重要的工具：

- `Arc`，用来在线程之间共享内存。
- `RwLock`、`Mutex`、`RefCell`，用来实现内部可变性。

我们遇到的第一个问题是 `Clone` 和 Trait Object 的阻抗失配，因为 Trait Object 使用和 `C++` 虚表不同的胖指针——如果某个 Trait 要求 `Self: Sized`，则我们无法将其封装为 Trait Object。所以，在启动服务器的时候，我们只能逐一进行模式匹配……

逐一模式匹配会产生大量重复代码，我们很快就会嗅到许多“坏味道”。利用宏来重构它是更加好的解决方案，但是如今看起来，似乎也没啥必要。

> 我们最终在 `Project 4` 中写出了 `with_pool!` 和 `with_engine!`。

线程池的实现遇到的问题相对来讲不多；仅仅是“有些麻烦”——我们需要在 `Rust` 中模拟 `Erlang` 那般的消息传递机制（`Worker` 和 `Master` 的表现相当接近 `Erlang` 中的线程——接受消息，而后同步或者异步地返回），但是我们却仅有如同 `Go` 语言般的信道，二者虽然颇为相似，但是确实有些微妙的差别——因为并非每一个发送者都有一个 `Pid`，要模拟同步调用的话需要一些额外的抽象，大概长这样：

```rust
fn spawn<R>(&mut self, runnable: R) where
	R: Send + 'static + FnOnce() {
  // Here, we are creating the channel explicitly.
	let (s, r) = unbounded();
	self.pool.send(RunTask(Box::new(runnable), 
    // here, is the `sender` for synchronous call
    s));
  if let Err(_) = r.recv() {
    panic!("Failed to start thread...");
  }
}
```

而在 `Erlang` 一类的 `Actor System` 中，创建信道的过程可以被省略，而后我们的代码可能长这样。

```erlang
poolSwpan(Pool, Task) ->
	Pool ! [newtask, Task, self()],
  receive 
    ok -> ok;
    [err, Message] -> [err, Message]
 	end.
```

不过就最终的接口来看，我们还是会将一切协议都封装起来的，所以反倒不是什么大事，仅仅是麻烦罢了。

在实现 benchmark 的时候，我们遇到的问题则是如何选择线程池还有引擎。最后解决方案则是直接写了请求远端服务器的 `KvEngine` 实现——`RemoteEngine`。

写 benchmark 的工作拖了很久——完全就是因为懒。而且因为我的电脑配置不够给力，我人为降低了一下样本的数量。

这次我们在 `sled` 面前败下阵来——`sled` 会比我们快大概 10% 左右。但是引擎对速度的影响却远不及线程池对速度的影响——我们的线程池大概会比 `rayon` 线程池慢 50% 上下。

### project 4 其二

另一个有些困难的事情就是让读不会阻塞写。

有一个很棒的优势——我们的文件 IO 是 Append-only 的。这意味着，写入索引的文件在理想状况下是不会改变的。

但是现实世界中充满了各种意外。我们不可能真的让文件用不改变——如果那样，硬盘会被只进不出的数据塞满。我们需要清理不用的数据。因此，写会被读阻塞——在过去的代码中，体现为无法获得 `index` 上的 `Write` 锁。

简单地使用线程安全的 `HashMap` 可以解决这个问题，但是对底层文件的 IO 种下的不安之种终将长出恶果——因为压缩期间会重写文件，内存中索引和文件的一致性被打破，这时候可能会给出错误的响应。而由于文件是用进程为粒度加锁的，`rust` 的机制无法阻止 `fs::remove_file` 的调用，如是，引进了不确定的行为。

方案之一是在使用线程安全的 `HashMap` 的同时，让压缩时的所有写入仅仅写入新的文件中；而后在压缩完成之后再一并将其融合。这样做仅仅只能一部分解决问题——而后，在将文件“偷梁换柱”的时候，我们还是要等待读者们离去。

有没有什么办法能够不用等待？假如我们只是删掉了原先的文件，那么它的每个句柄都会成为定时炸弹。可以考虑的做法是，让每一个读者在执行读指令之前，自觉地尝试放弃过期的文件。由此可以放心地移除文件，仅仅需要通过某些机制“通知”读者即可。

使用文件的“年代”来通知读者是一个办法：让每个线程中的读者维护一个共同的年代信息。同时在合适的时候自己关闭过时的句柄；但是，这个信息又该如何维护？如何定义“过老”？。

还有一些别的问题，我们对外似乎暴露了过多的 API，即便我们往往通过接口使用它。将这些接口抽象到 `Writer` 和 `Reader` 中去或许会是更好的选择……

### project 4 其三

在那之前，我们继续探索无锁的算法。我们需要作出决定：保留多少代文件？假如永远都只保留最新的文件，那么在删掉文件的时候，可能仍旧有上一代的读者在读取对应的数据。保留最近两代可能是合理的选择。

另一个选项是，在删除文件之前，通知每一个读者立刻执行放弃句柄的逻辑，这时候，文件可能会被关闭，异步执行的 IO 可能会触发异常……在一般的语言中是这样的，但是 `rust` 为我们保证了安全。如果在有人阅读文件的时候尝试关闭句柄，一般情况下是无法通过编译的。但是，问题仍旧存在，压缩机制在此时仍旧会被阻塞一小会儿——它需要等待为数不多（往往没有！）的已经在读着文件的读者们，因为读者会获取 `ConcHashMap` 中对应位置的锁。

读者释放锁的时机也值得考虑：如果读者在读文件之前就将锁释放了，那么写入在保证一致性的前提下可以等待更加短的时间；但是删除文件的时候却又会造成额外的风险；`epoch` 选择为每一个线程加上一个 `active` 位，我们用一种更加暴力的方法：“物化”一组 `RWLock`，让每一次读为那一代文件加上一个共享锁。而**仅仅**在删除的时候，我们会尝试获得那个文件上的排他锁。

另一种可能的办法是，延迟删除文件，在 `Reader`群中维护一个 `Map<u64, Atomic<u64>>` 的引用计数器，用来标记每条线程到各个时代的引用，每次在尝试读的时候，都主动放弃已经过时的文件，降低引用计数器，如果其值为`0`，那么我们便可以删除之，此时需要额外的方式来确定何时值是“过时”的；此处定义 `tail_epoch`，这个值由 `compact_file` 所衍生的线程增长，我们能够保证，`epoch` 小于这个值的文件绝不存在于索引內。

> 后来我发现这个方法不太可行，因为如果 `Reader` 被直接 `drop` 了，我们会遇到一些棘手的问题……于是我们仍旧选择在读的时候获取“物化”的 `RwLock` 的读锁。进入这个锁会有额外的开销……或许会有更加好的办法……吗？

我们需要在 `BinLocation` 中增加一个新字段——`epoch`，表示文件所在的纪元。同时需要维护一个变量`current_epoch`，用来帮助写者创建下一个纪元的文件。这个字段可能会在 `create_index` 中被初始化。读者可能不需要这个标志，但是写者可能需要。

读者如今需要维护一个纪元到文件句柄的映射；用来处理 `BinLocation` 的额外维度。

还有另一个小技巧可以允许压缩在后台异步进行：我们让每次纪元增加量为 `2`，其中第一个纪元用来存储压缩后的记录，第二个则马上可以接受新的写入了。

压缩完成之后，我们可以马上把 `tail_epoch` 加上 `2`，而后读者就可以在合适的时候清理文件了。