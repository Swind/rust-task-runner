---
name: rust-base
description: How to use the rust_base workspace — the rust_task, rust_io, and rust_net crates that port Chromium's base/ threading and net/ I/O model to Rust (rust_net's optional `tls` feature adds async TLS via rustls). Use this whenever you are writing or reviewing code that imports rust_task, rust_io, or rust_net; whenever you see ThreadPool, SequencedTaskRunner, TaskRunner, TaskTraits, bind_once, RepeatingTimer, TaskMonitor, IoTaskRunner, FileProxy, FdWatcher, SocketPosix, TcpClientSocket, TcpServerSocket, StreamSocket, or TlsClientSocket; or whenever someone asks how to post tasks to a thread pool, schedule delayed/sequenced work, watch a file descriptor with epoll, do async file I/O, run an async TCP client/server, or do async TLS / HTTPS (rustls) in this repo. Reach for this skill even if the user only describes the behavior ("run this off the main thread", "fire a callback when the socket is readable", "make an https request") without naming a type.
---

# rust_base

`rust_base` is a workspace of three crates that port Chromium's threading and
I/O model to idiomatic Rust:

| Crate | What it gives you | Platform |
|-------|-------------------|----------|
| `rust_task` | Thread pool, task runners, sequencing, delayed tasks, shutdown lifecycle, monitoring | cross-platform (`std` only) |
| `rust_io`   | epoll event loop + async file I/O — **builds on `rust_task`** | Linux |
| `rust_net`  | Async TCP socket (client + server), `StreamSocket` trait, async TLS (`tls` feature) — **builds on `rust_io`** | Linux |

```
rust_task  ←── rust_io  ←── rust_net
```

**`rust_task` is the foundation and the focus of this page.** Master it first —
its task-runner and `bind_once` semantics carry directly into the other
crates. When the task at hand involves file descriptors, files, sockets, TLS, or
an epoll loop, jump to the reference for that layer:

- **Async file I/O or watching a raw fd with epoll** → read [`references/rust_io.md`](references/rust_io.md)
- **Async TCP client or server (and the `StreamSocket` trait)** → read [`references/rust_net.md`](references/rust_net.md)
- **Async TLS / HTTPS — `rust_net`'s `tls` feature (rustls)** → read [`references/rust_tls.md`](references/rust_tls.md)

Those references assume you already understand the `rust_task` concepts below,
so don't skip ahead.

---

## The mental model

Everything is **post a callback, get it run elsewhere**. You never spawn threads
by hand. You pick *where* and *with what guarantees* a closure runs:

- **`ThreadPool`** owns a fixed set of OS worker threads. It's the entry point.
- A **`TaskRunner`** (parallel) lets posted tasks run on any worker, possibly concurrently.
- A **`SequencedTaskRunner`** guarantees tasks run one-at-a-time in FIFO order — never concurrently with each other. This is how you protect shared state *without a mutex*: give each piece of state its own sequence.

Callbacks are `Box<dyn FnOnce() + Send + 'static>`. Post methods return `bool`
(`false` means the task was rejected, e.g. after shutdown).

## Getting started

```rust
use rust_task::{ThreadPool, TaskTraits, TaskRunner, SequencedTaskRunner};

let pool = ThreadPool::new(4);   // 4 worker threads; returns Arc<ThreadPool>

// Fire-and-forget on any worker:
pool.post_task(TaskTraits::default(), Box::new(|| println!("hello")));

// A sequenced runner — these two ALWAYS run in order, never overlapping:
let runner = pool.create_sequenced_task_runner(TaskTraits::default());
runner.post_task(Box::new(|| println!("first")));
runner.post_task(Box::new(|| println!("second")));

// A parallel runner — tasks may run concurrently:
let par = pool.create_task_runner(TaskTraits::default());

pool.shutdown();   // blocks until all BlockShutdown tasks finish (see below)
```

`ThreadPool::new` returns an `Arc<ThreadPool>`, so clone the `Arc` to share it
across closures rather than wrapping it yourself.

## TaskTraits — describing a task

`TaskTraits` is a plain struct of public fields; build it with struct-update
syntax from `Default`:

```rust
use rust_task::{TaskTraits, TaskPriority, TaskShutdownBehavior, ThreadPolicy};

let traits = TaskTraits {
    priority: TaskPriority::UserBlocking,                 // BestEffort | UserVisible | UserBlocking
    shutdown_behavior: TaskShutdownBehavior::BlockShutdown,
    ..Default::default()
};
```

Defaults: `UserVisible` priority, `SkipOnShutdown`, `PreferBackground`, `may_block: false`.

### Shutdown behavior — the part that bites people

`shutdown()` is not "drop everything." What happens to a pending or running task
depends on the trait it was posted with:

| `TaskShutdownBehavior` | Effect of `shutdown()` |
|------------------------|------------------------|
| `SkipOnShutdown` (default) | Pending tasks are dropped; new posts are rejected |
| `ContinueOnShutdown` | New posts still accepted; tasks may keep running |
| `BlockShutdown` | `shutdown()` **blocks** until every such task has completed |

Use `BlockShutdown` for work that *must* finish (flushing a file, releasing an
external resource). Use the default for work that's safe to abandon.

## Task patterns

### Delayed tasks
```rust
use std::time::Duration;
runner.post_delayed_task(Box::new(|| println!("later")), Duration::from_millis(500));
```
A dedicated timer thread holds delayed tasks until their deadline, then hands
them to the pool.

### post_task_and_reply — work on one sequence, answer on another
```rust
// Runs `work` on `runner`, then posts `reply` back to the sequence that called
// post_task_and_reply (captured automatically). Classic "do heavy work off-thread,
// update state back home" pattern.
runner.post_task_and_reply(
    Box::new(|| { /* heavy work */ }),
    Box::new(|| { /* runs back on the caller's sequence */ }),
);
```

### RepeatingTimer
```rust
use rust_task::RepeatingTimer;
let timer = RepeatingTimer::new(runner.clone());
timer.start(Duration::from_secs(1), || println!("tick"));   // fires on `runner`
// timer.stop();  — also stops automatically when dropped
```

## bind_once — safe lifetime-aware callbacks

The whole point of posting an `FnOnce` is that the queue *holds* it until it
runs. If that closure captures an `Arc<T>`, the queue keeps `T` alive — sometimes
longer than you want. `bind_once` lets you bind a `Weak<T>` instead, so the task
**silently no-ops if the object is already gone**:

```rust
use rust_task::bind_once;
use std::sync::Arc;

let handler = Arc::new(Handler::new());

// Weak binding: if `handler`'s last strong ref drops before this runs, the
// callback is skipped — no dangling, no lifetime extension.
pool.post_task(
    TaskTraits::default(),
    bind_once(Arc::downgrade(&handler), |h| h.on_event()),
);

// Strong binding (Arc): always runs, keeps the object alive until it does.
pool.post_task(TaskTraits::default(), bind_once(Arc::clone(&handler), |h| h.on_event()));
```

`bind_repeating(weak, f)` is the `Fn` (multi-shot) analogue, used by things like
`RepeatingTimer`.

**Gotcha:** don't use a `Weak` binding when the callback *must* fire — e.g. a
"done" signal that another thread is blocked waiting on. If the object drops, the
waiter hangs forever. Use an `Arc` binding (or a plain closure) there.

## TaskMonitor — timing + hang detection

Opt-in observability. Build a monitor, hand it to the pool at construction:

```rust
use rust_task::{TaskMonitor, ThreadPool};
use std::sync::Arc;
use std::time::Duration;

let monitor = TaskMonitor::builder()
    .on_metrics(|m| println!("queue={:?} exec={:?}", m.queue_time, m.execution_time))
    .hang_threshold(Duration::from_secs(5))
    .on_hang(|h| eprintln!("worker {} stuck for {:?}", h.worker_id, h.stuck_duration))
    .build();

let pool = ThreadPool::new_with_monitor(4, Arc::clone(&monitor));
```

`IoTaskRunner` has a matching `new_with_monitor` — see the rust_io reference.

## Try it

```bash
cd rust_task
cargo test                              # unit + integration + doctests
cargo run --example event_bus           # shared state guarded by a SequencedTaskRunner
cargo run --example repeating_timer     # RepeatingTimer / post_delayed_task
cargo run --example task_monitor        # metrics + hang detection
```

## When you go beyond rust_task

The moment the task involves a **file descriptor, a file, a socket, or an event
loop**, you're in `rust_io` / `rust_net` territory. Both reuse the task-runner
and `bind_once` concepts above but add one hard rule worth previewing now: every
operation that touches epoll **must be called from the IO thread**, and you must
**keep the I/O object (`FileProxy` / `SocketPosix`) alive** until its callbacks
fire, because the event loop only holds `Weak` references to watchers.

Read the matching reference before writing that code:

- [`references/rust_io.md`](references/rust_io.md) — `IoTaskRunner` (epoll loop), `FdWatcher`/`FdWatchController`, `FileProxy` (async file I/O)
- [`references/rust_net.md`](references/rust_net.md) — `SocketPosix` async TCP client & server
