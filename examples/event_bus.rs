//! Event bus backed by a SequencedTaskRunner.
//!
//! All subscribe / unsubscribe / publish operations are posted to the same
//! sequence, giving three guarantees for free:
//!
//!  1. **Ordering** — events are dispatched strictly in publication order.
//!  2. **Serialization** — an unsubscribe posted before a publish is guaranteed
//!     to take effect before that publish dispatches; no external locking needed.
//!  3. **Re-entrancy** — a callback that calls publish() is safe: the new event
//!     is appended to the sequence and dispatched *after* the current event
//!     finishes, never inline.
//!
//! Run with:
//!   cargo run --example event_bus

use rust_task::{SequencedTaskRunner, TaskTraits, ThreadPool};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};

// ── Event type ────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
enum AppEvent {
    UserLoggedIn(String),
    MessageSent { from: String, text: String },
    UserLoggedOut(String),
}

// ── EventBus ──────────────────────────────────────────────────────────────────

// Arc<dyn Fn> so callbacks can be cheaply cloned out of the table before
// calling them — this releases the Mutex before any callback runs.
type Callback<E> = Arc<dyn Fn(&E) + Send + Sync + 'static>;

struct BusState<E> {
    // Vec preserves insertion order so dispatch order is deterministic.
    subscribers: Vec<(u64, Callback<E>)>,
}

struct EventBus<E: Send + 'static> {
    state: Arc<Mutex<BusState<E>>>,
    runner: Arc<dyn SequencedTaskRunner>,
    next_id: AtomicU64,
}

impl<E: Send + 'static> EventBus<E> {
    fn new(pool: &Arc<ThreadPool>) -> Arc<Self> {
        Arc::new(Self {
            state: Arc::new(Mutex::new(BusState {
                subscribers: Vec::new(),
            })),
            runner: pool.create_sequenced_task_runner(TaskTraits::default()),
            next_id: AtomicU64::new(0),
        })
    }

    // Registers a subscriber.  The ID is allocated immediately (atomic), but
    // the registration itself is posted to the sequence, so it takes effect
    // before any subsequent publish() call posted after this one.
    fn subscribe(&self, cb: impl Fn(&E) + Send + Sync + 'static) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let state = Arc::clone(&self.state);
        self.runner.post_task(Box::new(move || {
            state.lock().unwrap().subscribers.push((id, Arc::new(cb)));
        }));
        id
    }

    fn unsubscribe(&self, id: u64) {
        let state = Arc::clone(&self.state);
        self.runner.post_task(Box::new(move || {
            state.lock().unwrap().subscribers.retain(|(sid, _)| *sid != id);
        }));
    }

    fn publish(&self, event: E) {
        let state = Arc::clone(&self.state);
        self.runner.post_task(Box::new(move || {
            // Snapshot the callback list under the lock, then release it before
            // calling any callback.  This means:
            //   • The lock is never held while user code runs (no risk of
            //     contention if a callback is slow).
            //   • A callback calling publish() just posts a new task to the
            //     sequence — it never tries to acquire the Mutex inline.
            let cbs: Vec<Callback<E>> = state
                .lock()
                .unwrap()
                .subscribers
                .iter()
                .map(|(_, cb)| Arc::clone(cb))
                .collect();

            for cb in cbs {
                cb(&event);
            }
        }));
    }

    // Posts a one-shot closure after all previously-posted operations.
    // Use this to know when a batch of publishes has been fully dispatched.
    fn flush(&self, done: impl FnOnce() + Send + 'static) {
        self.runner.post_task(Box::new(done));
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn wait_flush(bus: &EventBus<AppEvent>) {
    let b = Arc::new(Barrier::new(2));
    let bc = Arc::clone(&b);
    bus.flush(move || { bc.wait(); });
    b.wait();
}

// ── Demo ──────────────────────────────────────────────────────────────────────

fn main() {
    let pool = ThreadPool::new(4);
    let bus = EventBus::<AppEvent>::new(&pool);

    let log = Arc::new(Mutex::new(Vec::<String>::new()));

    println!("=== Event Bus Demo ===\n");

    // ── 1. Multiple subscribers ───────────────────────────────────────────────

    println!("Step 1: register logger + audit subscribers, publish login + message");

    let l = Arc::clone(&log);
    bus.subscribe(move |e| {
        let s = match e {
            AppEvent::UserLoggedIn(u)              => format!("[logger] login:   {u}"),
            AppEvent::MessageSent { from, text }   => format!("[logger] message: {from} → {text}"),
            AppEvent::UserLoggedOut(u)             => format!("[logger] logout:  {u}"),
        };
        l.lock().unwrap().push(s);
    });

    let l = Arc::clone(&log);
    let audit_id = bus.subscribe(move |e| {
        if let AppEvent::UserLoggedIn(u) = e {
            l.lock().unwrap().push(format!("[audit ] {u} authenticated"));
        }
    });

    bus.publish(AppEvent::UserLoggedIn("alice".into()));
    bus.publish(AppEvent::MessageSent { from: "alice".into(), text: "hello, world".into() });

    wait_flush(&bus);
    print_log(&log);

    // ── 2. Unsubscribe serialized with publish ─────────────────────────────────
    //
    // unsubscribe() and the next publish() go through the same sequence, so
    // the removal is guaranteed to happen before the logout is dispatched.

    println!("Step 2: unsubscribe audit, then publish logout (audit must NOT see it)");

    bus.unsubscribe(audit_id);
    bus.publish(AppEvent::UserLoggedOut("alice".into()));

    wait_flush(&bus);
    print_log(&log);

    // ── 3. Re-entrant publish ──────────────────────────────────────────────────
    //
    // The auto-welcome subscriber calls bus.publish() inside its callback.
    // That publish is posted to the sequence and runs AFTER the current
    // dispatch task finishes — there is no inline re-dispatch.
    //
    // We need two flushes to observe the full result:
    //   flush 1 → drains "login bob" dispatch (which enqueues the welcome message)
    //   flush 2 → drains the welcome message dispatch

    println!("Step 3: auto-welcome subscriber calls publish() inside callback (re-entrant)");

    let bus2 = Arc::clone(&bus);
    let l = Arc::clone(&log);
    bus.subscribe(move |e| {
        if let AppEvent::UserLoggedIn(u) = e {
            l.lock().unwrap().push(format!("[welcom] queuing welcome for {u}"));
            // Re-entrant publish: appended to sequence, NOT dispatched inline.
            bus2.publish(AppEvent::MessageSent {
                from: "system".into(),
                text: format!("Welcome, {u}!"),
            });
        }
    });

    bus.publish(AppEvent::UserLoggedIn("bob".into()));

    wait_flush(&bus); // drain up to and including "login bob" dispatch
    wait_flush(&bus); // drain the welcome message that was queued by the callback

    print_log(&log);

    pool.shutdown();
}

fn print_log(log: &Mutex<Vec<String>>) {
    let mut guard = log.lock().unwrap();
    for entry in guard.drain(..) {
        println!("  {entry}");
    }
    println!();
}
