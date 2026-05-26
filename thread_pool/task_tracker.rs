use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};

use crate::task_traits::{TaskShutdownBehavior, TaskTraits};

pub struct TaskTracker {
    shutdown_started: AtomicBool,
    // Count of BlockShutdown tasks currently executing.
    num_tasks_blocking_shutdown: AtomicUsize,
    // Signalled when num_tasks_blocking_shutdown drops to zero.
    all_block_shutdown_done: (Mutex<()>, Condvar),
}

impl TaskTracker {
    pub fn new() -> Self {
        Self {
            shutdown_started: AtomicBool::new(false),
            num_tasks_blocking_shutdown: AtomicUsize::new(0),
            all_block_shutdown_done: (Mutex::new(()), Condvar::new()),
        }
    }

    // Returns true if the task may be posted.
    // After shutdown has started, only ContinueOnShutdown tasks are accepted.
    pub fn will_post_task(&self, traits: &TaskTraits) -> bool {
        if !self.shutdown_started.load(Ordering::Acquire) {
            return true;
        }
        matches!(traits.shutdown_behavior, TaskShutdownBehavior::ContinueOnShutdown)
    }

    // Called immediately before executing a task.
    // BlockShutdown tasks increment the counter so shutdown() waits for them.
    pub fn before_run_task(&self, traits: &TaskTraits) {
        if traits.shutdown_behavior == TaskShutdownBehavior::BlockShutdown {
            self.num_tasks_blocking_shutdown.fetch_add(1, Ordering::AcqRel);
        }
    }

    // Called immediately after executing a task.
    pub fn after_run_task(&self, traits: &TaskTraits) {
        if traits.shutdown_behavior == TaskShutdownBehavior::BlockShutdown {
            let prev = self.num_tasks_blocking_shutdown.fetch_sub(1, Ordering::AcqRel);
            if prev == 1 {
                let (lock, cvar) = &self.all_block_shutdown_done;
                let _guard = lock.lock().unwrap();
                cvar.notify_all();
            }
        }
    }

    pub fn is_shutdown_started(&self) -> bool {
        self.shutdown_started.load(Ordering::Acquire)
    }

    // Marks shutdown as started and blocks until all BlockShutdown tasks finish.
    pub fn shutdown(&self) {
        self.shutdown_started.store(true, Ordering::Release);

        let (lock, cvar) = &self.all_block_shutdown_done;
        let mut guard = lock.lock().unwrap();
        while self.num_tasks_blocking_shutdown.load(Ordering::Acquire) > 0 {
            guard = cvar.wait(guard).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_traits::{TaskPriority, TaskShutdownBehavior, TaskTraits, ThreadPolicy};

    fn traits_with(behavior: TaskShutdownBehavior) -> TaskTraits {
        TaskTraits {
            priority: TaskPriority::UserVisible,
            shutdown_behavior: behavior,
            thread_policy: ThreadPolicy::PreferBackground,
            may_block: false,
        }
    }

    #[test]
    fn allows_all_tasks_before_shutdown() {
        let tracker = TaskTracker::new();
        assert!(tracker.will_post_task(&traits_with(TaskShutdownBehavior::ContinueOnShutdown)));
        assert!(tracker.will_post_task(&traits_with(TaskShutdownBehavior::SkipOnShutdown)));
        assert!(tracker.will_post_task(&traits_with(TaskShutdownBehavior::BlockShutdown)));
    }

    #[test]
    fn after_shutdown_only_continue_on_shutdown_is_allowed() {
        let tracker = TaskTracker::new();
        tracker.shutdown_started.store(true, Ordering::Release);

        assert!(tracker.will_post_task(&traits_with(TaskShutdownBehavior::ContinueOnShutdown)));
        assert!(!tracker.will_post_task(&traits_with(TaskShutdownBehavior::SkipOnShutdown)));
        assert!(!tracker.will_post_task(&traits_with(TaskShutdownBehavior::BlockShutdown)));
    }

    #[test]
    fn shutdown_returns_immediately_when_no_block_shutdown_tasks() {
        let tracker = TaskTracker::new();
        // Should not block since no BlockShutdown tasks are running.
        tracker.shutdown();
    }

    #[test]
    fn shutdown_waits_for_block_shutdown_task() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let tracker = Arc::new(TaskTracker::new());
        let barrier = Arc::new(Barrier::new(2));
        let b = Arc::clone(&barrier);

        let traits = traits_with(TaskShutdownBehavior::BlockShutdown);
        tracker.before_run_task(&traits);

        let t = Arc::clone(&tracker);
        thread::spawn(move || {
            b.wait(); // signal that shutdown is now waiting
            t.after_run_task(&traits_with(TaskShutdownBehavior::BlockShutdown));
        });

        // shutdown() should block until the spawned thread calls after_run_task.
        let t2 = Arc::clone(&tracker);
        let shutdown_handle = thread::spawn(move || t2.shutdown());

        barrier.wait(); // tell the task thread that shutdown is waiting
        shutdown_handle.join().unwrap(); // should unblock after after_run_task
    }
}
