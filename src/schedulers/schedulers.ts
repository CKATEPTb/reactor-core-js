/**
 * @packageDocumentation
 * Browser scheduler implementations and contracts.
 */
import type {Executor, Scheduler} from "@/schedulers/types.js";
import {AnimationFrameScheduler} from "@/schedulers/animation-frame-scheduler.js";
import {ExecutorScheduler} from "@/schedulers/executor-scheduler.js";
import {ImmediateScheduler} from "@/schedulers/immediate-scheduler.js";
import {MicrotaskScheduler} from "@/schedulers/microtask-scheduler.js";
import {TimeoutScheduler} from "@/schedulers/timeout-scheduler.js";

/** Shared scheduler that executes work synchronously. */
const immediateScheduler = new ImmediateScheduler();
/** Shared scheduler that queues work with `queueMicrotask`. */
const microtaskScheduler = new MicrotaskScheduler();
/** Shared scheduler that queues work with `setTimeout`. */
const timeoutScheduler = new TimeoutScheduler();
/** Shared scheduler that queues work with `requestAnimationFrame` when available. */
const animationFrameScheduler = new AnimationFrameScheduler();

/** Browser-oriented scheduler factories and shared scheduler accessors. */
export const Schedulers = Object.freeze({
    /** Returns the synchronous immediate scheduler. */
    immediate(): Scheduler {
        return immediateScheduler;
    },

    /** Returns the shared microtask scheduler. */
    microtask(): Scheduler {
        return microtaskScheduler;
    },

    /** Returns the shared timeout scheduler. */
    timeout(): Scheduler {
        return timeoutScheduler;
    },

    /** Returns the shared animation-frame scheduler. */
    animationFrame(): Scheduler {
        return animationFrameScheduler;
    },

    /** Returns a shared scheduler suitable for single-threaded work. */
    single(): Scheduler {
        return microtaskScheduler;
    },

    /** Returns a shared scheduler suitable for parallel-style browser work. */
    parallel(): Scheduler {
        return microtaskScheduler;
    },

    /** Returns a shared scheduler suitable for delayed or blocking-like work. */
    boundedElastic(): Scheduler {
        return timeoutScheduler;
    },

    /** Creates a new microtask-backed single scheduler. */
    newSingle(name = "single"): Scheduler {
        return new MicrotaskScheduler(name);
    },

    /** Creates a new microtask-backed parallel scheduler. */
    newParallel(name = "parallel"): Scheduler {
        return new MicrotaskScheduler(name);
    },

    /** Creates a new timeout-backed bounded elastic scheduler. */
    newBoundedElastic(name = "boundedElastic"): Scheduler {
        return new TimeoutScheduler(name);
    },

    /** Creates a scheduler backed by a custom executor. */
    fromExecutor(executor: Executor, name = "executor"): Scheduler {
        return new ExecutorScheduler(name, executor);
    }
});
