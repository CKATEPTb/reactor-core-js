/**
 * @packageDocumentation
 * Browser scheduler implementations and contracts.
 */
import type {DurationInput, Executor, Scheduler} from "@/schedulers/types.js";
import {AnimationFrameScheduler} from "@/schedulers/animation-frame-scheduler.js";
import {DelayScheduler} from "@/schedulers/delay-scheduler.js";
import {ExecutorScheduler} from "@/schedulers/executor-scheduler.js";
import {ImmediateScheduler} from "@/schedulers/immediate-scheduler.js";
import {IntervalScheduler} from "@/schedulers/interval-scheduler.js";
import {MacroScheduler} from "@/schedulers/macro-scheduler.js";
import {MicrotaskScheduler} from "@/schedulers/microtask-scheduler.js";

/** Shared scheduler that executes work synchronously. */
const immediateScheduler = new ImmediateScheduler();
/** Shared scheduler that queues work through the microtask queue. */
const microScheduler = new MicrotaskScheduler("micro");
/** Shared scheduler that queues work through the macrotask queue. */
const macroScheduler = new MacroScheduler();
/** Shared scheduler that queues work with `requestAnimationFrame` when available. */
const animationFrameScheduler = new AnimationFrameScheduler();

/** Browser-oriented scheduler factories and Reactor-compatible aliases. */
export const Schedulers = Object.freeze({
    /** Returns the synchronous immediate scheduler. */
    immediate(): Scheduler {
        return immediateScheduler;
    },

    /** Returns the shared microtask scheduler. */
    micro(): Scheduler {
        return microScheduler;
    },

    /** Returns the shared macrotask scheduler. */
    macro(): Scheduler {
        return macroScheduler;
    },

    /** Creates a scheduler that applies a default delay when one is not provided per call. */
    delay(delay: DurationInput, name = "delay"): Scheduler {
        return new DelayScheduler(delay, name);
    },

    /** Creates a scheduler that applies a default interval when one is not provided per call. */
    interval(period: DurationInput, name = "interval"): Scheduler {
        return new IntervalScheduler(period, name);
    },

    /** Returns the shared animation-frame scheduler. */
    animationFrame(): Scheduler {
        return animationFrameScheduler;
    },

    /** Returns the shared microtask scheduler using its legacy name. */
    microtask(): Scheduler {
        return microScheduler;
    },

    /** Returns the shared macrotask scheduler using its legacy timeout name. */
    timeout(): Scheduler {
        return macroScheduler;
    },

    /** Returns a shared scheduler suitable for single-threaded work. */
    single(): Scheduler {
        return microScheduler;
    },

    /** Returns a shared scheduler suitable for parallel-style browser work. */
    parallel(): Scheduler {
        return microScheduler;
    },

    /** Returns a shared scheduler suitable for delayed or blocking-like work. */
    boundedElastic(): Scheduler {
        return macroScheduler;
    },

    /** Creates a new microtask-backed single scheduler. */
    newSingle(name = "single"): Scheduler {
        return new MicrotaskScheduler(name);
    },

    /** Creates a new microtask-backed parallel scheduler. */
    newParallel(name = "parallel"): Scheduler {
        return new MicrotaskScheduler(name);
    },

    /** Creates a new macrotask-backed bounded elastic scheduler. */
    newBoundedElastic(name = "boundedElastic"): Scheduler {
        return new MacroScheduler(name);
    },

    /** Creates a scheduler backed by a custom executor. */
    fromExecutor(executor: Executor, name = "executor"): Scheduler {
        return new ExecutorScheduler(name, executor);
    }
});
