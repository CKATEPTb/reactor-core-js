/**
 * @packageDocumentation
 * Browser scheduler implementations and contracts.
 */
import {BooleanDisposable, type Disposable} from "@/core/index.js";
import {toMillis} from "@/scheduler/duration.js";
import {BaseScheduler} from "@/scheduler/base-scheduler.js";
import type {DurationInput} from "@/scheduler/types.js";

/** Scheduler that queues zero-delay tasks in the browser microtask queue. */
export class MicrotaskScheduler extends BaseScheduler {
    /** Creates a microtask scheduler. */
    public constructor(name = "microtask") {
        super(name);
    }

    /** Schedules a task with `queueMicrotask` when there is no delay. */
    public schedule(task: () => void, delay?: DurationInput): Disposable {
        const disposedDisposable = this.disposedIfNeeded();
        if (disposedDisposable) {
            return disposedDisposable;
        }
        if (toMillis(delay) > 0) {
            return this.scheduleTimeout(task, delay);
        }
        let cancelled = false;
        queueMicrotask(() => {
            if (!cancelled && !this.isDisposed()) {
                task();
            }
        });
        return new BooleanDisposable(() => {
            cancelled = true;
        });
    }
}
