/**
 * @packageDocumentation
 * Browser scheduler implementations and contracts.
 */
import {BooleanDisposable} from "@/core/boolean-disposable.js";
import type {Disposable} from "@/core/types.js";
import {scheduleMicrotask} from "@/internal/microtask.js";
import {toMillis} from "@/schedulers/duration.js";
import {BaseScheduler} from "@/schedulers/base-scheduler.js";
import type {DurationInput} from "@/schedulers/types.js";

/** Scheduler that queues zero-delay tasks in the browser microtask queue. */
export class MicrotaskScheduler extends BaseScheduler {
    /** Creates a microtask scheduler. */
    public constructor(name = "microtask") {
        super(name);
    }

    /** Schedules a task on the microtask queue when there is no delay. */
    public schedule(task: () => void, delay?: DurationInput): Disposable {
        const disposedDisposable = this.disposedIfNeeded();
        if (disposedDisposable) {
            return disposedDisposable;
        }
        const delayMs = toMillis(delay);
        if (delayMs > 0) {
            return this.scheduleTimeout(task, delayMs);
        }
        let cancelled = false;
        scheduleMicrotask(() => {
            if (!cancelled && !this.isDisposed()) {
                task();
            }
        });
        return new BooleanDisposable(() => {
            cancelled = true;
        });
    }
}
