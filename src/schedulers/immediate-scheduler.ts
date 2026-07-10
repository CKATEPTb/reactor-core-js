/**
 * @packageDocumentation
 * Browser scheduler implementations and contracts.
 */
import type {Disposable} from "@/core/types.js";
import {disposed} from "@/schedulers/disposed.js";
import {toMillis} from "@/schedulers/duration.js";
import {BaseScheduler} from "@/schedulers/base-scheduler.js";
import type {DurationInput} from "@/schedulers/types.js";

/** Scheduler that executes zero-delay tasks synchronously. */
export class ImmediateScheduler extends BaseScheduler {
    /** Creates an immediate scheduler. */
    public constructor(name = "immediate") {
        super(name);
    }

    /** Schedules a task synchronously when there is no delay. */
    public schedule(task: () => void, delay?: DurationInput): Disposable {
        const disposedDisposable = this.disposedIfNeeded();
        if (disposedDisposable) {
            return disposedDisposable;
        }
        const delayMs = toMillis(delay);
        if (delayMs > 0) {
            return this.scheduleTimeout(task, delayMs);
        }
        task();
        return disposed();
    }
}
