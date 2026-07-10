/**
 * @packageDocumentation
 * Browser scheduler implementations and contracts.
 */
import type {Disposable} from "@/core/index.js";
import {disposed} from "@/scheduler/disposed.js";
import {toMillis} from "@/scheduler/duration.js";
import {BaseScheduler} from "@/scheduler/base-scheduler.js";
import type {DurationInput} from "@/scheduler/types.js";

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
        if (toMillis(delay) > 0) {
            return this.scheduleTimeout(task, delay);
        }
        task();
        return disposed();
    }
}
