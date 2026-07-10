/**
 * @packageDocumentation
 * Browser scheduler implementations and contracts.
 */
import type {Disposable} from "@/core/types.js";
import {BaseScheduler} from "@/schedulers/base-scheduler.js";
import {toMillis} from "@/schedulers/duration.js";
import type {DurationInput} from "@/schedulers/types.js";

/** Scheduler that queues tasks through `setTimeout`. */
export class TimeoutScheduler extends BaseScheduler {
    /** Creates a timeout scheduler. */
    public constructor(name = "timeout") {
        super(name);
    }

    /** Schedules a task with `setTimeout`. */
    public schedule(task: () => void, delay?: DurationInput): Disposable {
        return this.scheduleTimeout(task, toMillis(delay));
    }
}
