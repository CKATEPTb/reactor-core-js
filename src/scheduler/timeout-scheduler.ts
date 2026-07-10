/**
 * @packageDocumentation
 * Browser scheduler implementations and contracts.
 */
import type {Disposable} from "@/core/index.js";
import {BaseScheduler} from "@/scheduler/base-scheduler.js";
import type {DurationInput} from "@/scheduler/types.js";

/** Scheduler that queues tasks through `setTimeout`. */
export class TimeoutScheduler extends BaseScheduler {
    /** Creates a timeout scheduler. */
    public constructor(name = "timeout") {
        super(name);
    }

    /** Schedules a task with `setTimeout`. */
    public schedule(task: () => void, delay?: DurationInput): Disposable {
        return this.scheduleTimeout(task, delay);
    }
}
