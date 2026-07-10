/**
 * @packageDocumentation
 * Browser scheduler implementations and contracts.
 */
import type {Disposable} from "@/core/types.js";
import {BaseScheduler} from "@/schedulers/base-scheduler.js";
import {toMillis} from "@/schedulers/duration.js";
import type {DurationInput} from "@/schedulers/types.js";

/** Scheduler that queues tasks through the browser macrotask queue. */
export class MacroScheduler extends BaseScheduler {
    /** Optional duration applied when a schedule call does not provide one. */
    private readonly defaultDelay: DurationInput | undefined;

    /** Creates a macrotask scheduler. */
    public constructor(name = "macro", defaultDelay?: DurationInput) {
        super(name);
        this.defaultDelay = defaultDelay;
    }

    /** Schedules a task with `setTimeout`. */
    public schedule(task: () => void, delay?: DurationInput): Disposable {
        return this.scheduleTimeout(task, toMillis(delay ?? this.defaultDelay));
    }

    /** Schedules a repeated task with the default duration as fallback cadence. */
    public override schedulePeriodically(task: () => void, initialDelay?: DurationInput, period?: DurationInput): Disposable {
        return super.schedulePeriodically(task, initialDelay ?? this.defaultDelay, period ?? this.defaultDelay);
    }
}
