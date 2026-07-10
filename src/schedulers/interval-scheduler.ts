/**
 * @packageDocumentation
 * Browser scheduler implementations and contracts.
 */
import {MacroScheduler} from "@/schedulers/macro-scheduler.js";
import type {DurationInput} from "@/schedulers/types.js";

/** Scheduler that applies a configured interval as its default timing cadence. */
export class IntervalScheduler extends MacroScheduler {
    /** Creates a scheduler with the provided default interval. */
    public constructor(period: DurationInput, name = "interval") {
        super(name, period);
    }
}
