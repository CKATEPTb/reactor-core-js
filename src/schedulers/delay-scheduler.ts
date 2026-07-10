/**
 * @packageDocumentation
 * Browser scheduler implementations and contracts.
 */
import {MacroScheduler} from "@/schedulers/macro-scheduler.js";
import type {DurationInput} from "@/schedulers/types.js";

/** Scheduler that applies a configured delay when no call-specific delay is provided. */
export class DelayScheduler extends MacroScheduler {
    /** Creates a scheduler with the provided default delay. */
    public constructor(delay: DurationInput, name = "delay") {
        super(name, delay);
    }
}
