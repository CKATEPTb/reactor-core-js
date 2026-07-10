/**
 * @packageDocumentation
 * Browser scheduler implementations and contracts.
 */
import {MacroScheduler} from "@/schedulers/macro-scheduler.js";

/** Compatibility scheduler name for timeout-backed macrotask scheduling. */
export class TimeoutScheduler extends MacroScheduler {
    /** Creates a timeout-named macrotask scheduler. */
    public constructor(name = "timeout") {
        super(name);
    }
}
