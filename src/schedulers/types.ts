/**
 * @packageDocumentation
 * Browser scheduler implementations and contracts.
 */
import type {Disposable} from "@/core/types.js";

/** Duration accepted by browser schedulers, expressed in milliseconds or fields. */
export type DurationInput =
    | number
    | {
    milliseconds?: number;
    seconds?: number;
    minutes?: number;
    hours?: number;
};

/** Scheduler capable of executing immediate, delayed and periodic tasks. */
export interface Scheduler extends Disposable {
    /** Human-readable scheduler name. */
    readonly name: string;

    /** Returns the current scheduler time in milliseconds. */
    now(): number;

    /** Schedules a single task, optionally after a delay. */
    schedule(task: () => void, delay?: DurationInput): Disposable;

    /** Schedules a repeated task. */
    schedulePeriodically(task: () => void, initialDelay?: DurationInput, period?: DurationInput): Disposable;

    /** Creates a worker that can dispose a group of tasks together. */
    createWorker(): SchedulerWorker;
}

/** Worker groups scheduled tasks for coordinated disposal. */
export interface SchedulerWorker extends Disposable {
    /** Schedules a single task on this worker. */
    schedule(task: () => void, delay?: DurationInput): Disposable;

    /** Schedules a repeated task on this worker. */
    schedulePeriodically(task: () => void, initialDelay?: DurationInput, period?: DurationInput): Disposable;
}

/** Function that accepts a task for execution. */
export type Executor = (task: () => void) => void;
