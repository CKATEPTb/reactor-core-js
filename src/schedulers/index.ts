import {ImmediateScheduler} from "@/schedulers/ImmediateScheduler";
import {MicroScheduler} from "@/schedulers/MicroScheduler";
import {MacroScheduler} from "@/schedulers/MacroScheduler";
import {DelayScheduler} from "@/schedulers/DelayScheduler";
import {IntervalScheduler} from "@/schedulers/IntervalScheduler";

export * from '@/schedulers/Scheduler'

/**
 * A collection of commonly used schedulers for task execution.
 * Provides methods to create instances of various scheduler types.
 */
export const Schedulers = {
    /**
     * Creates an instance of `ImmediateScheduler`.
     * Executes tasks immediately without any delay.
     * @returns {ImmediateScheduler} An instance of ImmediateScheduler.
     */
    immediate: (): ImmediateScheduler => new ImmediateScheduler(),
    /**
     * Creates an instance of `MicroScheduler`.
     * Executes tasks asynchronously using the microtask queue.
     * @returns {MicroScheduler} An instance of MicroScheduler.
     */
    micro: (): MicroScheduler => new MicroScheduler(),
    /**
     * Creates an instance of `MacroScheduler`.
     * Executes tasks asynchronously using the macro task queue (via `setTimeout`).
     * @returns {MacroScheduler} An instance of MacroScheduler.
     */
    macro: (): MacroScheduler => new MacroScheduler(),
    /**
     * Creates an instance of `DelayScheduler` with a specified delay.
     * Executes tasks after a given delay using `setTimeout`.
     * @param {number} ms - The delay in milliseconds before executing the task.
     * @returns {DelayScheduler} An instance of DelayScheduler.
     */
    delay: (ms: number): DelayScheduler => new DelayScheduler(ms),
    /**
     * Creates an instance of `IntervalScheduler` with a specified interval.
     * Executes tasks multiple times with a given interval using `setInterval`.
     * @param {number} ms - The interval in milliseconds between task executing.
     * @returns {IntervalScheduler} An instance of IntervalScheduler.
     */
    interval: (ms: number): IntervalScheduler => new IntervalScheduler(ms)
}