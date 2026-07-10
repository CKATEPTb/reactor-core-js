import {Scheduler} from "@/schedulers/Scheduler";

/**
 * A scheduler that executes tasks asynchronously using the macro task queue.
 * Implements the `Scheduler` interface and schedules tasks using `setTimeout` with a delay of `0`.
 */
export class MacroScheduler implements Scheduler {
    /**
     * Schedules a task to be executed asynchronously.
     * Uses `setTimeout` with a delay of `0` to place the task in the macro task queue.
     * @param {Function} task - The task function to be executed.
     */
    public schedule(task: () => void): void {
        setTimeout(task, 0)
    }
}
