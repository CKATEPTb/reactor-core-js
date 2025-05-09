import {Scheduler} from "@/schedulers/Scheduler";

/**
 * A scheduler that executes tasks asynchronously using the microtask queue.
 * Implements the `Scheduler` interface and schedules tasks using `Promise.resolve().then(...)`.
 */
export class MicroScheduler implements Scheduler {
    /**
     * Schedules a task to be executed asynchronously as a microtask.
     * Uses `Promise.resolve().then(task)` to place the task in the microtask queue.
     * @param {Function} task - The task function to be executed.
     */
    public schedule(task: () => void): void {
        Promise.resolve().then(task)
    }
}
