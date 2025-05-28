import {CancellableScheduler} from "@/schedulers/Scheduler";

/**
 * A scheduler that executes tasks multiple times with a specified interval.
 * Implements the `CancellableScheduler` interface, allowing scheduled tasks to be canceled.
 */
export class IntervalScheduler implements CancellableScheduler {
    private readonly interval: number

    /**
     * Creates a new IntervalScheduler.
     * @param {number} delay - The interval duration in milliseconds.
     */
    constructor(delay: number) {
        this.interval = delay
    }

    /**
     * Schedules a task to be executed multiple times with a specified interval.
     * Returns an object with a cancel method to clear the timeout.
     * @param {Function} task - The task function to be executed.
     * @returns {Object} An object with a `cancel` method to stop the execution.
     */
    public schedule(task: () => void): { cancel: () => void } {
        const id = setInterval(task, this.interval)
        return {
            /**
             * Cancels the scheduled task.
             */
            cancel: () => clearInterval(id)
        }
    }
}
