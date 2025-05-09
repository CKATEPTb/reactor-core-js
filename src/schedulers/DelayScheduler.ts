import {CancellableScheduler} from "@/schedulers/Scheduler";

/**
 * A scheduler that executes tasks after a specified delay.
 * Implements the `CancellableScheduler` interface, allowing scheduled tasks to be canceled.
 */
export class DelayScheduler implements CancellableScheduler {
    private readonly delay: number

    /**
     * Creates a new DelayScheduler.
     * @param {number} delay - The delay duration in milliseconds.
     */
    constructor(delay: number) {
        this.delay = delay
    }

    /**
     * Schedules a task to be executed after the specified delay.
     * Returns an object with a cancel method to clear the timeout.
     * @param {Function} task - The task function to be executed.
     * @returns {Object} An object with a `cancel` method to stop the execution.
     */
    public schedule(task: () => void): { cancel: () => void } {
        const id = setTimeout(task, this.delay)
        return {
            /**
             * Cancels the scheduled task.
             */
            cancel: () => clearTimeout(id)
        }
    }
}
