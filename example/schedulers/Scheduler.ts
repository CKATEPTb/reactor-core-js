/**
 * Represents a basic scheduling interface for executing tasks.
 */
export interface Scheduler {
    /**
     * Schedules a task to be executed.
     * @param {Function} task - The task function to be executed.
     */
    schedule(task: () => void): void
}

/**
 * Represents a cancellable scheduling interface, extending the basic `Scheduler` interface.
 * Provides the ability to cancel a scheduled task.
 */
export interface CancellableScheduler extends Scheduler {
    /**
     * Schedules a task to be executed with the ability to cancel it.
     * @param {Function} task - The task function to be executed.
     * @returns {Object} An object containing a `cancel` method to stop the execution.
     */
    schedule(task: () => void): { cancel: () => void }
}
