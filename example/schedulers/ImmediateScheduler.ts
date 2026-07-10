import {Scheduler} from '@/schedulers/Scheduler'

/**
 * A scheduler that immediately executes tasks.
 * Implements the `Scheduler` interface to execute tasks without any delay.
 */

export class ImmediateScheduler implements Scheduler {
    /**
     * Schedules a task to be executed immediately.
     * @param {Function} task - The task function to be executed.
     */
    public schedule(task: () => void): void {
        task()
    }
}
