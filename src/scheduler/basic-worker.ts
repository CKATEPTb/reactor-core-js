/**
 * @packageDocumentation
 * Browser scheduler implementations and contracts.
 */
import type {Disposable} from "@/core/index.js";
import {CompositeDisposable} from "@/core/index.js";
import type {DurationInput, Scheduler, SchedulerWorker} from "@/scheduler/types.js";
import {disposed} from "@/scheduler/disposed.js";

/** Scheduler worker that tracks child tasks in a composite disposable. */
export class BasicWorker implements SchedulerWorker {
    /** Tasks currently owned by this worker. */
    private readonly tasks = new CompositeDisposable();
    /** Scheduler used to create tracked tasks. */
    private readonly scheduler: Scheduler;

    /** Creates a worker backed by the provided scheduler. */
    public constructor(scheduler: Scheduler) {
        this.scheduler = scheduler;
    }

    /** Schedules a task and tracks it for disposal. */
    public schedule(task: () => void, delay?: DurationInput): Disposable {
        if (this.tasks.isDisposed()) {
            return disposed();
        }
        let tracked: Disposable | undefined;
        const disposable = this.scheduler.schedule(() => {
            if (tracked) {
                this.tasks.remove(tracked);
            }
            task();
        }, delay);
        tracked = disposable;
        if (!disposable.isDisposed()) {
            this.tasks.add(disposable);
        }
        return disposable;
    }

    /** Schedules a periodic task and tracks it for disposal. */
    public schedulePeriodically(task: () => void, initialDelay?: DurationInput, period?: DurationInput): Disposable {
        if (this.tasks.isDisposed()) {
            return disposed();
        }
        const disposable = this.scheduler.schedulePeriodically(task, initialDelay, period);
        this.tasks.add(disposable);
        return disposable;
    }

    /** Disposes all tasks scheduled through this worker. */
    public dispose(): void {
        this.tasks.dispose();
    }

    /** Returns true after this worker has been disposed. */
    public isDisposed(): boolean {
        return this.tasks.isDisposed();
    }
}
