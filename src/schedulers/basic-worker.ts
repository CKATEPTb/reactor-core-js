/**
 * @packageDocumentation
 * Browser scheduler implementations and contracts.
 */
import {CompositeDisposable} from "@/core/composite-disposable.js";
import type {Disposable} from "@/core/types.js";
import type {DurationInput, Scheduler, SchedulerWorker} from "@/schedulers/types.js";
import {disposed} from "@/schedulers/disposed.js";

/** Scheduler worker that tracks child tasks in a composite disposable. */
export class BasicWorker implements SchedulerWorker {
    /** Tasks currently owned by this worker, allocated only after a task needs tracking. */
    private tasks: CompositeDisposable | undefined;
    /** Tracks whether this worker has been disposed. */
    private disposed = false;
    /** Scheduler used to create tracked tasks. */
    private readonly scheduler: Scheduler;

    /** Creates a worker backed by the provided scheduler. */
    public constructor(scheduler: Scheduler) {
        this.scheduler = scheduler;
    }

    /** Schedules a task and tracks it for disposal. */
    public schedule(task: () => void, delay?: DurationInput): Disposable {
        if (this.disposed) {
            return disposed();
        }
        let tracked: Disposable | undefined;
        let finished = false;
        const disposable = this.scheduler.schedule(() => {
            if (tracked) {
                this.tasks?.remove(tracked);
            } else {
                finished = true;
            }
            task();
        }, delay);
        tracked = disposable;
        if (this.disposed) {
            disposable.dispose();
        } else if (!finished && !disposable.isDisposed()) {
            (this.tasks ??= new CompositeDisposable()).add(disposable);
        }
        return disposable;
    }

    /** Schedules a periodic task and tracks it for disposal. */
    public schedulePeriodically(task: () => void, initialDelay?: DurationInput, period?: DurationInput): Disposable {
        if (this.disposed) {
            return disposed();
        }
        const disposable = this.scheduler.schedulePeriodically(task, initialDelay, period);
        if (this.disposed) {
            disposable.dispose();
        } else if (!disposable.isDisposed()) {
            (this.tasks ??= new CompositeDisposable()).add(disposable);
        }
        return disposable;
    }

    /** Disposes all tasks scheduled through this worker. */
    public dispose(): void {
        if (this.disposed) {
            return;
        }
        this.disposed = true;
        this.tasks?.dispose();
        this.tasks = undefined;
    }

    /** Returns true after this worker has been disposed. */
    public isDisposed(): boolean {
        return this.disposed;
    }
}
