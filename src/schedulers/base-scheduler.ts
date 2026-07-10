/**
 * @packageDocumentation
 * Browser scheduler implementations and contracts.
 */
import {BooleanDisposable} from "@/core/boolean-disposable.js";
import {CompositeDisposable} from "@/core/composite-disposable.js";
import type {Disposable} from "@/core/types.js";
import {BasicWorker} from "@/schedulers/basic-worker.js";
import {disposed} from "@/schedulers/disposed.js";
import {toMillis} from "@/schedulers/duration.js";
import type {DurationInput, Scheduler, SchedulerWorker} from "@/schedulers/types.js";

/** Base implementation for browser schedulers that share lifecycle behavior. */
export abstract class BaseScheduler implements Scheduler {
    /** Human-readable scheduler name. */
    public readonly name: string;
    /** Tracks whether this scheduler has been disposed. */
    private disposed = false;

    /** Creates a scheduler base with the provided name. */
    public constructor(name: string) {
        this.name = name;
    }

    /** Returns the current wall-clock time in milliseconds. */
    public now(): number {
        return Date.now();
    }

    /** Schedules a single task, optionally after a delay. */
    public abstract schedule(task: () => void, delay?: DurationInput): Disposable;

    /** Schedules a periodic task using this scheduler for the first execution. */
    public schedulePeriodically(task: () => void, initialDelay?: DurationInput, period?: DurationInput): Disposable {
        if (this.disposed) {
            return disposed();
        }
        const composite = new CompositeDisposable();
        const start = this.schedule(() => {
            if (composite.isDisposed()) {
                return;
            }
            task();
            const id = setInterval(() => {
                if (!composite.isDisposed() && !this.disposed) {
                    task();
                }
            }, Math.max(1, toMillis(period)));
            composite.add(new BooleanDisposable(() => clearInterval(id)));
        }, initialDelay);
        composite.add(start);
        return composite;
    }

    /** Creates a worker that tracks tasks for grouped disposal. */
    public createWorker(): SchedulerWorker {
        return new BasicWorker(this);
    }

    /** Marks this scheduler as disposed. */
    public dispose(): void {
        this.disposed = true;
    }

    /** Returns true when this scheduler is disposed. */
    public isDisposed(): boolean {
        return this.disposed;
    }

    /** Returns an already-disposed disposable when this scheduler is disposed. */
    protected disposedIfNeeded(): Disposable | undefined {
        return this.disposed ? disposed() : undefined;
    }

    /** Schedules a timeout-backed task with disposal and disposed-state checks. */
    protected scheduleTimeout(task: () => void, delayMs = 0): Disposable {
        if (this.disposed) {
            return disposed();
        }
        const id = setTimeout(() => {
            if (!this.disposed) {
                task();
            }
        }, delayMs);
        return new BooleanDisposable(() => clearTimeout(id));
    }
}
