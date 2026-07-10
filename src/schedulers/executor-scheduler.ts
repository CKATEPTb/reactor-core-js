/**
 * @packageDocumentation
 * Browser scheduler implementations and contracts.
 */
import {BooleanDisposable} from "@/core/boolean-disposable.js";
import type {Disposable} from "@/core/types.js";
import {BaseScheduler} from "@/schedulers/base-scheduler.js";
import type {DurationInput, Executor} from "@/schedulers/types.js";
import {toMillis} from "@/schedulers/duration.js";

/** Scheduler backed by a caller-provided task executor. */
export class ExecutorScheduler extends BaseScheduler {
    /** Executor callback used for immediate task scheduling. */
    private readonly executor: Executor;

    /** Creates a scheduler backed by a user-provided executor. */
    public constructor(name: string, executor: Executor) {
        super(name);
        this.executor = executor;
    }

    /** Schedules a task through the executor. */
    public schedule(task: () => void, delay?: DurationInput): Disposable {
        const disposedDisposable = this.disposedIfNeeded();
        if (disposedDisposable) {
            return disposedDisposable;
        }
        const delayMs = toMillis(delay);
        if (delayMs > 0) {
            return this.scheduleTimeout(() => {
                this.schedule(task);
            }, delayMs);
        }
        let cancelled = false;
        this.executor(() => {
            if (!cancelled && !this.isDisposed()) {
                task();
            }
        });
        return new BooleanDisposable(() => {
            cancelled = true;
        });
    }
}
