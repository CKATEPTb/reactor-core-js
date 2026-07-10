/**
 * @packageDocumentation
 * Browser scheduler implementations and contracts.
 */
import {BooleanDisposable} from "@/core/boolean-disposable.js";
import type {Disposable} from "@/core/types.js";
import {toMillis} from "@/schedulers/duration.js";
import {BaseScheduler} from "@/schedulers/base-scheduler.js";
import type {DurationInput} from "@/schedulers/types.js";

/** Scheduler that queues zero-delay tasks with `requestAnimationFrame` when available. */
export class AnimationFrameScheduler extends BaseScheduler {
    /** Creates an animation-frame scheduler. */
    public constructor(name = "animationFrame") {
        super(name);
    }

    /** Schedules a task with `requestAnimationFrame`, falling back to timeout scheduling. */
    public schedule(task: () => void, delay?: DurationInput): Disposable {
        const disposedDisposable = this.disposedIfNeeded();
        if (disposedDisposable) {
            return disposedDisposable;
        }
        const delayMs = toMillis(delay);
        if (delayMs > 0 || typeof requestAnimationFrame !== "function") {
            return this.scheduleTimeout(task, delayMs);
        }
        const id = requestAnimationFrame(() => {
            if (!this.isDisposed()) {
                task();
            }
        });
        return new BooleanDisposable(() => cancelAnimationFrame(id));
    }
}
