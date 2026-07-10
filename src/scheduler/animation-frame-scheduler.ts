/**
 * @packageDocumentation
 * Browser scheduler implementations and contracts.
 */
import {BooleanDisposable, type Disposable} from "@/core/index.js";
import {toMillis} from "@/scheduler/duration.js";
import {BaseScheduler} from "@/scheduler/base-scheduler.js";
import type {DurationInput} from "@/scheduler/types.js";

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
        if (toMillis(delay) > 0 || typeof requestAnimationFrame !== "function") {
            return this.scheduleTimeout(task, delay);
        }
        const id = requestAnimationFrame(() => {
            if (!this.isDisposed()) {
                task();
            }
        });
        return new BooleanDisposable(() => cancelAnimationFrame(id));
    }
}
