/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import type {Disposable, Subscriber} from "@/core/index.js";
import type {DurationInput, Scheduler} from "@/scheduler/index.js";
import {Schedulers, toMillis} from "@/scheduler/index.js";
import {raceWithAbort} from "@/internal/index.js";

/** Sentinel returned when an iterator race observes a timeout first. */
export const TIMEOUT = Symbol("timeout");

/** Returns true when a value implements the full Subscriber callback shape. */
export function isSubscriber<T>(value: unknown): value is Subscriber<T> {
    return (
        typeof value === "object" &&
        value !== null &&
        typeof (value as Subscriber<T>).onSubscribe === "function" &&
        typeof (value as Subscriber<T>).onNext === "function" &&
        typeof (value as Subscriber<T>).onError === "function" &&
        typeof (value as Subscriber<T>).onComplete === "function"
    );
}

/** Schedules a delay through a scheduler and cancels it when the signal aborts. */
export function scheduleDelay(scheduler: Scheduler, delay: DurationInput, signal?: AbortSignal): Promise<void> {
    const delayMs = toMillis(delay);
    if (delayMs === 0 && scheduler === Schedulers.immediate()) {
        return signal?.aborted ? Promise.reject(signal.reason) : Promise.resolve();
    }
    const abortSignal = signal;
    return new Promise((resolve, reject) => {
        if (abortSignal?.aborted) {
            reject(abortSignal.reason);
            return;
        }
        const disposable = scheduler.schedule(resolve, delay);
        const onAbort = () => {
            disposable.dispose();
            abortSignal?.removeEventListener("abort", onAbort);
            reject(abortSignal?.reason);
        };
        abortSignal?.addEventListener("abort", onAbort, {once: true});
    });
}

/** Races an iterator next result against a scheduler-backed timeout. */
export async function raceIteratorWithTimeout<T>(
    iterator: AsyncIterator<T>,
    timeout: DurationInput,
    scheduler: Scheduler,
    signal: AbortSignal
): Promise<IteratorResult<T> | typeof TIMEOUT> {
    let timeoutDisposable: Disposable | undefined;
    try {
        return await Promise.race<IteratorResult<T> | typeof TIMEOUT>([
            raceWithAbort(iterator.next(), signal),
            new Promise<typeof TIMEOUT>(resolve => {
                timeoutDisposable = scheduler.schedule(() => resolve(TIMEOUT), timeout);
            })
        ]);
    } finally {
        timeoutDisposable?.dispose();
    }
}
