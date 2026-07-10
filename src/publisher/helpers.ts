/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import type {Disposable} from "@/core/types.js";
import {raceWithAbort} from "@/internal/abort.js";
import {toMillis} from "@/schedulers/duration.js";
import {Schedulers} from "@/schedulers/schedulers.js";
import type {DurationInput, Scheduler} from "@/schedulers/types.js";
import type {Subscriber} from "@/subscription/subscriber.js";

/** Sentinel returned when an iterator race observes a timeout first. */
export const TIMEOUT = Symbol("timeout");

/** Returns the input value unchanged. */
export function identity<T>(value: T): T {
    return value;
}

/** Compact storage for zero, one or many cancellation callbacks. */
export type CancelCallbacks = (() => void) | Set<() => void>;

/** Adds a cancellation callback without allocating a Set for the common single-callback case. */
export function addCancelCallback(callbacks: CancelCallbacks | undefined, callback: () => void): CancelCallbacks {
    if (callbacks === undefined) {
        return callback;
    }
    if (callbacks instanceof Set) {
        callbacks.add(callback);
        return callbacks;
    }
    return new Set([callbacks, callback]);
}

/** Runs every registered cancellation callback and clears multi-callback storage. */
export function runCancelCallbacks(callbacks: CancelCallbacks | undefined): void {
    if (callbacks === undefined) {
        return;
    }
    if (callbacks instanceof Set) {
        for (const callback of callbacks) {
            callback();
        }
        callbacks.clear();
        return;
    }
    callbacks();
}

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
    if (!abortSignal) {
        return new Promise(resolve => {
            scheduler.schedule(resolve, delayMs);
        });
    }
    return new Promise((resolve, reject) => {
        if (abortSignal.aborted) {
            reject(abortSignal.reason);
            return;
        }
        let settled = false;
        let disposable: Disposable | undefined;
        const cleanup = () => {
            abortSignal?.removeEventListener("abort", onAbort);
        };
        const onAbort = () => {
            if (settled) {
                return;
            }
            settled = true;
            disposable?.dispose();
            cleanup();
            reject(abortSignal.reason);
        };
        abortSignal.addEventListener("abort", onAbort, {once: true});
        disposable = scheduler.schedule(() => {
            if (settled) {
                return;
            }
            settled = true;
            cleanup();
            resolve();
        }, delayMs);
        if (settled) {
            disposable.dispose();
        }
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
    const timeoutMs = toMillis(timeout);
    try {
        return await Promise.race<IteratorResult<T> | typeof TIMEOUT>([
            raceWithAbort(iterator.next(), signal),
            new Promise<typeof TIMEOUT>(resolve => {
                timeoutDisposable = scheduler.schedule(() => resolve(TIMEOUT), timeoutMs);
            })
        ]);
    } finally {
        timeoutDisposable?.dispose();
    }
}
