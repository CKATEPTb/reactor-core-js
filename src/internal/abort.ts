/**
 * @packageDocumentation
 * Internal async iteration and cancellation utilities.
 */
import {CancelledError} from "@/errors/index.js";

/** Returns a promise that resolves after `ms` unless the abort signal fires. */
export function abortableDelay(ms: number, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) {
        return Promise.reject(new CancelledError());
    }
    return new Promise((resolve, reject) => {
        const id = setTimeout(done, Math.max(0, ms));
        const onAbort = () => {
            clearTimeout(id);
            signal?.removeEventListener("abort", onAbort);
            reject(new CancelledError());
        };

        function done() {
            signal?.removeEventListener("abort", onAbort);
            resolve();
        }

        signal?.addEventListener("abort", onAbort, {once: true});
    });
}

/** Returns a cancellation error when the signal is already aborted. */
export function abortError(signal?: AbortSignal): CancelledError | undefined {
    return signal?.aborted ? new CancelledError() : undefined;
}

/** Races a promise against an abort signal and rejects with cancellation on abort. */
export function raceWithAbort<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
    if (!signal) {
        return promise;
    }
    if (signal.aborted) {
        return Promise.reject(new CancelledError());
    }
    return new Promise((resolve, reject) => {
        const onAbort = () => reject(new CancelledError());
        signal.addEventListener("abort", onAbort, {once: true});
        promise.then(
            value => {
                signal.removeEventListener("abort", onAbort);
                resolve(value);
            },
            error => {
                signal.removeEventListener("abort", onAbort);
                reject(error);
            }
        );
    });
}

/** Resolves the first emitted value from an async iterable source. */
export async function firstValueFrom<T>(iterable: AsyncIterable<T>, signal?: AbortSignal): Promise<T | undefined> {
    const iterator = iterable[Symbol.asyncIterator]();
    try {
        const result = await raceWithAbort(iterator.next(), signal);
        return result.done ? undefined : result.value;
    } finally {
        await iterator.return?.();
    }
}
