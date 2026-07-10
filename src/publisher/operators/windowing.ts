/**
 * @packageDocumentation
 * Advanced Reactor buffer and window compatibility operators for Flux.
 */
import {Context} from "@/context/context.js";
import {AsyncQueue} from "@/internal/async-queue.js";
import {toAsyncIterator} from "@/internal/iterable.js";
import {Flux} from "@/publisher/flux.js";
import {identity, raceIteratorWithTimeout, TIMEOUT} from "@/publisher/helpers.js";
import type {PublisherInput} from "@/publisher/types.js";
import {Schedulers} from "@/schedulers/schedulers.js";
import type {DurationInput, Scheduler} from "@/schedulers/types.js";

declare module "@/publisher/flux.js" {
    /** Advanced buffer and window operators added to Flux. */
    interface Flux<T> {
        /** Buffers values until `maxSize` is reached or `maxTime` elapses after the first buffered value. */
        bufferTimeout(maxSize: number, maxTime: DurationInput, scheduler?: Scheduler): Flux<T[]>;

        /** Buffers values until the predicate matches, optionally cutting before the matching value. */
        bufferUntil(predicate: (value: T) => boolean, cutBefore?: boolean): Flux<T[]>;

        /** Buffers adjacent values while the selected key stays equal. */
        bufferUntilChanged<K = T>(keySelector?: (value: T) => K, comparator?: (left: K, right: K) => boolean): Flux<T[]>;

        /** Buffers values between an opening signal and the corresponding closing publisher. */
        bufferWhen(openings: PublisherInput<unknown>, closeSelector: (opening: unknown) => PublisherInput<unknown>): Flux<T[]>;

        /** Buffers consecutive values while the predicate matches. */
        bufferWhile(predicate: (value: T) => boolean): Flux<T[]>;

        /** Windows values until `maxSize` is reached or `maxTime` elapses after the first value. */
        windowTimeout(maxSize: number, maxTime: DurationInput, scheduler?: Scheduler): Flux<Flux<T>>;

        /** Windows values until the predicate matches, optionally cutting before the matching value. */
        windowUntil(predicate: (value: T) => boolean, cutBefore?: boolean): Flux<Flux<T>>;

        /** Windows adjacent values while the selected key stays equal. */
        windowUntilChanged<K = T>(keySelector?: (value: T) => K, comparator?: (left: K, right: K) => boolean): Flux<Flux<T>>;

        /** Windows values between an opening signal and the corresponding closing publisher. */
        windowWhen(openings: PublisherInput<unknown>, closeSelector: (opening: unknown) => PublisherInput<unknown>): Flux<Flux<T>>;

        /** Windows consecutive values while the predicate matches. */
        windowWhile(predicate: (value: T) => boolean): Flux<Flux<T>>;
    }
}

Flux.prototype.bufferTimeout = function bufferTimeout<T>(
    this: Flux<T>,
    maxSize: number,
    maxTime: DurationInput,
    scheduler: Scheduler = Schedulers.timeout()
): Flux<T[]> {
    if (!Number.isInteger(maxSize) || maxSize <= 0) {
        throw new RangeError("bufferTimeout expects a strictly positive integer maxSize");
    }
    const source = this;
    return new Flux(async function* (signal, context) {
        const iterator = toAsyncIterator(source.iterate(signal, context));
        let bucket: T[] = [];
        try {
            while (!signal.aborted) {
                const result =
                    bucket.length === 0
                        ? await iterator.next()
                        : await raceIteratorWithTimeout(iterator, maxTime, scheduler, signal);
                if (result === TIMEOUT) {
                    if (bucket.length > 0) {
                        yield bucket;
                        bucket = [];
                    }
                    continue;
                }
                if (result.done) {
                    break;
                }
                bucket.push(result.value);
                if (bucket.length >= maxSize) {
                    yield bucket;
                    bucket = [];
                }
            }
            if (bucket.length > 0) {
                yield bucket;
            }
        } finally {
            await iterator.return?.();
        }
    });
};

Flux.prototype.bufferUntil = function bufferUntil<T>(
    this: Flux<T>,
    predicate: (value: T) => boolean,
    cutBefore = false
): Flux<T[]> {
    const source = this;
    return new Flux(async function* (signal, context) {
        let bucket: T[] = [];
        for await (const value of source.iterate(signal, context)) {
            if (cutBefore && predicate(value)) {
                if (bucket.length > 0) {
                    yield bucket;
                }
                bucket = [value];
                continue;
            }
            bucket.push(value);
            if (!cutBefore && predicate(value)) {
                yield bucket;
                bucket = [];
            }
        }
        if (bucket.length > 0) {
            yield bucket;
        }
    });
};

Flux.prototype.bufferUntilChanged = function bufferUntilChanged<T, K = T>(
    this: Flux<T>,
    keySelector: (value: T) => K = identity as (value: T) => K,
    comparator: (left: K, right: K) => boolean = Object.is
): Flux<T[]> {
    const source = this;
    return new Flux(async function* (signal, context) {
        let bucket: T[] = [];
        let previousKey: K | undefined;
        let hasKey = false;
        for await (const value of source.iterate(signal, context)) {
            const key = keySelector(value);
            if (hasKey && !comparator(previousKey as K, key)) {
                yield bucket;
                bucket = [];
            }
            bucket.push(value);
            previousKey = key;
            hasKey = true;
        }
        if (bucket.length > 0) {
            yield bucket;
        }
    });
};

Flux.prototype.bufferWhen = function bufferWhen<T>(
    this: Flux<T>,
    openings: PublisherInput<unknown>,
    closeSelector: (opening: unknown) => PublisherInput<unknown>
): Flux<T[]> {
    const source = this;
    return new Flux(async function* (signal, context) {
        const opening = await firstOpening(openings, signal, context);
        if (opening.done) {
            return;
        }
        const closeSignal = Symbol("close");
        const close: Promise<typeof closeSignal> = firstOpening(closeSelector(opening.value), signal, context).then(() => closeSignal);
        const iterator = toAsyncIterator(source.iterate(signal, context));
        const bucket: T[] = [];
        try {
            while (!signal.aborted) {
                const result = await Promise.race<IteratorResult<T> | typeof closeSignal>([iterator.next(), close]);
                if (result === closeSignal || result.done) {
                    break;
                }
                bucket.push(result.value);
            }
        } finally {
            await iterator.return?.();
        }
        if (bucket.length > 0) {
            yield bucket;
        }
    });
};

Flux.prototype.bufferWhile = function bufferWhile<T>(this: Flux<T>, predicate: (value: T) => boolean): Flux<T[]> {
    const source = this;
    return new Flux(async function* (signal, context) {
        let bucket: T[] = [];
        for await (const value of source.iterate(signal, context)) {
            if (predicate(value)) {
                bucket.push(value);
            } else if (bucket.length > 0) {
                yield bucket;
                bucket = [];
            }
        }
        if (bucket.length > 0) {
            yield bucket;
        }
    });
};

Flux.prototype.windowTimeout = function windowTimeout<T>(
    this: Flux<T>,
    maxSize: number,
    maxTime: DurationInput,
    scheduler?: Scheduler
): Flux<Flux<T>> {
    return windowsFromBuffers(this.bufferTimeout(maxSize, maxTime, scheduler));
};

Flux.prototype.windowUntil = function windowUntil<T>(
    this: Flux<T>,
    predicate: (value: T) => boolean,
    cutBefore?: boolean
): Flux<Flux<T>> {
    return windowsFromBuffers(this.bufferUntil(predicate, cutBefore));
};

Flux.prototype.windowUntilChanged = function windowUntilChanged<T, K = T>(
    this: Flux<T>,
    keySelector?: (value: T) => K,
    comparator?: (left: K, right: K) => boolean
): Flux<Flux<T>> {
    return windowsFromBuffers(this.bufferUntilChanged(keySelector, comparator));
};

Flux.prototype.windowWhen = function windowWhen<T>(
    this: Flux<T>,
    openings: PublisherInput<unknown>,
    closeSelector: (opening: unknown) => PublisherInput<unknown>
): Flux<Flux<T>> {
    return windowsFromBuffers(this.bufferWhen(openings, closeSelector));
};

Flux.prototype.windowWhile = function windowWhile<T>(this: Flux<T>, predicate: (value: T) => boolean): Flux<Flux<T>> {
    return windowsFromBuffers(this.bufferWhile(predicate));
};

/** Resolves the first opening or closing signal from a publisher input. */
async function firstOpening(source: PublisherInput<unknown>, signal: AbortSignal, context: Context): Promise<IteratorResult<unknown>> {
    const iterator = toAsyncIterator(Flux.from(source).iterate(signal, context));
    try {
        return await iterator.next();
    } finally {
        await iterator.return?.();
    }
}

/** Converts buffered arrays into window Flux values without yielding thenable publishers from an async generator. */
function windowsFromBuffers<T>(buffers: Flux<T[]>): Flux<Flux<T>> {
    return new Flux((signal, context) => {
        const queue = new AsyncQueue<Flux<T>>(signal);
        void (async () => {
            try {
                for await (const values of buffers.iterate(signal, context)) {
                    queue.push(Flux.fromIterable(values));
                }
                queue.complete();
            } catch (error) {
                queue.error(error);
            }
        })();
        return queue;
    });
}
