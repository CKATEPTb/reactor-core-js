/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import {Flux} from "@/publisher/flux.js";
import {
    type AnyIterable,
    isAsyncIterable,
    toAsyncIterator
} from "@/internal/iterable.js";
import {
    defaultIfEmptyIterable,
    distinctIterable,
    distinctUntilChangedIterable,
    skipIterable,
    skipWhileIterable,
    takeUntilIterable,
    takeWhileIterable
} from "@/internal/iterable-transform.js";
import {identity} from "@/publisher/helpers.js";
import type {PublisherInput} from "@/publisher/types.js";

declare module "@/publisher/flux.js" {
    /** Selection and filtering operators added to Flux. */
    interface Flux<T> {
        /** Emits at most the first `n` source values. */
        take(n: number): Flux<T>;

        /** Emits values while the predicate returns true, then cancels upstream. */
        takeWhile(predicate: (value: T) => boolean): Flux<T>;

        /** Emits values until the predicate returns true, including the matching value. */
        takeUntil(predicate: (value: T) => boolean): Flux<T>;

        /** Drops the first `n` source values. */
        skip(n: number): Flux<T>;

        /** Drops values while the predicate returns true, then relays the rest. */
        skipWhile(predicate: (value: T) => boolean): Flux<T>;

        /** Emits only the first value for each selected key. */
        distinct<K = T>(keySelector?: (value: T) => K): Flux<T>;

        /** Drops adjacent values whose selected key is equal to the previous key. */
        distinctUntilChanged<K = T>(keySelector?: (value: T) => K): Flux<T>;

        /** Emits `defaultValue` when the source completes without values. */
        defaultIfEmpty(defaultValue: T): Flux<T>;

        /** Switches to `alternate` when the source completes without values. */
        switchIfEmpty(alternate: PublisherInput<T>): Flux<T>;
    }
}

Flux.prototype.take = function take<T>(this: Flux<T>, n: number): Flux<T> {
    if (!Number.isInteger(n) || n < 0) {
        throw new RangeError("take expects a non-negative integer");
    }
    if (n === 0) {
        return Flux.empty<T>();
    }
    const source = this;
    return new Flux((signal, context) => {
        const controller = new AbortController();
        const abort = () => controller.abort(signal.reason);
        signal.addEventListener("abort", abort, {once: true});
        let values: AnyIterable<T>;
        try {
            values = source.iterate(controller.signal, context);
        } catch (error) {
            signal.removeEventListener("abort", abort);
            throw error;
        }
        if (!isAsyncIterable<T>(values)) {
            return (function* () {
                let remaining = n;
                try {
                    for (const value of values) {
                        yield value;
                        remaining -= 1;
                        if (remaining === 0) {
                            controller.abort();
                            return;
                        }
                    }
                } finally {
                    signal.removeEventListener("abort", abort);
                    if (signal.aborted && !controller.signal.aborted) {
                        controller.abort(signal.reason);
                    }
                }
            })();
        }
        return (async function* () {
            const iterator = toAsyncIterator(values);
            let remaining = n;
            let cancelledSource = false;
            try {
                while (!signal.aborted) {
                    const result = await iterator.next();
                    if (result.done) {
                        return;
                    }
                    yield result.value;
                    remaining -= 1;
                    if (remaining === 0) {
                        cancelledSource = true;
                        controller.abort();
                        return;
                    }
                }
            } finally {
                signal.removeEventListener("abort", abort);
                if (signal.aborted && !controller.signal.aborted) {
                    controller.abort(signal.reason);
                }
                if (cancelledSource || signal.aborted) {
                    await iterator.return?.();
                }
            }
        })();
    });
};

Flux.prototype.takeWhile = function takeWhile<T>(this: Flux<T>, predicate: (value: T) => boolean): Flux<T> {
    const source = this;
    return new Flux((signal, context) => takeWhileIterable(source.iterate(signal, context), predicate));
};

Flux.prototype.takeUntil = function takeUntil<T>(this: Flux<T>, predicate: (value: T) => boolean): Flux<T> {
    const source = this;
    return new Flux((signal, context) => takeUntilIterable(source.iterate(signal, context), predicate));
};

Flux.prototype.skip = function skip<T>(this: Flux<T>, n: number): Flux<T> {
    if (!Number.isInteger(n) || n < 0) {
        throw new RangeError("skip expects a non-negative integer");
    }
    if (n === 0) {
        return this;
    }
    const source = this;
    return new Flux((signal, context) => skipIterable(source.iterate(signal, context), n));
};

Flux.prototype.skipWhile = function skipWhile<T>(this: Flux<T>, predicate: (value: T) => boolean): Flux<T> {
    const source = this;
    return new Flux((signal, context) => skipWhileIterable(source.iterate(signal, context), predicate));
};

Flux.prototype.distinct = function distinct<T, K = T>(
    this: Flux<T>,
    keySelector: (value: T) => K = identity as (value: T) => K
): Flux<T> {
    const source = this;
    return new Flux((signal, context) => distinctIterable(source.iterate(signal, context), keySelector));
};

Flux.prototype.distinctUntilChanged = function distinctUntilChanged<T, K = T>(
    this: Flux<T>,
    keySelector: (value: T) => K = identity as (value: T) => K
): Flux<T> {
    const source = this;
    return new Flux((signal, context) => distinctUntilChangedIterable(source.iterate(signal, context), keySelector));
};

Flux.prototype.defaultIfEmpty = function defaultIfEmpty<T>(this: Flux<T>, defaultValue: T): Flux<T> {
    const source = this;
    return new Flux((signal, context) => defaultIfEmptyIterable(source.iterate(signal, context), defaultValue));
};

Flux.prototype.switchIfEmpty = function switchIfEmpty<T>(this: Flux<T>, alternate: PublisherInput<T>): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        let empty = true;
        for await (const value of source.iterate(signal, context)) {
            empty = false;
            yield value;
        }
        if (empty) {
            for await (const value of Flux.from(alternate).iterate(signal, context)) {
                yield value;
            }
        }
    });
};
