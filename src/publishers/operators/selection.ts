/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import {Flux} from "@/publishers/flux.js";
import {toAsyncIterator} from "@/internal/index.js";
import type {PublisherInput} from "@/publishers/types.js";

declare module "@/publishers/flux.js" {
    /** Selection and filtering operators added to Flux. */
    interface Flux<T> {
        /** Reactor take operator. */
        take(n: number): Flux<T>;

        /** Reactor takeWhile operator. */
        takeWhile(predicate: (value: T) => boolean): Flux<T>;

        /** Reactor takeUntil operator. */
        takeUntil(predicate: (value: T) => boolean): Flux<T>;

        /** Reactor skip operator. */
        skip(n: number): Flux<T>;

        /** Reactor skipWhile operator. */
        skipWhile(predicate: (value: T) => boolean): Flux<T>;

        /** Reactor distinct operator. */
        distinct<K = T>(keySelector?: (value: T) => K): Flux<T>;

        /** Reactor distinctUntilChanged operator. */
        distinctUntilChanged<K = T>(keySelector?: (value: T) => K): Flux<T>;

        /** Reactor defaultIfEmpty operator. */
        defaultIfEmpty(defaultValue: T): Flux<T>;

        /** Reactor switchIfEmpty operator. */
        switchIfEmpty(alternate: PublisherInput<T>): Flux<T>;
    }
}

Flux.prototype.take = function take<T>(this: Flux<T>, n: number): Flux<T> {
    if (!Number.isInteger(n) || n < 0) {
        throw new RangeError("take expects a non-negative integer");
    }
    const source = this;
    return new Flux(async function* (signal, context) {
        if (n === 0) {
            return;
        }
        const controller = new AbortController();
        const abort = () => controller.abort(signal.reason);
        signal.addEventListener("abort", abort, {once: true});
        const iterator = toAsyncIterator(source.iterate(controller.signal, context));
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
    });
};

Flux.prototype.takeWhile = function takeWhile<T>(this: Flux<T>, predicate: (value: T) => boolean): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        for await (const value of source.iterate(signal, context)) {
            if (!predicate(value)) {
                return;
            }
            yield value;
        }
    });
};

Flux.prototype.takeUntil = function takeUntil<T>(this: Flux<T>, predicate: (value: T) => boolean): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        for await (const value of source.iterate(signal, context)) {
            yield value;
            if (predicate(value)) {
                return;
            }
        }
    });
};

Flux.prototype.skip = function skip<T>(this: Flux<T>, n: number): Flux<T> {
    if (!Number.isInteger(n) || n < 0) {
        throw new RangeError("skip expects a non-negative integer");
    }
    const source = this;
    return new Flux(async function* (signal, context) {
        let skipped = 0;
        for await (const value of source.iterate(signal, context)) {
            if (skipped < n) {
                skipped += 1;
            } else {
                yield value;
            }
        }
    });
};

Flux.prototype.skipWhile = function skipWhile<T>(this: Flux<T>, predicate: (value: T) => boolean): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        let skipping = true;
        for await (const value of source.iterate(signal, context)) {
            if (skipping && predicate(value)) {
                continue;
            }
            skipping = false;
            yield value;
        }
    });
};

Flux.prototype.distinct = function distinct<T, K = T>(
    this: Flux<T>,
    keySelector: (value: T) => K = value => value as unknown as K
): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        const seen = new Set<K>();
        for await (const value of source.iterate(signal, context)) {
            const key = keySelector(value);
            if (!seen.has(key)) {
                seen.add(key);
                yield value;
            }
        }
    });
};

Flux.prototype.distinctUntilChanged = function distinctUntilChanged<T, K = T>(
    this: Flux<T>,
    keySelector: (value: T) => K = value => value as unknown as K
): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        let hasPrevious = false;
        let previous: K | undefined;
        for await (const value of source.iterate(signal, context)) {
            const key = keySelector(value);
            if (!hasPrevious || !Object.is(previous, key)) {
                hasPrevious = true;
                previous = key;
                yield value;
            }
        }
    });
};

Flux.prototype.defaultIfEmpty = function defaultIfEmpty<T>(this: Flux<T>, defaultValue: T): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        let empty = true;
        for await (const value of source.iterate(signal, context)) {
            empty = false;
            yield value;
        }
        if (empty) {
            yield defaultValue;
        }
    });
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
