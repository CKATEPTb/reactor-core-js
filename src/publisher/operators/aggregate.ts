/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import {NoSuchElementError} from "@/errors/classes.js";
import {AsyncQueue} from "@/internal/async-queue.js";
import {type AnyIterable, isAsyncIterable} from "@/internal/iterable.js";
import {collectIterable, countIterable} from "@/internal/iterable-terminal.js";
import {Flux} from "@/publisher/flux.js";
import {Mono} from "@/publisher/mono.js";

/** Sentinel used when a terminal operation completes without emitting. */
const NO_VALUE = Symbol("no-value");

/** Terminal operation result that may represent an empty Mono. */
type MaybeValue<T> = T | typeof NO_VALUE;

declare module "@/publisher/flux.js" {
    /** Aggregate and terminal count-style operators added to Flux. */
    interface Flux<T> {
        /** Groups source values into arrays of `size`, with a final partial buffer when needed. */
        buffer(size: number): Flux<T[]>;

        /** Splits source values into finite Flux windows containing up to `size` values each. */
        window(size: number): Flux<Flux<T>>;

        /** Emits the seed and each accumulated value produced from the source. */
        scan<R>(seed: R, accumulator: (accumulated: R, value: T) => R): Flux<R>;

        /** Emits running accumulated values, using the first source value as the initial state. */
        scan(accumulator: (accumulated: T, value: T) => T): Flux<T>;

        /** Reduces source values into one value, using the first value as the initial state. */
        reduce(accumulator: (left: T, right: T) => T): Mono<T>;

        /** Reduces source values into one value starting from the provided seed. */
        reduce<R>(seed: R, accumulator: (left: R, right: T) => R): Mono<R>;

        /** Collects all source values into an array emitted after completion. */
        collectList(): Mono<T[]>;

        /** Counts source values and emits the final count after completion. */
        count(): Mono<number>;

        /** Emits true when at least one source value matches the predicate. */
        any(predicate: (value: T) => boolean): Mono<boolean>;

        /** Emits true only when every source value matches the predicate. */
        all(predicate: (value: T) => boolean): Mono<boolean>;

        /** Emits true when a source value matches `value` by `Object.is`. */
        hasElement(value: T): Mono<boolean>;

        /** Emits the first source value, or completes empty when the source has none. */
        next(): Mono<T>;

        /** Emits the only source value, an optional default for empty sources, or fails on multiple values. */
        single(defaultValue?: T): Mono<T>;

        /** Emits the only source value, completes empty for empty sources, or fails on multiple values. */
        singleOrEmpty(): Mono<T>;
    }
}

Flux.prototype.buffer = function buffer<T>(this: Flux<T>, size: number): Flux<T[]> {
    if (!Number.isInteger(size) || size <= 0) {
        throw new RangeError("buffer size must be a strictly positive integer");
    }
    const source = this;
    return new Flux(async function* (signal, context) {
        let bucket: T[] = [];
        for await (const value of source.iterate(signal, context)) {
            bucket.push(value);
            if (bucket.length === size) {
                yield bucket;
                bucket = [];
            }
        }
        if (bucket.length > 0) {
            yield bucket;
        }
    });
};

Flux.prototype.window = function window<T>(this: Flux<T>, size: number): Flux<Flux<T>> {
    const buffers = this.buffer(size);
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
};

Flux.prototype.scan = function scan<T, R>(
    this: Flux<T>,
    seedOrAccumulator: R | ((accumulated: T, value: T) => T),
    accumulator?: (accumulated: R, value: T) => R
): Flux<R | T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        if (accumulator) {
            let current = seedOrAccumulator as R;
            yield current;
            for await (const value of source.iterate(signal, context)) {
                current = accumulator(current, value);
                yield current;
            }
        } else {
            let initialized = false;
            let current: T | undefined;
            const acc = seedOrAccumulator as (accumulated: T, value: T) => T;
            for await (const value of source.iterate(signal, context)) {
                if (!initialized) {
                    current = value;
                    initialized = true;
                    yield current;
                } else {
                    current = acc(current as T, value);
                    yield current;
                }
            }
        }
    });
};

Flux.prototype.reduce = function reduce<T, R>(
    this: Flux<T>,
    seedOrAccumulator: R | ((left: T, right: T) => T),
    accumulator?: (left: R, right: T) => R
): Mono<R | T> {
    return terminalMono(this, values => accumulator
        ? reduceSeed(values, seedOrAccumulator as R, accumulator)
        : reduceNoSeed(values, seedOrAccumulator as (left: T, right: T) => T));
};

Flux.prototype.collectList = function collectList<T>(this: Flux<T>): Mono<T[]> {
    return terminalMono(this, collectIterable);
};

Flux.prototype.count = function count<T>(this: Flux<T>): Mono<number> {
    return terminalMono(this, countIterable);
};

Flux.prototype.any = function any<T>(this: Flux<T>, predicate: (value: T) => boolean): Mono<boolean> {
    return terminalMono(this, values => anyValue(values, predicate));
};

Flux.prototype.all = function all<T>(this: Flux<T>, predicate: (value: T) => boolean): Mono<boolean> {
    return terminalMono(this, values => allValue(values, predicate));
};

Flux.prototype.hasElement = function hasElement<T>(this: Flux<T>, value: T): Mono<boolean> {
    return this.any(next => Object.is(next, value));
};

Flux.prototype.next = function next<T>(this: Flux<T>): Mono<T> {
    return terminalMono(this, firstValue);
};

Flux.prototype.single = function single<T>(this: Flux<T>, defaultValue?: T): Mono<T> {
    const hasDefault = arguments.length > 0;
    return terminalMono(this, values => singleValue(values, false, hasDefault, defaultValue));
};

Flux.prototype.singleOrEmpty = function singleOrEmpty<T>(this: Flux<T>): Mono<T> {
    return terminalMono(this, values => singleValue<T>(values, true));
};

/** Creates a Mono from a terminal iterable computation while preserving sync sources. */
function terminalMono<T, R>(
    source: Flux<T>,
    evaluate: (values: AnyIterable<T>) => MaybeValue<R> | Promise<MaybeValue<R>>
): Mono<R> {
    return new Mono((signal, context) => {
        const values = source.iterate(signal, context);
        return isAsyncIterable<T>(values) ? emitAsync(() => evaluate(values)) : emitSync(() => evaluate(values) as MaybeValue<R>);
    });
}

/** Emits a terminal sync value unless it represents an empty Mono. */
function emitSync<T>(producer: () => MaybeValue<T>): Iterable<T> {
    return (function* () {
        const value = producer();
        if (value !== NO_VALUE) {
            yield value as T;
        }
    })();
}

/** Emits a terminal async value unless it represents an empty Mono. */
function emitAsync<T>(producer: () => MaybeValue<T> | Promise<MaybeValue<T>>): AsyncIterable<T> {
    return (async function* () {
        const value = await producer();
        if (value !== NO_VALUE) {
            yield value as T;
        }
    })();
}

/** Reduces values with an explicit seed. */
function reduceSeed<T, R>(
    values: AnyIterable<T>,
    seed: R,
    accumulator: (left: R, right: T) => R
): R | Promise<R> {
    if (isAsyncIterable<T>(values)) {
        return (async () => {
            let current = seed;
            for await (const value of values) {
                current = accumulator(current, value);
            }
            return current;
        })();
    }
    let current = seed;
    for (const value of values) {
        current = accumulator(current, value);
    }
    return current;
}

/** Reduces values using the first value as the seed. */
function reduceNoSeed<T>(
    values: AnyIterable<T>,
    accumulator: (left: T, right: T) => T
): MaybeValue<T> | Promise<MaybeValue<T>> {
    if (isAsyncIterable<T>(values)) {
        return (async () => {
            let initialized = false;
            let current: T | undefined;
            for await (const value of values) {
                current = initialized ? accumulator(current as T, value) : value;
                initialized = true;
            }
            return initialized ? current as T : NO_VALUE;
        })();
    }
    let initialized = false;
    let current: T | undefined;
    for (const value of values) {
        current = initialized ? accumulator(current as T, value) : value;
        initialized = true;
    }
    return initialized ? current as T : NO_VALUE;
}

/** Returns true when any value matches the predicate. */
function anyValue<T>(values: AnyIterable<T>, predicate: (value: T) => boolean): boolean | Promise<boolean> {
    if (isAsyncIterable<T>(values)) {
        return (async () => {
            for await (const value of values) {
                if (predicate(value)) {
                    return true;
                }
            }
            return false;
        })();
    }
    for (const value of values) {
        if (predicate(value)) {
            return true;
        }
    }
    return false;
}

/** Returns true when every value matches the predicate. */
function allValue<T>(values: AnyIterable<T>, predicate: (value: T) => boolean): boolean | Promise<boolean> {
    if (isAsyncIterable<T>(values)) {
        return (async () => {
            for await (const value of values) {
                if (!predicate(value)) {
                    return false;
                }
            }
            return true;
        })();
    }
    for (const value of values) {
        if (!predicate(value)) {
            return false;
        }
    }
    return true;
}

/** Returns the first value and closes the source iterator early. */
function firstValue<T>(values: AnyIterable<T>): MaybeValue<T> | Promise<MaybeValue<T>> {
    if (isAsyncIterable<T>(values)) {
        return (async () => {
            const iterator = values[Symbol.asyncIterator]();
            try {
                const next = await iterator.next();
                return next.done ? NO_VALUE : next.value;
            } finally {
                await iterator.return?.();
            }
        })();
    }
    const iterator = values[Symbol.iterator]();
    try {
        const next = iterator.next();
        return next.done ? NO_VALUE : next.value;
    } finally {
        iterator.return?.();
    }
}

/** Returns the only value, a default value or the empty sentinel. */
function singleValue<T>(
    values: AnyIterable<T>,
    emptyIsAllowed: boolean,
    hasDefault = false,
    defaultValue?: T
): MaybeValue<T> | Promise<MaybeValue<T>> {
    if (isAsyncIterable<T>(values)) {
        return (async () => singleResult(await collectSingle(values), emptyIsAllowed, hasDefault, defaultValue))();
    }
    let seen = false;
    let result: T | undefined;
    for (const value of values) {
        if (seen) {
            throw new Error("Source emitted more than one item");
        }
        seen = true;
        result = value;
    }
    return singleResult(seen ? result as T : NO_VALUE, emptyIsAllowed, hasDefault, defaultValue);
}

/** Collects exactly one async value or returns the empty sentinel. */
async function collectSingle<T>(values: AsyncIterable<T>): Promise<MaybeValue<T>> {
    let seen = false;
    let result: T | undefined;
    for await (const value of values) {
        if (seen) {
            throw new Error("Source emitted more than one item");
        }
        seen = true;
        result = value;
    }
    return seen ? result as T : NO_VALUE;
}

/** Resolves single-value empty/default behavior. */
function singleResult<T>(
    value: MaybeValue<T>,
    emptyIsAllowed: boolean,
    hasDefault: boolean,
    defaultValue: T | undefined
): MaybeValue<T> {
    if (value !== NO_VALUE) {
        return value;
    }
    if (hasDefault) {
        return defaultValue as T;
    }
    if (emptyIsAllowed) {
        return NO_VALUE;
    }
    throw new NoSuchElementError();
}
