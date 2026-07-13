/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import {NoSuchElementError} from "@/errors/classes.js";
import {UNBOUNDED_DEMAND} from "@/core/demand.js";
import {AsyncQueue} from "@/internal/async-queue.js";
import {Flux} from "@/publisher/flux.js";
import {Mono} from "@/publisher/mono.js";
import {liftBuffer} from "@/publisher/operators/buffer-lift.js";
import {liftOneToOne} from "@/publisher/operators/lift.js";
import {NO_TERMINAL_VALUE, terminalMono} from "@/publisher/operators/terminal-mono.js";

/** Sentinel used when a terminal operation completes without emitting. */
const NO_VALUE: typeof NO_TERMINAL_VALUE = NO_TERMINAL_VALUE;

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
    return liftBuffer(
        source,
        async function* (signal, context) {
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
        },
        size
    );
};

Flux.prototype.window = function window<T>(this: Flux<T>, size: number): Flux<Flux<T>> {
    const buffers = this.buffer(size);
    return liftOneToOne(
        buffers,
        (signal, context) => {
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
        },
        () => ({onNext: values => Flux.fromIterable(values)})
    );
};

Flux.prototype.scan = function scan<T, R>(
    this: Flux<T>,
    seedOrAccumulator: R | ((accumulated: T, value: T) => T),
    accumulator?: (accumulated: R, value: T) => R
): Flux<R | T> {
    const source = this;
    if (accumulator) {
        return new Flux(async function* (signal, context) {
            let current = seedOrAccumulator as R;
            yield current;
            for await (const value of source.iterate(signal, context)) {
                current = accumulator(current, value);
                yield current;
            }
        });
    }
    const reduceValues = seedOrAccumulator as (accumulated: T, value: T) => T;
    return liftOneToOne(
        source,
        async function* (signal, context) {
            let initialized = false;
            let current: T | undefined;
            for await (const value of source.iterate(signal, context)) {
                if (!initialized) {
                    current = value;
                    initialized = true;
                    yield current;
                } else {
                    current = reduceValues(current as T, value);
                    yield current;
                }
            }
        },
        () => {
            let initialized = false;
            let current: T | undefined;
            return {
                /** Produces the next running accumulation. */
                onNext(value: T) {
                    current = initialized ? reduceValues(current as T, value) : value;
                    initialized = true;
                    return current;
                }
            };
        }
    );
};

Flux.prototype.reduce = function reduce<T, R>(
    this: Flux<T>,
    seedOrAccumulator: R | ((left: T, right: T) => T),
    accumulator?: (left: R, right: T) => R
): Mono<R | T> {
    if (accumulator) {
        const seed = seedOrAccumulator as R;
        return terminalMono<T, R | T>(this, UNBOUNDED_DEMAND, () => {
            let current = seed;
            return {
                /** Folds one source value into the seeded result. */
                onNext(value) {
                    current = accumulator(current, value);
                    return false;
                },
                result: () => current
            };
        });
    }
    const reduceValues = seedOrAccumulator as (left: T, right: T) => T;
    return terminalMono<T, R | T>(this, UNBOUNDED_DEMAND, () => {
        let initialized = false;
        let current: T | undefined;
        return {
            /** Uses the first value as the seed and folds later values. */
            onNext(value) {
                current = initialized ? reduceValues(current as T, value) : value;
                initialized = true;
                return false;
            },
            result: () => initialized ? current as T : NO_VALUE
        };
    });
};

Flux.prototype.collectList = function collectList<T>(this: Flux<T>): Mono<T[]> {
    return terminalMono(this, UNBOUNDED_DEMAND, () => {
        const values: T[] = [];
        return {
            /** Appends one value to the collected list. */
            onNext(value) {
                values.push(value);
                return false;
            },
            result: () => values
        };
    });
};

Flux.prototype.count = function count<T>(this: Flux<T>): Mono<number> {
    return terminalMono(this, UNBOUNDED_DEMAND, () => {
        let count = 0;
        return {
            /** Counts one source value. */
            onNext() {
                count += 1;
                return false;
            },
            result: () => count
        };
    });
};

Flux.prototype.any = function any<T>(this: Flux<T>, predicate: (value: T) => boolean): Mono<boolean> {
    return terminalMono(this, UNBOUNDED_DEMAND, () => {
        let matched = false;
        return {
            /** Tests one value and finishes on the first match. */
            onNext(value) {
                matched = predicate(value);
                return matched;
            },
            result: () => matched
        };
    });
};

Flux.prototype.all = function all<T>(this: Flux<T>, predicate: (value: T) => boolean): Mono<boolean> {
    return terminalMono(this, UNBOUNDED_DEMAND, () => {
        let matched = true;
        return {
            /** Tests one value and finishes on the first mismatch. */
            onNext(value) {
                matched = predicate(value);
                return !matched;
            },
            result: () => matched
        };
    });
};

Flux.prototype.hasElement = function hasElement<T>(this: Flux<T>, value: T): Mono<boolean> {
    return this.any(next => Object.is(next, value));
};

Flux.prototype.next = function next<T>(this: Flux<T>): Mono<T> {
    return terminalMono<T, T>(this, 1, () => {
        let result: MaybeValue<T> = NO_VALUE;
        return {
            /** Captures the first value and finishes immediately. */
            onNext(value) {
                result = value;
                return true;
            },
            result: () => result
        };
    });
};

Flux.prototype.single = function single<T>(this: Flux<T>, defaultValue?: T): Mono<T> {
    const hasDefault = arguments.length > 0;
    return singleTerminal(this, false, hasDefault, defaultValue);
};

Flux.prototype.singleOrEmpty = function singleOrEmpty<T>(this: Flux<T>): Mono<T> {
    return singleTerminal(this, true, false);
};

/** Creates a bounded terminal Mono that validates a source has at most one value. */
function singleTerminal<T>(
    source: Flux<T>,
    emptyIsAllowed: boolean,
    hasDefault = false,
    defaultValue?: T
): Mono<T> {
    return terminalMono<T, T>(source, 2, () => {
        let seen = false;
        let value: T | undefined;
        return {
            /** Captures one value and rejects a second source value. */
            onNext(next) {
                if (seen) {
                    throw new Error("Source emitted more than one item");
                }
                seen = true;
                value = next;
                return false;
            },
            result: () => singleResult(seen ? value as T : NO_VALUE, emptyIsAllowed, hasDefault, defaultValue)
        };
    });
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
