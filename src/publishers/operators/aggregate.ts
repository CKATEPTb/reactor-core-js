/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import {NoSuchElementError} from "@/errors/index.js";
import {AsyncQueue} from "@/internal/index.js";
import {Flux} from "@/publishers/flux.js";
import {Mono} from "@/publishers/mono.js";

declare module "@/publishers/flux.js" {
    /** Aggregate and terminal count-style operators added to Flux. */
    interface Flux<T> {
        /** Reactor buffer operator. */
        buffer(size: number): Flux<T[]>;

        /** Reactor window operator. */
        window(size: number): Flux<Flux<T>>;

        /** Reactor scan operator. */
        scan<R>(seed: R, accumulator: (accumulated: R, value: T) => R): Flux<R>;

        /** Reactor scan operator. */
        scan(accumulator: (accumulated: T, value: T) => T): Flux<T>;

        /** Reactor reduce operator. */
        reduce(accumulator: (left: T, right: T) => T): Mono<T>;

        /** Reactor reduce operator. */
        reduce<R>(seed: R, accumulator: (left: R, right: T) => R): Mono<R>;

        /** Reactor collectList operator. */
        collectList(): Mono<T[]>;

        /** Reactor count operator. */
        count(): Mono<number>;

        /** Reactor any operator. */
        any(predicate: (value: T) => boolean): Mono<boolean>;

        /** Reactor all operator. */
        all(predicate: (value: T) => boolean): Mono<boolean>;

        /** Reactor hasElement operator. */
        hasElement(value: T): Mono<boolean>;

        /** Reactor next operator. */
        next(): Mono<T>;

        /** Reactor single operator. */
        single(defaultValue?: T): Mono<T>;

        /** Reactor singleOrEmpty operator. */
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
    const source = this;
    return new Mono(async function* (signal, context) {
        if (accumulator) {
            let current = seedOrAccumulator as R;
            for await (const value of source.iterate(signal, context)) {
                current = accumulator(current, value);
            }
            yield current;
        } else {
            let initialized = false;
            let current: T | undefined;
            const acc = seedOrAccumulator as (left: T, right: T) => T;
            for await (const value of source.iterate(signal, context)) {
                current = initialized ? acc(current as T, value) : value;
                initialized = true;
            }
            if (initialized) {
                yield current as T;
            }
        }
    });
};

Flux.prototype.collectList = function collectList<T>(this: Flux<T>): Mono<T[]> {
    const source = this;
    return new Mono(async function* (signal, context) {
        const values: T[] = [];
        for await (const value of source.iterate(signal, context)) {
            values.push(value);
        }
        yield values;
    });
};

Flux.prototype.count = function count<T>(this: Flux<T>): Mono<number> {
    const source = this;
    return new Mono(async function* (signal, context) {
        let count = 0;
        for await (const _ of source.iterate(signal, context)) {
            count += 1;
        }
        yield count;
    });
};

Flux.prototype.any = function any<T>(this: Flux<T>, predicate: (value: T) => boolean): Mono<boolean> {
    const source = this;
    return new Mono(async function* (signal, context) {
        for await (const value of source.iterate(signal, context)) {
            if (predicate(value)) {
                yield true;
                return;
            }
        }
        yield false;
    });
};

Flux.prototype.all = function all<T>(this: Flux<T>, predicate: (value: T) => boolean): Mono<boolean> {
    const source = this;
    return new Mono(async function* (signal, context) {
        for await (const value of source.iterate(signal, context)) {
            if (!predicate(value)) {
                yield false;
                return;
            }
        }
        yield true;
    });
};

Flux.prototype.hasElement = function hasElement<T>(this: Flux<T>, value: T): Mono<boolean> {
    return this.any(next => Object.is(next, value));
};

Flux.prototype.next = function next<T>(this: Flux<T>): Mono<T> {
    const source = this;
    return new Mono(async function* (signal, context) {
        for await (const value of source.iterate(signal, context)) {
            yield value;
            return;
        }
    });
};

Flux.prototype.single = function single<T>(this: Flux<T>, defaultValue?: T): Mono<T> {
    const source = this;
    const hasDefault = arguments.length > 0;
    return new Mono(async function* (signal, context) {
        let seen = false;
        let singleValue: T | undefined;
        for await (const value of source.iterate(signal, context)) {
            if (seen) {
                throw new Error("Source emitted more than one item");
            }
            seen = true;
            singleValue = value;
        }
        if (seen) {
            yield singleValue as T;
        } else if (hasDefault) {
            yield defaultValue as T;
        } else {
            throw new NoSuchElementError();
        }
    });
};

Flux.prototype.singleOrEmpty = function singleOrEmpty<T>(this: Flux<T>): Mono<T> {
    const source = this;
    return new Mono(async function* (signal, context) {
        let seen = false;
        let singleValue: T | undefined;
        for await (const value of source.iterate(signal, context)) {
            if (seen) {
                throw new Error("Source emitted more than one item");
            }
            seen = true;
            singleValue = value;
        }
        if (seen) {
            yield singleValue as T;
        }
    });
};
