/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import {UNBOUNDED_DEMAND} from "@/core/demand.js";
import {NoSuchElementError} from "@/errors/classes.js";
import {consumePublisher} from "@/internal/publisher-terminal.js";
import {Flux} from "@/publisher/flux.js";
import {identity} from "@/publisher/helpers.js";
import {Mono} from "@/publisher/mono.js";
import {liftOneToOne} from "@/publisher/operators/lift.js";
import {terminalMono} from "@/publisher/operators/terminal-mono.js";
import type {PublisherInput} from "@/publisher/types.js";
import type {Subscriber} from "@/subscription/subscriber.js";

/** Constructor-like runtime value used by `ofType`. */
type ClassLike<R> = abstract new (...args: never[]) => R;

declare module "@/publisher/flux.js" {
    /** Reactor compatibility operators added to Flux. */
    interface Flux<T> {
        /** Passes this Flux to a transformer and returns the transformer's result. */
        as<R>(transformer: (source: Flux<T>) => R): R;

        /** Resolves with the first value, or undefined when the source completes empty. */
        blockFirst(): Promise<T | undefined>;

        /** Resolves with the last value, or undefined when the source completes empty. */
        blockLast(): Promise<T | undefined>;

        /** Creates a container per subscription and accumulates every source value into it. */
        collect<R>(supplier: () => R, accumulator: (container: R, value: T) => void): Mono<R>;

        /** Collects source values into a Map using extracted keys and optional mapped values. */
        collectMap<K, V = T>(keyExtractor: (value: T) => K, valueExtractor?: (value: T) => V): Mono<Map<K, V>>;

        /** Collects source values into a Map whose keys point to arrays of matching values. */
        collectMultimap<K, V = T>(keyExtractor: (value: T) => K, valueExtractor?: (value: T) => V): Mono<Map<K, V[]>>;

        /** Collects all source values into an array and sorts it before emitting. */
        collectSortedList(comparator?: (left: T, right: T) => number): Mono<T[]>;

        /** Appends literal values after this Flux completes. */
        concatWithValues(...values: readonly T[]): Flux<T>;

        /** Emits the value at a zero-based index, a default, or an error when missing. */
        elementAt(index: number, defaultValue?: T): Mono<T>;

        /** Emits true when the source emits at least one value. */
        hasElements(): Mono<boolean>;

        /** Wraps this source in a new Flux to hide its concrete identity. */
        hide(): Flux<T>;

        /** Drops all values and completes when the source completes successfully. */
        ignoreElements(): Mono<void>;

        /** Emits the final source value, an optional default, or an error when empty. */
        last(defaultValue?: T): Mono<T>;

        /** Maps each value and drops null or undefined mapping results. */
        mapNotNull<R>(mapper: (value: T) => R | null | undefined): Flux<R>;

        /** Keeps only values that are instances of the provided runtime class. */
        ofType<R>(type: ClassLike<R>): Flux<R>;

        /** Creates an initial state per subscription and reduces source values into it. */
        reduceWith<R>(supplier: () => R, accumulator: (accumulated: R, value: T) => R): Mono<R>;

        /** Creates an initial state per subscription and emits each running accumulation. */
        scanWith<R>(supplier: () => R, accumulator: (accumulated: R, value: T) => R): Flux<R>;

        /** Drops the final `n` values from the source. */
        skipLast(n: number): Flux<T>;

        /** Collects all source values, sorts them, and replays the sorted values. */
        sort(comparator?: (left: T, right: T) => number): Flux<T>;

        /** Subscribes the provided subscriber and returns the same subscriber instance. */
        subscribeWith<S extends Subscriber<T>>(subscriber: S): S;

        /** Emits only the final `n` values after the source completes. */
        takeLast(n: number): Flux<T>;

        /** Waits for this source and then waits for `other`, ignoring all values. */
        thenEmpty(other: PublisherInput<unknown>): Mono<void>;
    }
}


Flux.prototype.as = function as<T, R>(this: Flux<T>, transformer: (source: Flux<T>) => R): R {
    return transformer(this);
};

Flux.prototype.blockFirst = function blockFirst<T>(this: Flux<T>): Promise<T | undefined> {
    return this.next().block();
};

Flux.prototype.blockLast = function blockLast<T>(this: Flux<T>): Promise<T | undefined> {
    return this.toPromise();
};

Flux.prototype.collect = function collect<T, R>(
    this: Flux<T>,
    supplier: () => R,
    accumulator: (container: R, value: T) => void
): Mono<R> {
    return terminalMono(this, UNBOUNDED_DEMAND, () => {
        const container = supplier();
        return {
            /** Accumulates one value into the supplied container. */
            onNext(value) {
                accumulator(container, value);
                return false;
            },
            result: () => container
        };
    });
};

Flux.prototype.collectMap = function collectMap<T, K, V = T>(
    this: Flux<T>,
    keyExtractor: (value: T) => K,
    valueExtractor: (value: T) => V = identity as (value: T) => V
): Mono<Map<K, V>> {
    return this.collect(
        () => new Map<K, V>(),
        (map, value) => map.set(keyExtractor(value), valueExtractor(value))
    );
};

Flux.prototype.collectMultimap = function collectMultimap<T, K, V = T>(
    this: Flux<T>,
    keyExtractor: (value: T) => K,
    valueExtractor: (value: T) => V = identity as (value: T) => V
): Mono<Map<K, V[]>> {
    return this.collect(
        () => new Map<K, V[]>(),
        (map, value) => {
            const key = keyExtractor(value);
            const bucket = map.get(key);
            if (bucket) {
                bucket.push(valueExtractor(value));
            } else {
                map.set(key, [valueExtractor(value)]);
            }
        }
    );
};

Flux.prototype.collectSortedList = function collectSortedList<T>(
    this: Flux<T>,
    comparator?: (left: T, right: T) => number
): Mono<T[]> {
    return this.collectList().map(values => values.sort(comparator));
};

Flux.prototype.concatWithValues = function concatWithValues<T>(this: Flux<T>, ...values: readonly T[]): Flux<T> {
    return this.concatWith(Flux.just(...values));
};

Flux.prototype.elementAt = function elementAt<T>(this: Flux<T>, index: number, defaultValue?: T): Mono<T> {
    if (!Number.isInteger(index) || index < 0) {
        throw new RangeError("index must be a non-negative integer");
    }
    const hasDefault = arguments.length > 1;
    return terminalMono<T, T>(this, index + 1, () => {
        let remaining = index;
        let result: T | undefined;
        let found = false;
        return {
            /** Skips values until the requested index is reached. */
            onNext(value) {
                if (remaining > 0) {
                    remaining -= 1;
                    return false;
                }
                result = value;
                found = true;
                return true;
            },
            /** Resolves the indexed value, default, or missing-value error. */
            result() {
                if (found) {
                    return result as T;
                }
                if (hasDefault) {
                    return defaultValue as T;
                }
                throw new NoSuchElementError();
            }
        };
    });
};

Flux.prototype.hasElements = function hasElements<T>(this: Flux<T>): Mono<boolean> {
    return terminalMono(this, 1, () => {
        let present = false;
        return {
            /** Marks the source as non-empty and finishes. */
            onNext() {
                present = true;
                return true;
            },
            result: () => present
        };
    });
};

Flux.prototype.hide = function hide<T>(this: Flux<T>): Flux<T> {
    const source = this;
    return liftOneToOne(
        source,
        (signal, context) => source.iterate(signal, context),
        () => ({onNext: identity})
    );
};

Flux.prototype.ignoreElements = function ignoreElements<T>(this: Flux<T>): Mono<void> {
    return this.then();
};

Flux.prototype.last = function last<T>(this: Flux<T>, defaultValue?: T): Mono<T> {
    const hasDefault = arguments.length > 0;
    return terminalMono(this, UNBOUNDED_DEMAND, () => {
        let seen = false;
        let lastValue: T | undefined;
        return {
            /** Retains the latest source value. */
            onNext(value) {
                seen = true;
                lastValue = value;
                return false;
            },
            /** Resolves the last value, default, or empty-source error. */
            result() {
                if (seen) {
                    return lastValue as T;
                }
                if (hasDefault) {
                    return defaultValue as T;
                }
                throw new NoSuchElementError();
            }
        };
    });
};

Flux.prototype.mapNotNull = function mapNotNull<T, R>(
    this: Flux<T>,
    mapper: (value: T) => R | null | undefined
): Flux<R> {
    return this.handle((value, sink) => {
        const mapped = mapper(value);
        if (mapped !== null && mapped !== undefined) {
            sink.next(mapped);
        }
    });
};

Flux.prototype.ofType = function ofType<T, R>(this: Flux<T>, type: ClassLike<R>): Flux<R> {
    return this.filter(value => value instanceof type).cast<R>();
};

Flux.prototype.reduceWith = function reduceWith<T, R>(
    this: Flux<T>,
    supplier: () => R,
    accumulator: (accumulated: R, value: T) => R
): Mono<R> {
    return terminalMono(this, UNBOUNDED_DEMAND, () => {
        let current = supplier();
        return {
            /** Folds one value into the supplied initial state. */
            onNext(value) {
                current = accumulator(current, value);
                return false;
            },
            result: () => current
        };
    });
};

Flux.prototype.scanWith = function scanWith<T, R>(
    this: Flux<T>,
    supplier: () => R,
    accumulator: (accumulated: R, value: T) => R
): Flux<R> {
    const source = this;
    return new Flux(async function* (signal, context) {
        for await (const value of source.scan(supplier(), accumulator).iterate(signal, context)) {
            yield value;
        }
    });
};

Flux.prototype.skipLast = function skipLast<T>(this: Flux<T>, n: number): Flux<T> {
    if (!Number.isInteger(n) || n < 0) {
        throw new RangeError("skipLast expects a non-negative integer");
    }
    if (n === 0) {
        return this;
    }
    const source = this;
    return new Flux(async function* (signal, context) {
        const queue: T[] = [];
        let head = 0;
        for await (const value of source.iterate(signal, context)) {
            queue.push(value);
            if (queue.length - head > n) {
                const next = queue[head] as T;
                head += 1;
                if (head > 1024 && head * 2 > queue.length) {
                    queue.copyWithin(0, head);
                    queue.length -= head;
                    head = 0;
                }
                yield next;
            }
        }
    });
};

Flux.prototype.sort = function sort<T>(this: Flux<T>, comparator?: (left: T, right: T) => number): Flux<T> {
    return new Flux((signal, context) => this.collectSortedList(comparator).flatMapMany(Flux.fromIterable).iterate(signal, context));
};

Flux.prototype.subscribeWith = function subscribeWith<T, S extends Subscriber<T>>(this: Flux<T>, subscriber: S): S {
    this.subscribe(subscriber);
    return subscriber;
};

Flux.prototype.takeLast = function takeLast<T>(this: Flux<T>, n: number): Flux<T> {
    if (!Number.isInteger(n) || n < 0) {
        throw new RangeError("takeLast expects a non-negative integer");
    }
    if (n === 0) {
        return Flux.empty<T>();
    }
    const source = this;
    return new Flux(async function* (signal, context) {
        const values: T[] = [];
        let head = 0;
        await consumePublisher<T, void>(source, signal, context, UNBOUNDED_DEMAND, {
            /** Stores one value in the bounded trailing ring. */
            onNext(value) {
                if (values.length < n) {
                    values.push(value);
                } else {
                    values[head] = value;
                    head = (head + 1) % n;
                }
                return false;
            },
            result: () => undefined
        });
        for (let index = 0; index < values.length; index += 1) {
            if (signal.aborted) {
                return;
            }
            yield values[(head + index) % values.length] as T;
        }
    });
};

Flux.prototype.thenEmpty = function thenEmpty<T>(this: Flux<T>, other: PublisherInput<unknown>): Mono<void> {
    return this.thenMany(other).then();
};
