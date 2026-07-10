/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import {NoSuchElementError} from "@/errors/index.js";
import type {Subscriber} from "@/core/index.js";
import {Flux} from "@/publishers/flux.js";
import {Mono} from "@/publishers/mono.js";
import type {PublisherInput} from "@/publishers/types.js";

/** Constructor-like runtime value used by `ofType`. */
type ClassLike<R> = abstract new (...args: never[]) => R;

declare module "@/publishers/flux.js" {
    /** Reactor compatibility operators added to Flux. */
    interface Flux<T> {
        /** Reactor as operator. */
        as<R>(transformer: (source: Flux<T>) => R): R;

        /** Reactor blockFirst operator. */
        blockFirst(): Promise<T | undefined>;

        /** Reactor blockLast operator. */
        blockLast(): Promise<T | undefined>;

        /** Reactor collect operator. */
        collect<R>(supplier: () => R, accumulator: (container: R, value: T) => void): Mono<R>;

        /** Reactor collectMap operator. */
        collectMap<K, V = T>(keyExtractor: (value: T) => K, valueExtractor?: (value: T) => V): Mono<Map<K, V>>;

        /** Reactor collectMultimap operator. */
        collectMultimap<K, V = T>(keyExtractor: (value: T) => K, valueExtractor?: (value: T) => V): Mono<Map<K, V[]>>;

        /** Reactor collectSortedList operator. */
        collectSortedList(comparator?: (left: T, right: T) => number): Mono<T[]>;

        /** Reactor concatWithValues operator. */
        concatWithValues(...values: readonly T[]): Flux<T>;

        /** Reactor elementAt operator. */
        elementAt(index: number, defaultValue?: T): Mono<T>;

        /** Reactor hasElements operator. */
        hasElements(): Mono<boolean>;

        /** Reactor hide operator. */
        hide(): Flux<T>;

        /** Reactor ignoreElements operator. */
        ignoreElements(): Mono<void>;

        /** Reactor last operator. */
        last(defaultValue?: T): Mono<T>;

        /** Reactor mapNotNull operator. */
        mapNotNull<R>(mapper: (value: T) => R | null | undefined): Flux<R>;

        /** Reactor ofType operator. */
        ofType<R>(type: ClassLike<R>): Flux<R>;

        /** Reactor reduceWith operator. */
        reduceWith<R>(supplier: () => R, accumulator: (accumulated: R, value: T) => R): Mono<R>;

        /** Reactor scanWith operator. */
        scanWith<R>(supplier: () => R, accumulator: (accumulated: R, value: T) => R): Flux<R>;

        /** Reactor skipLast operator. */
        skipLast(n: number): Flux<T>;

        /** Reactor sort operator. */
        sort(comparator?: (left: T, right: T) => number): Flux<T>;

        /** Reactor subscribeWith operator. */
        subscribeWith<S extends Subscriber<T>>(subscriber: S): S;

        /** Reactor takeLast operator. */
        takeLast(n: number): Flux<T>;

        /** Reactor thenEmpty operator. */
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
    const source = this;
    return new Mono(async function* (signal, context) {
        const container = supplier();
        for await (const value of source.iterate(signal, context)) {
            accumulator(container, value);
        }
        yield container;
    });
};

Flux.prototype.collectMap = function collectMap<T, K, V = T>(
    this: Flux<T>,
    keyExtractor: (value: T) => K,
    valueExtractor: (value: T) => V = value => value as unknown as V
): Mono<Map<K, V>> {
    return this.collect(
        () => new Map<K, V>(),
        (map, value) => map.set(keyExtractor(value), valueExtractor(value))
    );
};

Flux.prototype.collectMultimap = function collectMultimap<T, K, V = T>(
    this: Flux<T>,
    keyExtractor: (value: T) => K,
    valueExtractor: (value: T) => V = value => value as unknown as V
): Mono<Map<K, V[]>> {
    return this.collect(
        () => new Map<K, V[]>(),
        (map, value) => {
            const key = keyExtractor(value);
            const bucket = map.get(key) ?? [];
            bucket.push(valueExtractor(value));
            map.set(key, bucket);
        }
    );
};

Flux.prototype.collectSortedList = function collectSortedList<T>(
    this: Flux<T>,
    comparator?: (left: T, right: T) => number
): Mono<T[]> {
    return this.collectList().map(values => [...values].sort(comparator));
};

Flux.prototype.concatWithValues = function concatWithValues<T>(this: Flux<T>, ...values: readonly T[]): Flux<T> {
    return this.concatWith(Flux.just(...values));
};

Flux.prototype.elementAt = function elementAt<T>(this: Flux<T>, index: number, defaultValue?: T): Mono<T> {
    if (!Number.isInteger(index) || index < 0) {
        throw new RangeError("index must be a non-negative integer");
    }
    const hasDefault = arguments.length > 1;
    return this.skip(index).next().switchIfEmpty(hasDefault ? Mono.just(defaultValue as T) : Mono.error(new NoSuchElementError()));
};

Flux.prototype.hasElements = function hasElements<T>(this: Flux<T>): Mono<boolean> {
    return this.any(() => true);
};

Flux.prototype.hide = function hide<T>(this: Flux<T>): Flux<T> {
    const source = this;
    return new Flux((signal, context) => source.iterate(signal, context));
};

Flux.prototype.ignoreElements = function ignoreElements<T>(this: Flux<T>): Mono<void> {
    return this.then();
};

Flux.prototype.last = function last<T>(this: Flux<T>, defaultValue?: T): Mono<T> {
    const source = this;
    const hasDefault = arguments.length > 0;
    return new Mono(async function* (signal, context) {
        let seen = false;
        let lastValue: T | undefined;
        for await (const value of source.iterate(signal, context)) {
            seen = true;
            lastValue = value;
        }
        if (seen) {
            yield lastValue as T;
        } else if (hasDefault) {
            yield defaultValue as T;
        } else {
            throw new NoSuchElementError();
        }
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
    return this.reduce(supplier(), accumulator);
};

Flux.prototype.scanWith = function scanWith<T, R>(
    this: Flux<T>,
    supplier: () => R,
    accumulator: (accumulated: R, value: T) => R
): Flux<R> {
    return this.scan(supplier(), accumulator);
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
        for await (const value of source.iterate(signal, context)) {
            if (values.length < n) {
                values.push(value);
            } else {
                values[head] = value;
                head = (head + 1) % n;
            }
        }
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
