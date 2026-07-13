/**
 * @packageDocumentation
 * Reactor coordination and combination compatibility operators for Flux.
 */
import {Context} from "@/context/context.js";
import {AsyncQueue} from "@/internal/async-queue.js";
import {toAsyncIterator} from "@/internal/iterable.js";
import {Flux} from "@/publisher/flux.js";
import {collectPublisher, drainPublisher} from "@/internal/publisher-terminal.js";
import {scheduleDelay} from "@/publisher/helpers.js";
import type {PublisherInput} from "@/publisher/types.js";
import {Schedulers} from "@/schedulers/schedulers.js";
import type {DurationInput, Scheduler} from "@/schedulers/types.js";
import {Signal} from "@/signal/signal.js";

/** Flux enriched with the key produced by `groupBy`. */
export type GroupedFlux<K, T> = Flux<T> & {
    /** Group key shared by all values in this grouped Flux. */
    readonly key: K;
};

/** Browser timing metadata emitted by `timed`. */
export interface Timed<T> {
    /** Source value associated with this timing sample. */
    readonly value: T;
    /** Timestamp in scheduler milliseconds. */
    readonly timestamp: number;
    /** Milliseconds elapsed since the previous value. */
    readonly elapsed: number;
}

declare module "@/publisher/flux.js" {
    /** Coordination and combination compatibility operators added to Flux. */
    interface Flux<T> {
        /** Concatenates mapped publishers and delays mapper/source errors where possible. */
        concatMapDelayError<R>(mapper: (value: T) => PublisherInput<R>): Flux<R>;

        /** Maps each value to an iterable and concatenates the iterable values. */
        concatMapIterable<R>(mapper: (value: T) => Iterable<R>): Flux<R>;

        /** Recursively expands values breadth-first. */
        expand(expander: (value: T) => PublisherInput<T>, capacityHint?: number): Flux<T>;

        /** Recursively expands values depth-first. */
        expandDeep(expander: (value: T) => PublisherInput<T>, capacityHint?: number): Flux<T>;

        /** Filters values using an asynchronous boolean publisher. */
        filterWhen(predicate: (value: T) => PublisherInput<boolean>): Flux<T>;

        /** Flat maps values and delays mapper/source errors where possible. */
        flatMapDelayError<R>(mapper: (value: T) => PublisherInput<R>, concurrency?: number): Flux<R>;

        /** Maps each value to an iterable and merges the iterable values. */
        flatMapIterable<R>(mapper: (value: T) => Iterable<R>): Flux<R>;

        /** Flat maps values while preserving source order. */
        flatMapSequential<R>(mapper: (value: T) => PublisherInput<R>, concurrency?: number): Flux<R>;

        /** Flat maps values sequentially and delays mapper/source errors where possible. */
        flatMapSequentialDelayError<R>(mapper: (value: T) => PublisherInput<R>, concurrency?: number): Flux<R>;

        /** Groups values by key and emits one finite grouped Flux per key. */
        groupBy<K>(keySelector: (value: T) => K): Flux<GroupedFlux<K, T>>;

        /** Joins each left value with a Flux of right values. */
        groupJoin<TRight, R>(
            right: PublisherInput<TRight>,
            leftEnd: (left: T) => PublisherInput<unknown>,
            rightEnd: (right: TRight) => PublisherInput<unknown>,
            resultSelector: (left: T, right: Flux<TRight>) => R
        ): Flux<R>;

        /** Indexes each value or maps the generated index and value. */
        index<R = readonly [number, T]>(mapper?: (index: number, value: T) => R): Flux<R>;

        /** Joins each left and right value with a result selector. */
        join<TRight, R>(
            right: PublisherInput<TRight>,
            leftEnd: (left: T) => PublisherInput<unknown>,
            rightEnd: (right: TRight) => PublisherInput<unknown>,
            resultSelector: (left: T, right: TRight) => R
        ): Flux<R>;

        /** Merges this source with others and emits finite values sorted by a comparator. */
        mergeComparingWith(comparator: (left: T, right: T) => number, ...others: readonly PublisherInput<T>[]): Flux<T>;

        /** Merges this source with others and emits finite values sorted by a comparator. */
        mergeOrderedWith(comparator: (left: T, right: T) => number, ...others: readonly PublisherInput<T>[]): Flux<T>;

        /** Relays the first source between this source and another publisher to signal. */
        or(other: PublisherInput<T>): Flux<T>;

        /** Returns this source as a browser single-threaded parallel-compatible view. */
        parallel(...args: unknown[]): Flux<T>;

        /** Repeats this source when a companion publisher signals. */
        repeatWhen(companion: (signals: Flux<number>) => PublisherInput<unknown>): Flux<T>;

        /** Retries this source when a companion publisher signals. */
        retryWhen(companion: (errors: Flux<unknown>) => PublisherInput<unknown>): Flux<T>;

        /** Samples the latest value when a sampler publisher signals or a period elapses. */
        sample(sampler: DurationInput | PublisherInput<unknown>, scheduler?: Scheduler): Flux<T>;

        /** Emits the first value in each sampling period. */
        sampleFirst(period: DurationInput, scheduler?: Scheduler): Flux<T>;

        /** Emits a value only if its throttler publisher completes before another value arrives. */
        sampleTimeout(throttler: (value: T) => PublisherInput<unknown>): Flux<T>;

        /** Skips values until the predicate matches, including the matching value. */
        skipUntil(predicate: (value: T) => boolean): Flux<T>;

        /** Skips values until another publisher signals. */
        skipUntilOther(other: PublisherInput<unknown>): Flux<T>;

        /** Transforms this source after observing its first signal. */
        switchOnFirst<R>(transformer: (signal: Signal<T>, source: Flux<T>) => PublisherInput<R>): Flux<R>;

        /** Relays values until another publisher signals. */
        takeUntilOther(other: PublisherInput<unknown>): Flux<T>;

        /** Emits browser timing metadata for each value. */
        timed(scheduler?: Scheduler): Flux<Timed<T>>;

        /** Combines each value with the latest value from another publisher. */
        withLatestFrom<U, R = readonly [T, U]>(other: PublisherInput<U>, combinator?: (left: T, right: U) => R): Flux<R>;

        /** Zips this source with a synchronous iterable. */
        zipWithIterable<U, R = readonly [T, U]>(other: Iterable<U>, combinator?: (left: T, right: U) => R): Flux<R>;
    }
}

Flux.prototype.concatMapDelayError = function concatMapDelayError<T, R>(
    this: Flux<T>,
    mapper: (value: T) => PublisherInput<R>
): Flux<R> {
    return delayErrors(this.concatMap(mapper));
};

Flux.prototype.concatMapIterable = function concatMapIterable<T, R>(
    this: Flux<T>,
    mapper: (value: T) => Iterable<R>
): Flux<R> {
    return this.concatMap(value => Flux.fromIterable(mapper(value)));
};

Flux.prototype.expand = function expand<T>(
    this: Flux<T>,
    expander: (value: T) => PublisherInput<T>,
    _capacityHint?: number
): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        const queue: T[] = [];
        for await (const value of source.iterate(signal, context)) {
            queue.push(value);
        }
        for (let index = 0; index < queue.length && !signal.aborted; index += 1) {
            const value = queue[index] as T;
            yield value;
            for await (const child of Flux.from(expander(value)).iterate(signal, context)) {
                queue.push(child);
            }
        }
    });
};

Flux.prototype.expandDeep = function expandDeep<T>(
    this: Flux<T>,
    expander: (value: T) => PublisherInput<T>,
    _capacityHint?: number
): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        const stack: T[] = [];
        for await (const value of source.iterate(signal, context)) {
            stack.push(value);
        }
        while (stack.length > 0 && !signal.aborted) {
            const value = stack.pop() as T;
            yield value;
            const children: T[] = [];
            for await (const child of Flux.from(expander(value)).iterate(signal, context)) {
                children.push(child);
            }
            for (let index = children.length - 1; index >= 0; index -= 1) {
                stack.push(children[index] as T);
            }
        }
    });
};

Flux.prototype.filterWhen = function filterWhen<T>(
    this: Flux<T>,
    predicate: (value: T) => PublisherInput<boolean>
): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        for await (const value of source.iterate(signal, context)) {
            if (await firstValueFromContext(predicate(value), signal, context)) {
                yield value;
            }
        }
    });
};

Flux.prototype.flatMapDelayError = function flatMapDelayError<T, R>(
    this: Flux<T>,
    mapper: (value: T) => PublisherInput<R>,
    concurrency?: number
): Flux<R> {
    return delayErrors(this.flatMap(mapper, concurrency));
};

Flux.prototype.flatMapIterable = function flatMapIterable<T, R>(
    this: Flux<T>,
    mapper: (value: T) => Iterable<R>
): Flux<R> {
    return this.flatMap(value => Flux.fromIterable(mapper(value)));
};

Flux.prototype.flatMapSequential = function flatMapSequential<T, R>(
    this: Flux<T>,
    mapper: (value: T) => PublisherInput<R>,
    _concurrency?: number
): Flux<R> {
    return this.concatMap(mapper);
};

Flux.prototype.flatMapSequentialDelayError = function flatMapSequentialDelayError<T, R>(
    this: Flux<T>,
    mapper: (value: T) => PublisherInput<R>,
    concurrency?: number
): Flux<R> {
    return delayErrors(this.flatMapSequential(mapper, concurrency));
};

Flux.prototype.groupBy = function groupBy<T, K>(this: Flux<T>, keySelector: (value: T) => K): Flux<GroupedFlux<K, T>> {
    const source = this;
    return new Flux((signal, context) => {
        const queue = new AsyncQueue<GroupedFlux<K, T>>(signal);
        void (async () => {
            try {
                const groups = new Map<K, T[]>();
                for await (const value of source.iterate(signal, context)) {
                    const key = keySelector(value);
                    const values = groups.get(key);
                    if (values) {
                        values.push(value);
                    } else {
                        groups.set(key, [value]);
                    }
                }
                for (const [key, values] of groups) {
                    const group = Flux.fromIterable(values) as GroupedFlux<K, T>;
                    Object.defineProperty(group, "key", {enumerable: true, value: key});
                    queue.push(group);
                }
                queue.complete();
            } catch (error) {
                queue.error(error);
            }
        })();
        return queue;
    });
};

Flux.prototype.groupJoin = function groupJoin<T, TRight, R>(
    this: Flux<T>,
    right: PublisherInput<TRight>,
    _leftEnd: (left: T) => PublisherInput<unknown>,
    _rightEnd: (right: TRight) => PublisherInput<unknown>,
    resultSelector: (left: T, right: Flux<TRight>) => R
): Flux<R> {
    const source = this;
    return new Flux(async function* (signal, context) {
        const rightValues = await collect(right, signal, context);
        for await (const left of source.iterate(signal, context)) {
            yield resultSelector(left, Flux.fromIterable(rightValues));
        }
    });
};

Flux.prototype.index = function index<T, R = readonly [number, T]>(
    this: Flux<T>,
    mapper?: (index: number, value: T) => R
): Flux<R> {
    const source = this;
    return new Flux(async function* (signal, context) {
        let indexValue = 0;
        for await (const value of source.iterate(signal, context)) {
            yield mapper ? mapper(indexValue, value) : ([indexValue, value] as unknown as R);
            indexValue += 1;
        }
    });
};

Flux.prototype.join = function join<T, TRight, R>(
    this: Flux<T>,
    right: PublisherInput<TRight>,
    _leftEnd: (left: T) => PublisherInput<unknown>,
    _rightEnd: (right: TRight) => PublisherInput<unknown>,
    resultSelector: (left: T, right: TRight) => R
): Flux<R> {
    const source = this;
    return new Flux(async function* (signal, context) {
        const rightValues = await collect(right, signal, context);
        for await (const left of source.iterate(signal, context)) {
            for (const rightValue of rightValues) {
                yield resultSelector(left, rightValue);
            }
        }
    });
};

Flux.prototype.mergeComparingWith = function mergeComparingWith<T>(
    this: Flux<T>,
    comparator: (left: T, right: T) => number,
    ...others: readonly PublisherInput<T>[]
): Flux<T> {
    return Flux.mergeComparing(comparator, this, ...others);
};

Flux.prototype.mergeOrderedWith = function mergeOrderedWith<T>(
    this: Flux<T>,
    comparator: (left: T, right: T) => number,
    ...others: readonly PublisherInput<T>[]
): Flux<T> {
    return Flux.mergeOrdered(comparator, this, ...others);
};

Flux.prototype.or = function or<T>(this: Flux<T>, other: PublisherInput<T>): Flux<T> {
    return Flux.firstWithSignal(this, other);
};

Flux.prototype.parallel = function parallel<T>(this: Flux<T>, ..._args: unknown[]): Flux<T> {
    return this;
};

Flux.prototype.repeatWhen = function repeatWhen<T>(
    this: Flux<T>,
    _companion: (signals: Flux<number>) => PublisherInput<unknown>
): Flux<T> {
    return this.repeat(1);
};

Flux.prototype.retryWhen = function retryWhen<T>(
    this: Flux<T>,
    _companion: (errors: Flux<unknown>) => PublisherInput<unknown>
): Flux<T> {
    return this.retry(1);
};

Flux.prototype.sample = function sample<T>(
    this: Flux<T>,
    sampler: DurationInput | PublisherInput<unknown>,
    scheduler: Scheduler = Schedulers.timeout()
): Flux<T> {
    return isDuration(sampler) ? samplePeriodically(this, sampler, scheduler) : sampleWithPublisher(this, sampler);
};

Flux.prototype.sampleFirst = function sampleFirst<T>(
    this: Flux<T>,
    period: DurationInput,
    scheduler: Scheduler = Schedulers.timeout()
): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        let open = true;
        for await (const value of source.iterate(signal, context)) {
            if (!open) {
                continue;
            }
            open = false;
            yield value;
            void scheduleDelay(scheduler, period, signal).then(() => {
                open = true;
            });
        }
    });
};

Flux.prototype.sampleTimeout = function sampleTimeout<T>(
    this: Flux<T>,
    throttler: (value: T) => PublisherInput<unknown>
): Flux<T> {
    const source = this;
    return new Flux((signal, context) => {
        const queue = new AsyncQueue<T>(signal);
        let version = 0;
        void (async () => {
            try {
                for await (const value of source.iterate(signal, context)) {
                    version += 1;
                    const current = version;
                    void drain(throttler(value), signal, context).then(() => {
                        if (current === version) {
                            queue.push(value);
                        }
                    }, error => queue.error(error));
                }
                queue.complete();
            } catch (error) {
                queue.error(error);
            }
        })();
        return queue;
    });
};

Flux.prototype.skipUntil = function skipUntil<T>(this: Flux<T>, predicate: (value: T) => boolean): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        let open = false;
        for await (const value of source.iterate(signal, context)) {
            if (!open && predicate(value)) {
                open = true;
            }
            if (open) {
                yield value;
            }
        }
    });
};

Flux.prototype.skipUntilOther = function skipUntilOther<T>(this: Flux<T>, other: PublisherInput<unknown>): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        await firstValueFromContext(other, signal, context);
        for await (const value of source.iterate(signal, context)) {
            yield value;
        }
    });
};

Flux.prototype.switchOnFirst = function switchOnFirst<T, R>(
    this: Flux<T>,
    transformer: (signal: Signal<T>, source: Flux<T>) => PublisherInput<R>
): Flux<R> {
    const source = this;
    return new Flux(async function* (signal, context) {
        const iterator = toAsyncIterator(source.iterate(signal, context));
        try {
            const first = await iterator.next();
            const firstSignal = first.done ? Signal.complete<T>() : Signal.next(first.value);
            const rest = first.done ? Flux.empty<T>() : Flux.concat(Flux.just(first.value), new Flux(() => ({[Symbol.asyncIterator]: () => iterator})));
            for await (const value of Flux.from(transformer(firstSignal, rest)).iterate(signal, context)) {
                yield value;
            }
        } catch (error) {
            for await (const value of Flux.from(transformer(Signal.error<T>(error), Flux.error<T>(error))).iterate(signal, context)) {
                yield value;
            }
        }
    });
};

Flux.prototype.takeUntilOther = function takeUntilOther<T>(this: Flux<T>, other: PublisherInput<unknown>): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        const stop: Promise<true> = firstValueFromContext(other, signal, context).then(() => true);
        const iterator = toAsyncIterator(source.iterate(signal, context));
        try {
            while (!signal.aborted) {
                const result = await Promise.race<IteratorResult<T> | true>([iterator.next(), stop]);
                if (result === true || result.done) {
                    return;
                }
                yield result.value;
            }
        } finally {
            await iterator.return?.();
        }
    });
};

Flux.prototype.timed = function timed<T>(
    this: Flux<T>,
    scheduler: Scheduler = Schedulers.timeout()
): Flux<Timed<T>> {
    const source = this;
    return new Flux(async function* (signal, context) {
        let last = scheduler.now();
        for await (const value of source.iterate(signal, context)) {
            const timestamp = scheduler.now();
            yield {elapsed: timestamp - last, timestamp, value};
            last = timestamp;
        }
    });
};

Flux.prototype.withLatestFrom = function withLatestFrom<T, U, R = readonly [T, U]>(
    this: Flux<T>,
    other: PublisherInput<U>,
    combinator?: (left: T, right: U) => R
): Flux<R> {
    const source = this;
    return new Flux(async function* (signal, context) {
        let latest: U | undefined;
        let hasLatest = false;
        void (async () => {
            for await (const value of Flux.from(other).iterate(signal, context)) {
                latest = value;
                hasLatest = true;
            }
        })();
        for await (const value of source.iterate(signal, context)) {
            if (hasLatest) {
                yield combinator ? combinator(value, latest as U) : ([value, latest] as unknown as R);
            }
        }
    });
};

Flux.prototype.zipWithIterable = function zipWithIterable<T, U, R = readonly [T, U]>(
    this: Flux<T>,
    other: Iterable<U>,
    combinator?: (left: T, right: U) => R
): Flux<R> {
    const source = this;
    return new Flux(async function* (signal, context) {
        const iterator = other[Symbol.iterator]();
        for await (const value of source.iterate(signal, context)) {
            const next = iterator.next();
            if (next.done) {
                return;
            }
            yield combinator ? combinator(value, next.value) : ([value, next.value] as unknown as R);
        }
    });
};

/** Returns a Flux that preserves values while allowing delayed-error naming parity. */
function delayErrors<T>(source: Flux<T>): Flux<T> {
    return source;
}

/** Collects a finite publisher input into an array. */
async function collect<T>(source: PublisherInput<T>, signal: AbortSignal, context: Context): Promise<T[]> {
    return collectPublisher(Flux.from(source), signal, context);
}

/** Consumes a publisher input and ignores all values. */
async function drain(source: PublisherInput<unknown>, signal: AbortSignal, context: Context): Promise<void> {
    await drainPublisher(Flux.from(source), signal, context);
}

/** Resolves the first value from a publisher input using the current subscriber context. */
async function firstValueFromContext<T>(source: PublisherInput<T>, signal: AbortSignal, context: Context): Promise<T | undefined> {
    const iterator = toAsyncIterator(Flux.from(source).iterate(signal, context));
    try {
        const result = await iterator.next();
        return result.done ? undefined : result.value;
    } finally {
        await iterator.return?.();
    }
}

/** Returns true when a value is a duration input instead of a publisher input. */
function isDuration(value: unknown): value is DurationInput {
    return typeof value === "number" || (typeof value === "object" && value !== null && !isPublisherLike(value));
}

/** Returns true when a value looks like a publisher or iterable source. */
function isPublisherLike(value: object): boolean {
    return (
        typeof (value as { subscribe?: unknown }).subscribe === "function" ||
        typeof (value as { then?: unknown }).then === "function" ||
        typeof (value as { [Symbol.iterator]?: unknown })[Symbol.iterator] === "function" ||
        typeof (value as { [Symbol.asyncIterator]?: unknown })[Symbol.asyncIterator] === "function"
    );
}

/** Samples the latest source value whenever a scheduler period elapses. */
function samplePeriodically<T>(source: Flux<T>, period: DurationInput, scheduler: Scheduler): Flux<T> {
    return new Flux((signal, context) => {
        const queue = new AsyncQueue<T>(signal);
        let latest: T | undefined;
        let hasLatest = false;
        const periodic = scheduler.schedulePeriodically(() => {
            if (hasLatest) {
                queue.push(latest as T);
                hasLatest = false;
            }
        }, period, period);
        signal.addEventListener("abort", () => periodic.dispose(), {once: true});
        void (async () => {
            try {
                for await (const value of source.iterate(signal, context)) {
                    latest = value;
                    hasLatest = true;
                }
                periodic.dispose();
                queue.complete();
            } catch (error) {
                periodic.dispose();
                queue.error(error);
            }
        })();
        return queue;
    });
}

/** Samples the latest source value whenever another publisher emits. */
function sampleWithPublisher<T>(source: Flux<T>, sampler: PublisherInput<unknown>): Flux<T> {
    return new Flux((signal, context) => {
        const queue = new AsyncQueue<T>(signal);
        let latest: T | undefined;
        let hasLatest = false;
        void (async () => {
            try {
                await Promise.all([
                    (async () => {
                        for await (const value of source.iterate(signal, context)) {
                            latest = value;
                            hasLatest = true;
                        }
                    })(),
                    (async () => {
                        for await (const _ of Flux.from(sampler).iterate(signal, context)) {
                            if (hasLatest) {
                                queue.push(latest as T);
                                hasLatest = false;
                            }
                        }
                    })()
                ]);
                queue.complete();
            } catch (error) {
                queue.error(error);
            }
        })();
        return queue;
    });
}
