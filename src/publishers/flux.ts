/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import {BooleanDisposable, type Disposable} from "@/core/index.js";
import {Context, type ContextView} from "@/context/index.js";
import {asError, CompositeError, throwIfNullish} from "@/errors/index.js";
import {
    type AnyIterable,
    AsyncQueue,
    firstValueFrom,
    isAsyncIterable,
    isIterable,
    raceWithAbort,
    toAsyncIterator
} from "@/internal/index.js";
import type {Publisher} from "@/publishers/publisher.js";
import {type DurationInput, type Scheduler, Schedulers} from "@/scheduler/index.js";
import type {CoreSubscriber, Subscriber, Subscription} from "@/subscriptions/index.js";
import {isSubscriber, scheduleDelay} from "@/publishers/helpers.js";
import {IterableSubscription} from "@/subscriptions/iterable-subscription.js";
import type {FluxSinkCallback, PublisherInput, SourceFactory} from "@/publishers/types.js";

/** Multi-value Reactive Streams publisher with Reactor-style operators. */
export class Flux<T> implements Publisher<T>, AsyncIterable<T> {
    /** Lazy source factory invoked for each subscription or async iteration. */
    private readonly sourceFactory: SourceFactory<T>;

    /** Creates a Flux from a lazy source factory. */
    public constructor(sourceFactory: SourceFactory<T>) {
        this.sourceFactory = sourceFactory;
    }

    /** Creates a Flux that emits the provided values and then completes. */
    public static just<T>(...values: readonly T[]): Flux<T> {
        for (const value of values) {
            throwIfNullish(value, "value");
        }
        return Flux.fromIterable(values);
    }

    /** Creates a Flux from an array-like readonly value list. */
    public static fromArray<T>(values: readonly T[]): Flux<T> {
        return Flux.fromIterable(values);
    }

    /** Creates a Flux from a stream-like iterable or a lazy stream supplier. */
    public static fromStream<T>(stream: Iterable<T> | (() => Iterable<T>)): Flux<T> {
        throwIfNullish(stream, "stream");
        return typeof stream === "function" ? Flux.defer(() => Flux.fromIterable((stream as () => Iterable<T>)())) : Flux.fromIterable(stream);
    }

    /** Creates a Flux from a JavaScript iterable. */
    public static fromIterable<T>(values: Iterable<T>): Flux<T> {
        throwIfNullish(values, "values");
        return new Flux(() => values);
    }

    /** Adapts a publisher, iterable, async iterable or promise-like value to Flux. */
    public static from<T>(input: PublisherInput<T>): Flux<T> {
        throwIfNullish(input, "input");
        if (input instanceof Flux) {
            return input;
        }
        if (isAsyncIterable<T>(input) || isIterable<T>(input)) {
            return new Flux(() => input);
        }
        if (typeof (input as Publisher<T>).subscribe === "function") {
            return fromPublisher(input as Publisher<T>);
        }
        if (typeof (input as PromiseLike<T>).then === "function") {
            return new Flux(async function* (signal) {
                const value = await raceWithAbort(Promise.resolve(input as PromiseLike<T>), signal);
                if (value !== undefined && value !== null) {
                    yield value;
                }
            });
        }
        throw new TypeError("Unsupported publisher input");
    }

    /** Creates a Flux that completes without emitting values. */
    public static empty<T = never>(): Flux<T> {
        return new Flux(function* () {
            // empty
        });
    }

    /** Creates a Flux that never emits or completes until cancelled. */
    public static never<T = never>(): Flux<T> {
        return new Flux(async function* (signal) {
            await new Promise<void>((_resolve, reject) => {
                signal.addEventListener("abort", () => reject(signal.reason), {once: true});
            });
        });
    }

    /** Creates a Flux that terminates with the provided error. */
    public static error<T = never>(error: unknown | (() => unknown)): Flux<T> {
        return new Flux(async function* () {
            throw typeof error === "function" ? (error as () => unknown)() : error;
        });
    }

    /** Defers source creation until subscription time. */
    public static defer<T>(supplier: () => PublisherInput<T>): Flux<T> {
        return new Flux((signal, context) => Flux.from(supplier()).iterate(signal, context));
    }

    /** Defers source creation and exposes the subscriber context. */
    public static deferContextual<T>(supplier: (context: ContextView) => PublisherInput<T>): Flux<T> {
        return new Flux((signal, context) => Flux.from(supplier(context.readOnly())).iterate(signal, context));
    }

    /** Creates a Flux by bridging callback-style producers through a sink. */
    public static create<T>(callback: FluxSinkCallback<T>): Flux<T> {
        throwIfNullish(callback, "callback");
        return new Flux((signal, context) => {
            const queue = new AsyncQueue<T>(signal);
            let cancelled = false;
            const cancelCallbacks = new Set<() => void>();
            const sink = {
                /** Emits one value into the created Flux. */
                next(value: T) {
                    throwIfNullish(value, "value");
                    queue.push(value);
                },
                /** Terminates the created Flux with an error. */
                error(error: unknown) {
                    queue.error(error);
                },
                /** Completes the created Flux. */
                complete() {
                    queue.complete();
                },
                /** Registers a callback invoked when the subscription is cancelled. */
                onCancel(cancelCallback: () => void) {
                    cancelCallbacks.add(cancelCallback);
                    return sink;
                },
                /** Returns true after cancellation has been observed. */
                isCancelled() {
                    return cancelled || signal.aborted;
                }
            };
            signal.addEventListener(
                "abort",
                () => {
                    cancelled = true;
                    for (const cancelCallback of cancelCallbacks) {
                        cancelCallback();
                    }
                    queue.complete();
                },
                {once: true}
            );
            try {
                callback(sink);
            } catch (error) {
                queue.error(error);
            }
            return queue;
        });
    }

    /** Creates a Flux by bridging a single-threaded push-style producer through a sink. */
    public static push<T>(callback: FluxSinkCallback<T>): Flux<T> {
        return Flux.create(callback);
    }

    /** Creates a Flux by repeatedly invoking a synchronous generator callback. */
    public static generate<T>(generator: (sink: {
        next(value: T): void;
        complete(): void;
        error(error: unknown): void
    }) => void): Flux<T> {
        throwIfNullish(generator, "generator");
        return new Flux(async function* (signal) {
            let done = false;
            while (!done && !signal.aborted) {
                let hasValue = false;
                let value: T | undefined;
                const sink = {
                    /** Emits one generated value for this turn. */
                    next(nextValue: T) {
                        if (hasValue) {
                            throw new Error("Synchronous generator can emit at most one value per turn");
                        }
                        hasValue = true;
                        value = nextValue;
                    },
                    /** Completes the generated sequence. */
                    complete() {
                        done = true;
                    },
                    /** Fails the generated sequence. */
                    error(error: unknown) {
                        throw error;
                    }
                };
                generator(sink);
                if (hasValue) {
                    yield value as T;
                } else if (!done) {
                    throw new Error("Synchronous generator must emit, complete or fail");
                }
            }
        });
    }

    /** Creates a finite integer range. */
    public static range(start: number, count: number): Flux<number> {
        if (!Number.isInteger(start) || !Number.isInteger(count) || count < 0) {
            throw new RangeError("Flux.range expects an integer start and a non-negative integer count");
        }
        return new Flux(function* () {
            for (let i = 0; i < count; i += 1) {
                yield start + i;
            }
        });
    }

    /** Creates an interval Flux that emits incrementing numbers. */
    public static interval(period: DurationInput, scheduler: Scheduler = Schedulers.timeout()): Flux<number> {
        return new Flux(async function* (signal) {
            let index = 0;
            while (!signal.aborted) {
                await scheduleDelay(scheduler, period, signal);
                if (!signal.aborted) {
                    yield index;
                    index += 1;
                }
            }
        });
    }

    /** Merges several publishers concurrently. */
    public static merge<T>(...sources: readonly PublisherInput<T>[]): Flux<T> {
        return mergeSources(sources.map(source => Flux.from(source)), Number.POSITIVE_INFINITY);
    }

    /** Merges several publishers concurrently and delays failures until every source terminates. */
    public static mergeDelayError<T>(...sources: readonly PublisherInput<T>[]): Flux<T> {
        return mergeSourcesDelayError(sources.map(source => Flux.from(source)), Number.POSITIVE_INFINITY);
    }

    /** Merges sources sequentially while subscribing to one source at a time. */
    public static mergeSequential<T>(...sources: readonly PublisherInput<T>[]): Flux<T> {
        return Flux.concat(...sources);
    }

    /** Merges sources sequentially and delays failures until every source has been consumed. */
    public static mergeSequentialDelayError<T>(...sources: readonly PublisherInput<T>[]): Flux<T> {
        return concatDelayErrorSources(sources);
    }

    /** Merges finite sources and emits their values sorted by a comparator. */
    public static mergeComparing<T>(
        comparatorOrSource?: ((left: T, right: T) => number) | PublisherInput<T>,
        ...sources: readonly PublisherInput<T>[]
    ): Flux<T> {
        const {comparator, inputs} = orderedMergeInputs(comparatorOrSource, sources);
        return sortMergedSources(inputs, comparator);
    }

    /** Merges finite sources, delays failures and emits their values sorted by a comparator. */
    public static mergeComparingDelayError<T>(
        comparatorOrSource?: ((left: T, right: T) => number) | PublisherInput<T>,
        ...sources: readonly PublisherInput<T>[]
    ): Flux<T> {
        const {comparator, inputs} = orderedMergeInputs(comparatorOrSource, sources);
        return sortMergedSourcesDelayError(inputs, comparator);
    }

    /** Merges finite sources and emits their values in comparator order. */
    public static mergeOrdered<T>(
        comparatorOrSource?: ((left: T, right: T) => number) | PublisherInput<T>,
        ...sources: readonly PublisherInput<T>[]
    ): Flux<T> {
        return Flux.mergeComparing(comparatorOrSource, ...sources);
    }

    /** Merges finite sources with priority ordering. */
    public static mergePriority<T>(
        comparatorOrSource?: ((left: T, right: T) => number) | PublisherInput<T>,
        ...sources: readonly PublisherInput<T>[]
    ): Flux<T> {
        return Flux.mergeComparing(comparatorOrSource, ...sources);
    }

    /** Merges finite sources with priority ordering and delayed failures. */
    public static mergePriorityDelayError<T>(
        comparatorOrSource?: ((left: T, right: T) => number) | PublisherInput<T>,
        ...sources: readonly PublisherInput<T>[]
    ): Flux<T> {
        return Flux.mergeComparingDelayError(comparatorOrSource, ...sources);
    }

    /** Concatenates several publishers sequentially. */
    public static concat<T>(...sources: readonly PublisherInput<T>[]): Flux<T> {
        return new Flux(async function* (signal, context) {
            for (const source of sources) {
                for await (const value of Flux.from(source).iterate(signal, context)) {
                    if (signal.aborted) {
                        return;
                    }
                    yield value;
                }
            }
        });
    }

    /** Concatenates several publishers sequentially and delays failures until every source has been consumed. */
    public static concatDelayError<T>(...sources: readonly PublisherInput<T>[]): Flux<T> {
        return concatDelayErrorSources(sources);
    }

    /** Relays the first source that emits any signal. */
    public static first<T>(...sources: readonly PublisherInput<T>[]): Flux<T> {
        return Flux.firstWithSignal(...sources);
    }

    /** Relays the first source that emits any signal. */
    public static firstWithSignal<T>(...sources: readonly PublisherInput<T>[]): Flux<T> {
        return new Flux((signal, context) => {
            const queue = new AsyncQueue<T>(signal);
            if (sources.length === 0) {
                queue.complete();
                return queue;
            }
            let settled = false;
            for (const source of sources) {
                void (async () => {
                    try {
                        const iterator = toAsyncIterator(Flux.from(source).iterate(signal, context));
                        try {
                            const first = await raceWithAbort(iterator.next(), signal);
                            if (settled) {
                                await iterator.return?.();
                                return;
                            }
                            settled = true;
                            if (first.done) {
                                queue.complete();
                                return;
                            }
                            queue.push(first.value);
                            for (; ;) {
                                const next = await raceWithAbort(iterator.next(), signal);
                                if (next.done) {
                                    queue.complete();
                                    return;
                                }
                                queue.push(next.value);
                            }
                        } finally {
                            await iterator.return?.();
                        }
                    } catch (error) {
                        if (!settled) {
                            settled = true;
                            queue.error(error);
                        }
                    }
                })();
            }
            return queue;
        });
    }

    /** Relays the first source that produces a value, ignoring empty completions. */
    public static firstWithValue<T>(...sources: readonly PublisherInput<T>[]): Flux<T> {
        return new Flux((signal, context) => {
            const queue = new AsyncQueue<T>(signal);
            if (sources.length === 0) {
                queue.complete();
                return queue;
            }
            let remaining = sources.length;
            let settled = false;
            const errors: unknown[] = [];
            for (const source of sources) {
                void (async () => {
                    try {
                        const value = await firstValueFrom(Flux.from(source), signal);
                        if (settled) {
                            return;
                        }
                        if (value !== undefined) {
                            settled = true;
                            queue.push(value);
                            queue.complete();
                            return;
                        }
                    } catch (error) {
                        errors.push(error);
                    }
                    remaining -= 1;
                    if (remaining === 0 && !settled) {
                        errors.length > 0 ? queue.error(new CompositeError(errors)) : queue.complete();
                    }
                })();
            }
            return queue;
        });
    }

    /** Zips publishers by combining one value from each source. */
    public static zip<T extends readonly unknown[], R = T>(
        sources: { [K in keyof T]: PublisherInput<T[K]> },
        combinator?: (values: T) => R
    ): Flux<R> {
        return new Flux(async function* (signal, context) {
            const iterators = sources.map(source => toAsyncIterator(Flux.from(source).iterate(signal, context)));
            try {
                while (!signal.aborted) {
                    const results = await Promise.all(iterators.map(iterator => raceWithAbort(iterator.next(), signal)));
                    if (results.some(result => result.done)) {
                        return;
                    }
                    const values = results.map(result => result.value) as unknown as T;
                    yield combinator ? combinator(values) : (values as unknown as R);
                }
            } finally {
                await Promise.allSettled(iterators.map(iterator => iterator.return?.()));
            }
        });
    }

    /** Combines the latest value from each source after all sources emitted once. */
    public static combineLatest<T extends readonly unknown[], R = T>(
        sources: { [K in keyof T]: PublisherInput<T[K]> },
        combinator?: (values: T) => R
    ): Flux<R> {
        return new Flux((signal, context) => {
            const queue = new AsyncQueue<R>(signal);
            const sourceCount = sources.length;
            if (sourceCount === 0) {
                queue.complete();
                return queue;
            }
            const latest = new Array<unknown>(sources.length);
            const hasValue = new Array<boolean>(sources.length).fill(false);
            let readyCount = 0;
            let completed = 0;
            let failed = false;
            for (let index = 0; index < sourceCount; index += 1) {
                const source = sources[index]!;
                void (async () => {
                    try {
                        for await (const value of Flux.from(source).iterate(signal, context)) {
                            latest[index] = value;
                            if (!hasValue[index]) {
                                hasValue[index] = true;
                                readyCount += 1;
                            }
                            if (readyCount === sourceCount) {
                                const values = latest.slice() as unknown as T;
                                queue.push(combinator ? combinator(values) : (values as unknown as R));
                            }
                        }
                        completed += 1;
                        if (completed === sourceCount && !failed) {
                            queue.complete();
                        }
                    } catch (error) {
                        failed = true;
                        queue.error(error);
                    }
                })();
            }
            return queue;
        });
    }

    /** Switches to the most recent publisher emitted by an outer publisher. */
    public static switchOnNext<T>(sources: PublisherInput<PublisherInput<T>>): Flux<T> {
        return new Flux((signal, context) => {
            const queue = new AsyncQueue<T>(signal);
            let active = 0;
            let outerDone = false;
            let failed = false;
            const completeIfDone = () => {
                if (outerDone && active === 0 && !failed) {
                    queue.complete();
                }
            };
            void (async () => {
                const iterator = toAsyncIterator(Flux.from(sources).iterate(signal, context));
                try {
                    let version = 0;
                    while (!signal.aborted) {
                        const next = await raceWithAbort(iterator.next(), signal);
                        if (next.done) {
                            break;
                        }
                        version += 1;
                        const current = version;
                        active += 1;
                        void (async () => {
                            try {
                                for await (const value of Flux.from(next.value).iterate(signal, context)) {
                                    if (current === version) {
                                        queue.push(value);
                                    }
                                }
                            } catch (error) {
                                failed = true;
                                queue.error(error);
                            } finally {
                                active -= 1;
                                completeIfDone();
                            }
                        })();
                    }
                    outerDone = true;
                    completeIfDone();
                } catch (error) {
                    failed = true;
                    queue.error(error);
                } finally {
                    void iterator.return?.();
                }
            })();
            return queue;
        });
    }

    /** Uses a resource for the duration of a derived publisher and cleans it up afterwards. */
    public static using<T, D>(
        resourceSupplier: () => D,
        sourceFactory: (resource: D) => PublisherInput<T>,
        resourceCleanup: (resource: D) => void = resource => {
            (resource as { close?: () => void }).close?.();
        }
    ): Flux<T> {
        return new Flux(async function* (signal, context) {
            const resource = resourceSupplier();
            try {
                for await (const value of Flux.from(sourceFactory(resource)).iterate(signal, context)) {
                    yield value;
                }
            } finally {
                resourceCleanup(resource);
            }
        });
    }

    /** Uses an asynchronously supplied resource and runs asynchronous cleanup publishers. */
    public static usingWhen<T, D>(
        resourceSupplier: PublisherInput<D>,
        sourceFactory: (resource: D) => PublisherInput<T>,
        asyncComplete?: (resource: D) => PublisherInput<unknown>,
        asyncError?: (resource: D, error: unknown) => PublisherInput<unknown>,
        asyncCancel?: (resource: D) => PublisherInput<unknown>
    ): Flux<T> {
        return new Flux(async function* (signal, context) {
            const resource = await firstValueFrom(Flux.from(resourceSupplier), signal);
            if (resource === undefined) {
                return;
            }
            let failed: unknown;
            try {
                for await (const value of Flux.from(sourceFactory(resource)).iterate(signal, context)) {
                    yield value;
                }
            } catch (error) {
                failed = error;
                if (asyncError) {
                    await drainPublisher(asyncError(resource, error), new AbortController().signal, context);
                }
                throw error;
            } finally {
                if (signal.aborted && asyncCancel) {
                    await drainPublisher(asyncCancel(resource), new AbortController().signal, context);
                } else if (failed === undefined && asyncComplete) {
                    await drainPublisher(asyncComplete(resource), new AbortController().signal, context);
                }
            }
        });
    }

    /** Subscribes a full Reactive Streams subscriber. */
    public subscribe(subscriber: Subscriber<T>): void;
    /** Subscribes callbacks and requests unbounded demand. */
    public subscribe(onNext?: (value: T) => void, onError?: (error: unknown) => void, onComplete?: () => void): Disposable;
    /** Subscribes either a full subscriber or callback handlers. */
    public subscribe(
        subscriberOrNext?: Subscriber<T> | ((value: T) => void),
        onError: (error: unknown) => void = error => {
            throw asError(error);
        },
        onComplete: () => void = () => {
            // no-op
        }
    ): void | Disposable {
        if (isSubscriber<T>(subscriberOrNext)) {
            this.subscribeActual(subscriberOrNext);
            return;
        }

        let subscription: Subscription | undefined;
        const disposable = new BooleanDisposable(() => subscription?.cancel());
        const subscriber: Subscriber<T> = {
            /** Stores the subscription and requests unbounded demand. */
            onSubscribe(nextSubscription) {
                subscription = nextSubscription;
                nextSubscription.request(Number.POSITIVE_INFINITY);
            },
            onNext: subscriberOrNext ?? (() => undefined),
            onError,
            onComplete
        };
        this.subscribeActual(subscriber);
        return disposable;
    }

    /** Subscribes a normalized Reactive Streams subscriber to this Flux. */
    protected subscribeActual(subscriber: Subscriber<T>): void {
        const context = (subscriber as CoreSubscriber<T>).currentContext?.() ?? Context.empty();
        const subscription = new IterableSubscription(this, subscriber, context);
        subscription.start();
        subscriber.onSubscribe(subscription);
        subscription.deliverStartFailure();
    }

    /** Returns the lazy iterable backing this Flux for internal operator composition. */
    public iterate(signal: AbortSignal, context: Context = Context.empty()): AnyIterable<T> {
        return this.sourceFactory(signal, context);
    }

    /** Returns an async iterator over this Flux. */
    public [Symbol.asyncIterator](): AsyncIterator<T> {
        const controller = new AbortController();
        const iterator = toAsyncIterator(this.iterate(controller.signal, Context.empty()));
        return {
            next: () => iterator.next(),
            return: async value => {
                controller.abort();
                if (iterator.return) {
                    await iterator.return(value);
                }
                return {done: true, value: value as T};
            },
            throw: async error => {
                controller.abort();
                if (iterator.throw) {
                    return iterator.throw(error);
                }
                throw error;
            }
        };
    }

    /** Returns this Flux as an async iterable. */
    public toIterable(): AsyncIterable<T> {
        return this;
    }

    /** Collects all values into an array. */
    public async toArray(): Promise<T[]> {
        const values: T[] = [];
        for await (const value of this) {
            values.push(value);
        }
        return values;
    }

    /** Resolves to the last emitted value, or undefined for an empty source. */
    public async toPromise(): Promise<T | undefined> {
        let last: T | undefined;
        for await (const value of this) {
            last = value;
        }
        return last;
    }
}

/** Adapts a Reactive Streams publisher to the Flux async-iteration model. */
function fromPublisher<T>(publisher: Publisher<T>): Flux<T> {
    return new Flux((signal, context) => {
        const queue = new AsyncQueue<T>(signal);
        let subscription: Subscription | undefined;
        publisher.subscribe({
            /** Captures upstream subscription and requests unbounded demand. */
            onSubscribe(nextSubscription) {
                subscription = nextSubscription;
                nextSubscription.request(Number.POSITIVE_INFINITY);
            },
            /** Pushes upstream values into the bridge queue. */
            onNext(value) {
                queue.push(value);
            },
            /** Fails the bridge queue with the upstream error. */
            onError(error) {
                queue.error(error);
            },
            /** Completes the bridge queue when upstream completes. */
            onComplete() {
                queue.complete();
            }
        });
        signal.addEventListener("abort", () => subscription?.cancel(), {once: true});
        return queue;
    });
}

/** Merges sources while limiting the number of concurrently running sources. */
function mergeSources<T>(sources: readonly Flux<T>[], concurrency: number): Flux<T> {
    return new Flux((signal, context) => {
        const queue = new AsyncQueue<T>(signal);
        const running = new Set<Promise<void>>();
        let failed = false;
        void (async () => {
            try {
                for (const source of sources) {
                    while (running.size >= concurrency && !signal.aborted) {
                        await Promise.race(running);
                    }
                    let task: Promise<void> = Promise.resolve();
                    task = (async () => {
                        try {
                            for await (const value of source.iterate(signal, context)) {
                                queue.push(value);
                            }
                        } catch (error) {
                            failed = true;
                            queue.error(error);
                        } finally {
                            running.delete(task);
                        }
                    })();
                    running.add(task);
                }
                await Promise.all(running);
                if (!failed) {
                    queue.complete();
                }
            } catch (error) {
                queue.error(error);
            }
        })();
        return queue;
    });
}

/** Merges sources while collecting failures until all active sources terminate. */
function mergeSourcesDelayError<T>(sources: readonly Flux<T>[], concurrency: number): Flux<T> {
    return new Flux((signal, context) => {
        const queue = new AsyncQueue<T>(signal);
        const running = new Set<Promise<void>>();
        const errors: unknown[] = [];
        void (async () => {
            try {
                for (const source of sources) {
                    while (running.size >= concurrency && !signal.aborted) {
                        await Promise.race(running);
                    }
                    let task: Promise<void> = Promise.resolve();
                    task = (async () => {
                        try {
                            for await (const value of source.iterate(signal, context)) {
                                queue.push(value);
                            }
                        } catch (error) {
                            errors.push(error);
                        } finally {
                            running.delete(task);
                        }
                    })();
                    running.add(task);
                }
                await Promise.all(running);
                errors.length > 0 ? queue.error(new CompositeError(errors)) : queue.complete();
            } catch (error) {
                queue.error(error);
            }
        })();
        return queue;
    });
}

/** Concatenates sources while collecting failures until all sources have run. */
function concatDelayErrorSources<T>(sources: readonly PublisherInput<T>[]): Flux<T> {
    return new Flux(async function* (signal, context) {
        const errors: unknown[] = [];
        for (const source of sources) {
            try {
                for await (const value of Flux.from(source).iterate(signal, context)) {
                    yield value;
                }
            } catch (error) {
                errors.push(error);
            }
        }
        if (errors.length > 0) {
            throw new CompositeError(errors);
        }
    });
}

/** Normalizes ordered merge arguments into a comparator and source list. */
function orderedMergeInputs<T>(
    comparatorOrSource: ((left: T, right: T) => number) | PublisherInput<T> | undefined,
    sources: readonly PublisherInput<T>[]
): { comparator: ((left: T, right: T) => number) | undefined; inputs: readonly PublisherInput<T>[] } {
    if (typeof comparatorOrSource === "function") {
        return {comparator: comparatorOrSource as (left: T, right: T) => number, inputs: sources};
    }
    return {
        comparator: undefined,
        inputs: comparatorOrSource === undefined ? sources : [comparatorOrSource, ...sources]
    };
}

/** Collects finite merged values and emits them sorted by the optional comparator. */
function sortMergedSources<T>(sources: readonly PublisherInput<T>[], comparator?: (left: T, right: T) => number): Flux<T> {
    return new Flux(async function* (signal, context) {
        const values: T[] = [];
        for await (const value of Flux.merge(...sources).iterate(signal, context)) {
            values.push(value);
        }
        values.sort(comparator);
        yield* values;
    });
}

/** Collects finite merged values with delayed errors and emits them sorted by the optional comparator. */
function sortMergedSourcesDelayError<T>(
    sources: readonly PublisherInput<T>[],
    comparator?: (left: T, right: T) => number
): Flux<T> {
    return new Flux(async function* (signal, context) {
        const values: T[] = [];
        for await (const value of Flux.mergeDelayError(...sources).iterate(signal, context)) {
            values.push(value);
        }
        values.sort(comparator);
        yield* values;
    });
}

/** Consumes a cleanup publisher and ignores all emitted values. */
async function drainPublisher(source: PublisherInput<unknown>, signal: AbortSignal, context: Context): Promise<void> {
    for await (const _ of Flux.from(source).iterate(signal, context)) {
        // cleanup values are ignored
    }
}
