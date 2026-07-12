/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import {BooleanDisposable} from "@/core/boolean-disposable.js";
import {addCap} from "@/core/demand.js";
import type {Disposable} from "@/core/types.js";
import {Context} from "@/context/context.js";
import type {ContextView} from "@/context/context-view.js";
import {CompositeError} from "@/errors/classes.js";
import {asError, throwIfNullish} from "@/errors/helpers.js";
import {firstValueFrom, raceWithAbort} from "@/internal/abort.js";
import {AsyncQueue} from "@/internal/async-queue.js";
import {
    closeAsyncIterator,
    EMPTY_ITERABLE,
    type AnyIterable,
    isAsyncIterable,
    isIterable,
    neverIterable,
    toAsyncIterator
} from "@/internal/iterable.js";
import {collectIterable, lastIterableValue} from "@/internal/iterable-terminal.js";
import {mapIterable} from "@/internal/iterable-transform.js";
import {eventSource, type WebSocketFluxOptions, webSocketSource} from "@/publisher/browser-sources.js";
import type {Publisher} from "@/publisher/publisher.js";
import {addCancelCallback, type CancelCallbacks, isSubscriber, runCancelCallbacks, scheduleDelay} from "@/publisher/helpers.js";
import {IterableSubscription} from "@/subscription/iterable-subscription.js";
import type {FluxSinkCallback, PublisherInput, SourceFactory} from "@/publisher/types.js";
import {Schedulers} from "@/schedulers/schedulers.js";
import type {DurationInput, Scheduler} from "@/schedulers/types.js";
import type {CoreSubscriber} from "@/subscription/core-subscriber.js";
import type {Subscriber} from "@/subscription/subscriber.js";
import type {Subscription} from "@/subscription/subscription.js";

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
        if (values.length === 0) {
            return Flux.empty<T>();
        }
        for (const value of values) {
            throwIfNullish(value, "value");
        }
        return Flux.fromIterable(values);
    }

    /** Creates a Flux from an array-like readonly value list. */
    public static fromArray<T>(values: readonly T[]): Flux<T> {
        throwIfNullish(values, "values");
        if (values.length === 0) {
            return Flux.empty<T>();
        }
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
        if (Array.isArray(values) && values.length === 0) {
            return Flux.empty<T>();
        }
        return new Flux(() => values);
    }

    /** Creates a Flux from browser `Window` events and removes the listener on cancellation. */
    public static fromEvent<K extends keyof WindowEventMap>(
        target: Window,
        type: K,
        options?: boolean | AddEventListenerOptions
    ): Flux<WindowEventMap[K]>;
    /** Creates a Flux from browser `Document` events and removes the listener on cancellation. */
    public static fromEvent<K extends keyof DocumentEventMap>(
        target: Document,
        type: K,
        options?: boolean | AddEventListenerOptions
    ): Flux<DocumentEventMap[K]>;
    /** Creates a Flux from browser `HTMLElement` events and removes the listener on cancellation. */
    public static fromEvent<K extends keyof HTMLElementEventMap>(
        target: HTMLElement,
        type: K,
        options?: boolean | AddEventListenerOptions
    ): Flux<HTMLElementEventMap[K]>;
    /** Creates a Flux from any browser `EventTarget` and removes the listener on cancellation. */
    public static fromEvent<TEvent extends Event = Event>(
        target: EventTarget,
        type: string,
        options?: boolean | AddEventListenerOptions
    ): Flux<TEvent>;
    /** Creates a Flux from browser events and removes the listener on cancellation. */
    public static fromEvent<TEvent extends Event = Event>(
        target: EventTarget,
        type: string,
        options?: boolean | AddEventListenerOptions
    ): Flux<TEvent> {
        return new Flux(eventSource<TEvent>(target, type, options));
    }

    /** Creates a Flux from browser WebSocket message events. */
    public static fromWebSocket<T = unknown>(
        url: string | URL,
        options?: string | string[] | WebSocketFluxOptions
    ): Flux<MessageEvent<T>>;
    /** Creates a Flux from browser WebSocket message events. */
    public static fromWebSocket<T = unknown>(
        socket: WebSocket,
        options?: WebSocketFluxOptions
    ): Flux<MessageEvent<T>>;
    /** Creates a Flux from browser WebSocket message events and cleans listeners on cancellation. */
    public static fromWebSocket<T = unknown>(
        input: WebSocket | string | URL,
        options?: string | string[] | WebSocketFluxOptions
    ): Flux<MessageEvent<T>> {
        return new Flux(webSocketSource<T>(input, options));
    }

    /** Adapts a publisher, iterable, async iterable or promise-like value to Flux. */
    public static from<T>(input: PublisherInput<T>): Flux<T> {
        throwIfNullish(input, "input");
        if (input instanceof Flux) {
            return input;
        }
        if (typeof (input as Publisher<T>).subscribe === "function") {
            return fromPublisher(input as Publisher<T>);
        }
        if (isAsyncIterable<T>(input)) {
            return new Flux(() => input);
        }
        if (isIterable<T>(input)) {
            return Flux.fromIterable(input);
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
        return EMPTY_FLUX as Flux<T>;
    }

    /** Creates a Flux that never emits or completes until cancelled. */
    public static never<T = never>(): Flux<T> {
        return NEVER_FLUX as Flux<T>;
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
            let cancelCallbacks: CancelCallbacks | undefined;
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
                    cancelCallbacks = addCancelCallback(cancelCallbacks, cancelCallback);
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
                    runCancelCallbacks(cancelCallbacks);
                    cancelCallbacks = undefined;
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
            while (!done && !signal.aborted) {
                hasValue = false;
                value = undefined;
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
        if (count === 0) {
            return Flux.empty<number>();
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
        if (sources.length === 0) {
            return Flux.empty<T>();
        }
        if (sources.length === 1) {
            return Flux.from(sources[0]!);
        }
        return mergeSources(toFluxArray(sources));
    }

    /** Merges several publishers concurrently and delays failures until every source terminates. */
    public static mergeDelayError<T>(...sources: readonly PublisherInput<T>[]): Flux<T> {
        if (sources.length === 0) {
            return Flux.empty<T>();
        }
        if (sources.length === 1) {
            return Flux.from(sources[0]!);
        }
        return mergeSourcesDelayError(toFluxArray(sources));
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
        if (sources.length === 0) {
            return Flux.empty<T>();
        }
        if (sources.length === 1) {
            return Flux.from(sources[0]!);
        }
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
        if (sources.length === 0) {
            return Flux.empty<T>();
        }
        if (sources.length === 1) {
            return Flux.from(sources[0]!);
        }
        return concatDelayErrorSources(sources);
    }

    /** Relays the first source that emits any signal. */
    public static first<T>(...sources: readonly PublisherInput<T>[]): Flux<T> {
        return Flux.firstWithSignal(...sources);
    }

    /** Relays the first source that emits any signal. */
    public static firstWithSignal<T>(...sources: readonly PublisherInput<T>[]): Flux<T> {
        if (sources.length === 0) {
            return Flux.empty<T>();
        }
        if (sources.length === 1) {
            return Flux.from(sources[0]!);
        }
        return new Flux((signal, context) => {
            const queue = new AsyncQueue<T>(signal);
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
        if (sources.length === 0) {
            return Flux.empty<T>();
        }
        return new Flux((signal, context) => {
            const queue = new AsyncQueue<T>(signal);
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
        const sourceCount = sources.length;
        if (sourceCount === 0) {
            return Flux.empty<R>();
        }
        if (sourceCount === 1) {
            return singleTupleSource(sources[0]!, combinator);
        }
        return new Flux(async function* (signal, context) {
            const iterators = new Array<AsyncIterator<unknown>>(sourceCount);
            const nexts = new Array<Promise<IteratorResult<unknown>>>(sourceCount);
            for (let index = 0; index < sourceCount; index += 1) {
                iterators[index] = toAsyncIterator(Flux.from(sources[index]!).iterate(signal, context));
            }
            try {
                while (!signal.aborted) {
                    for (let index = 0; index < sourceCount; index += 1) {
                        nexts[index] = raceWithAbort(iterators[index]!.next(), signal);
                    }
                    const results = await Promise.all(nexts);
                    const values = new Array<unknown>(sourceCount);
                    for (let index = 0; index < sourceCount; index += 1) {
                        const result = results[index]!;
                        if (result.done) {
                            return;
                        }
                        values[index] = result.value;
                    }
                    const tuple = values as unknown as T;
                    yield combinator ? combinator(tuple) : (tuple as unknown as R);
                }
            } finally {
                for (const iterator of iterators) {
                    await closeAsyncIterator(iterator);
                }
            }
        });
    }

    /** Combines the latest value from each source after all sources emitted once. */
    public static combineLatest<T extends readonly unknown[], R = T>(
        sources: { [K in keyof T]: PublisherInput<T[K]> },
        combinator?: (values: T) => R
    ): Flux<R> {
        if (sources.length === 0) {
            return Flux.empty<R>();
        }
        if (sources.length === 1) {
            return singleTupleSource(sources[0]!, combinator);
        }
        return new Flux((signal, context) => {
            const queue = new AsyncQueue<R>(signal);
            const sourceCount = sources.length;
            const latest = new Array<unknown>(sourceCount);
            const hasValue = new Array<boolean>(sourceCount);
            let readyCount = 0;
            let completed = 0;
            let failed = false;
            for (let index = 0; index < sourceCount; index += 1) {
                const source = sources[index]!;
                void (async () => {
                    try {
                        for await (const value of Flux.from(source).iterate(signal, context)) {
                            latest[index] = value;
                            if (hasValue[index] !== true) {
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
        resourceCleanup: (resource: D) => void = closeResource as (resource: D) => void
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
        onError: (error: unknown) => void = throwUnhandledSubscriberError,
        onComplete: () => void = noopSubscriberCallback
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
            onNext: subscriberOrNext ?? noopSubscriberCallback,
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
        return collectIterable(this.iterate(new AbortController().signal, Context.empty()));
    }

    /** Resolves to the last emitted value, or undefined for an empty source. */
    public async toPromise(): Promise<T | undefined> {
        return lastIterableValue(this.iterate(new AbortController().signal, Context.empty()));
    }
}

/** Shared stateless empty Flux instance. */
const EMPTY_FLUX = new Flux<never>(() => EMPTY_ITERABLE);

/** Shared stateless never Flux instance. */
const NEVER_FLUX = new Flux<never>(signal => neverIterable(signal));

/** Default cleanup for resources that expose a close method. */
function closeResource(resource: unknown): void {
    (resource as { close?: () => void }).close?.();
}

/** Shared no-op subscriber callback. */
function noopSubscriberCallback(): void {
    // no-op
}

/** Throws unhandled subscriber errors like the default callback contract. */
function throwUnhandledSubscriberError(error: unknown): never {
    throw asError(error);
}

/** Adapts a Reactive Streams publisher to the Flux async-iteration model. */
function fromPublisher<T>(publisher: Publisher<T>): Flux<T> {
    return new PublisherFlux(publisher);
}

/** Flux adapter that keeps Reactive Streams subscriptions on their native demand path. */
class PublisherFlux<T> extends Flux<T> {
    /** Creates a direct Flux adapter for a Reactive Streams publisher. */
    public constructor(private readonly publisher: Publisher<T>) {
        super(signal => publisherAsyncIterable(publisher, signal));
    }

    /** Subscribes directly so request amounts are not converted to iterator pulls. */
    protected override subscribeActual(subscriber: Subscriber<T>): void {
        this.publisher.subscribe(subscriber);
    }
}

/** Adapts a publisher to pull-based async iteration without requesting it unboundedly. */
function publisherAsyncIterable<T>(publisher: Publisher<T>, signal: AbortSignal): AsyncIterable<T> {
    return {
        /** Creates one pull-based iterator and its dedicated publisher subscription. */
        [Symbol.asyncIterator]() {
            const queue = new AsyncQueue<T>();
            let subscription: Subscription | undefined;
            let pendingDemand = 0;
            let terminated = signal.aborted;

            const cleanup = () => signal.removeEventListener("abort", cancel);
            const cancel = () => {
                if (terminated) {
                    return;
                }
                terminated = true;
                cleanup();
                subscription?.cancel();
                void queue.return();
            };

            if (terminated) {
                queue.complete();
            } else {
                signal.addEventListener("abort", cancel, {once: true});
                try {
                    publisher.subscribe({
                        /** Stores the upstream and drains any pulls made before onSubscribe. */
                        onSubscribe(nextSubscription) {
                            if (subscription || terminated) {
                                nextSubscription.cancel();
                                return;
                            }
                            subscription = nextSubscription;
                            if (pendingDemand > 0) {
                                const request = pendingDemand;
                                pendingDemand = 0;
                                nextSubscription.request(request);
                            }
                        },
                        /** Queues one publisher value for an async iterator pull. */
                        onNext(value) {
                            queue.push(value);
                        },
                        /** Propagates the terminal publisher error to the iterator. */
                        onError(error) {
                            if (terminated) {
                                return;
                            }
                            terminated = true;
                            cleanup();
                            queue.error(error);
                        },
                        /** Completes the iterator when the publisher completes. */
                        onComplete() {
                            if (terminated) {
                                return;
                            }
                            terminated = true;
                            cleanup();
                            queue.complete();
                        }
                    });
                } catch (error) {
                    terminated = true;
                    cleanup();
                    queue.error(error);
                }
            }

            return {
                /** Requests exactly one upstream item for one iterator pull. */
                next() {
                    if (!terminated) {
                        if (subscription) {
                            subscription.request(1);
                        } else {
                            pendingDemand = addCap(pendingDemand, 1);
                        }
                    }
                    return queue.next();
                },
                /** Cancels upstream when iteration stops early. */
                return() {
                    cancel();
                    return queue.return();
                },
                /** Cancels upstream and rejects the iterator with the supplied error. */
                throw(error?: unknown) {
                    cancel();
                    return Promise.reject(error);
                }
            };
        }
    };
}

/** Adapts publisher inputs to Flux instances without callback allocation. */
function toFluxArray<T>(sources: readonly PublisherInput<T>[]): Flux<T>[] {
    const fluxes = new Array<Flux<T>>(sources.length);
    for (let index = 0; index < sources.length; index += 1) {
        fluxes[index] = Flux.from(sources[index]!);
    }
    return fluxes;
}

/** Emits one-element tuple values without multi-source coordination overhead. */
function singleTupleSource<T extends readonly unknown[], R>(
    source: PublisherInput<T[0]>,
    combinator: ((values: T) => R) | undefined
): Flux<R> {
    return new Flux((signal, context) =>
        mapIterable(Flux.from(source).iterate(signal, context), value => {
            const tuple = [value] as unknown as T;
            return combinator ? combinator(tuple) : (tuple as unknown as R);
        })
    );
}

/** Merges sources concurrently without bounded-concurrency bookkeeping. */
function mergeSources<T>(sources: readonly Flux<T>[]): Flux<T> {
    return new Flux((signal, context) => {
        const queue = new AsyncQueue<T>(signal);
        const tasks = new Array<Promise<void>>(sources.length);
        let failed = false;
        void (async () => {
            try {
                for (let index = 0; index < sources.length; index += 1) {
                    tasks[index] = drainMergedSource(sources[index]!, signal, context, queue, () => {
                        failed = true;
                    });
                }
                await Promise.all(tasks);
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

/** Merges sources concurrently and collects failures until all active sources terminate. */
function mergeSourcesDelayError<T>(sources: readonly Flux<T>[]): Flux<T> {
    return new Flux((signal, context) => {
        const queue = new AsyncQueue<T>(signal);
        const tasks = new Array<Promise<void>>(sources.length);
        const errors: unknown[] = [];
        void (async () => {
            try {
                for (let index = 0; index < sources.length; index += 1) {
                    tasks[index] = drainMergedSourceDelayError(sources[index]!, signal, context, queue, errors);
                }
                await Promise.all(tasks);
                errors.length > 0 ? queue.error(new CompositeError(errors)) : queue.complete();
            } catch (error) {
                queue.error(error);
            }
        })();
        return queue;
    });
}

/** Drains one merged source and reports its first failure through the output queue. */
async function drainMergedSource<T>(
    source: Flux<T>,
    signal: AbortSignal,
    context: Context,
    queue: AsyncQueue<T>,
    markFailed: () => void
): Promise<void> {
    try {
        for await (const value of source.iterate(signal, context)) {
            queue.push(value);
        }
    } catch (error) {
        markFailed();
        queue.error(error);
    }
}

/** Drains one merged source while recording failures for delayed-error merging. */
async function drainMergedSourceDelayError<T>(
    source: Flux<T>,
    signal: AbortSignal,
    context: Context,
    queue: AsyncQueue<T>,
    errors: unknown[]
): Promise<void> {
    try {
        for await (const value of source.iterate(signal, context)) {
            queue.push(value);
        }
    } catch (error) {
        errors.push(error);
    }
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
