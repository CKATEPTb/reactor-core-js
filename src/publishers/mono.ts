/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import {type ContextView} from "@/context/index.js";
import {CompositeError, throwIfNullish} from "@/errors/index.js";
import {AsyncQueue, firstValueFrom, raceWithAbort, toAsyncIterator} from "@/internal/index.js";
import {type DurationInput, type Scheduler, Schedulers} from "@/scheduler/index.js";
import {Flux} from "@/publishers/flux.js";
import {scheduleDelay} from "@/publishers/helpers.js";
import type {MonoSinkCallback, PublisherInput, SourceFactory} from "@/publishers/types.js";

/** Zero-or-one Reactive Streams publisher with Reactor-style Mono operators. */
export class Mono<T> extends Flux<T> {
    /** Creates a Mono from a lazy source factory. */
    public constructor(sourceFactory: SourceFactory<T>) {
        super(async function* (signal, context) {
            const iterator = toAsyncIterator(sourceFactory(signal, context));
            try {
                const first = await raceWithAbort(iterator.next(), signal);
                if (!first.done) {
                    yield first.value;
                }
            } finally {
                await iterator.return?.();
            }
        });
    }

    /** Creates a Mono that emits exactly one non-nullish value. */
    public static override just<T>(value: T): Mono<T> {
        throwIfNullish(value, "value");
        return new Mono(function* () {
            yield value;
        });
    }

    /** Creates a Mono that completes without a value. */
    public static override empty<T = never>(): Mono<T> {
        return new Mono(function* () {
            // empty
        });
    }

    /** Creates a Mono that never emits or completes until cancelled. */
    public static override never<T = never>(): Mono<T> {
        return new Mono(async function* (signal) {
            await new Promise<void>((_resolve, reject) => {
                signal.addEventListener("abort", () => reject(signal.reason), {once: true});
            });
        });
    }

    /** Creates a Mono that terminates with the provided error. */
    public static override error<T = never>(error: unknown | (() => unknown)): Mono<T> {
        return new Mono(async function* () {
            throw typeof error === "function" ? (error as () => unknown)() : error;
        });
    }

    /** Defers Mono source creation until subscription time. */
    public static override defer<T>(supplier: () => PublisherInput<T>): Mono<T> {
        return new Mono((signal, context) => Flux.from(supplier()).next().iterate(signal, context));
    }

    /** Defers Mono source creation and exposes the subscriber context. */
    public static override deferContextual<T>(supplier: (context: ContextView) => PublisherInput<T>): Mono<T> {
        return new Mono((signal, context) => Flux.from(supplier(context.readOnly())).next().iterate(signal, context));
    }

    /** Relays the first Mono that emits any signal. */
    public static override first<T>(...sources: readonly PublisherInput<T>[]): Mono<T> {
        return Mono.firstWithSignal(...sources);
    }

    /** Adapts a source and keeps at most its first value. */
    public static override from<T>(input: PublisherInput<T>): Mono<T> {
        if (input instanceof Mono) {
            return input;
        }
        return Flux.from(input).next();
    }

    /** Creates a Mono from a promise-like value. */
    public static fromPromise<T>(promise: PromiseLike<T>): Mono<T> {
        return new Mono(async function* (signal) {
            const value = await raceWithAbort(Promise.resolve(promise), signal);
            if (value !== undefined && value !== null) {
                yield value;
            }
        });
    }

    /** Creates a Mono from a CompletionStage-like promise. */
    public static fromCompletionStage<T>(stage: PromiseLike<T> | (() => PromiseLike<T>)): Mono<T> {
        return typeof stage === "function" ? Mono.defer(() => Mono.fromPromise((stage as () => PromiseLike<T>)())) : Mono.fromPromise(stage);
    }

    /** Creates a Mono from a Future-like promise. */
    public static fromFuture<T>(future: PromiseLike<T> | (() => PromiseLike<T>), _suppressCancel?: boolean): Mono<T> {
        return typeof future === "function" ? Mono.defer(() => Mono.fromPromise((future as () => PromiseLike<T>)())) : Mono.fromPromise(future);
    }

    /** Adapts a publisher directly, preserving only its first value for Mono semantics. */
    public static fromDirect<T>(input: PublisherInput<T>): Mono<T> {
        return new Mono((signal, context) => Flux.from(input).iterate(signal, context));
    }

    /** Creates a Mono from a synchronous supplier. */
    public static fromSupplier<T>(supplier: () => T | null | undefined): Mono<T> {
        return new Mono(function* () {
            const value = supplier();
            if (value !== undefined && value !== null) {
                yield value;
            }
        });
    }

    /** Creates a Mono from a synchronous callable. */
    public static fromCallable<T>(callable: () => T | null | undefined): Mono<T> {
        return Mono.fromSupplier(callable);
    }

    /** Runs a callback and completes after it returns. */
    public static fromRunnable(runnable: () => void): Mono<void> {
        return new Mono(function* () {
            runnable();
        });
    }

    /** Creates a valued Mono for non-nullish input or an empty Mono otherwise. */
    public static justOrEmpty<T>(value: T | null | undefined): Mono<T> {
        return value === null || value === undefined ? Mono.empty<T>() : Mono.just(value);
    }

    /** Ignores every value from a source and completes when the source completes. */
    public static ignoreElements<T>(source: PublisherInput<T>): Mono<T> {
        return Flux.from(source).then() as unknown as Mono<T>;
    }

    /** Compares two sources element-by-element. */
    public static sequenceEqual<T>(
        left: PublisherInput<T>,
        right: PublisherInput<T>,
        comparator: (left: T, right: T) => boolean = Object.is
    ): Mono<boolean> {
        return new Mono(async function* (signal, context) {
            const leftIterator = toAsyncIterator(Flux.from(left).iterate(signal, context));
            const rightIterator = toAsyncIterator(Flux.from(right).iterate(signal, context));
            try {
                for (; ;) {
                    const [leftNext, rightNext] = await Promise.all([
                        raceWithAbort(leftIterator.next(), signal),
                        raceWithAbort(rightIterator.next(), signal)
                    ]);
                    if (leftNext.done || rightNext.done) {
                        yield Boolean(leftNext.done && rightNext.done);
                        return;
                    }
                    if (!comparator(leftNext.value, rightNext.value)) {
                        yield false;
                        return;
                    }
                }
            } finally {
                await Promise.allSettled([leftIterator.return?.(), rightIterator.return?.()]);
            }
        });
    }

    /** Creates a Mono by bridging callback-style producers through a sink. */
    public static override create<T>(callback: MonoSinkCallback<T>): Mono<T> {
        return new Mono(signal => {
            const queue = new AsyncQueue<T>(signal);
            let cancelled = false;
            let terminated = false;
            const cancelCallbacks = new Set<() => void>();
            const sink = {
                /** Emits one value and completes this Mono sink. */
                next(value: T) {
                    this.success(value);
                },
                /** Completes this Mono sink without a value. */
                complete() {
                    this.success();
                },
                /** Completes this Mono sink with an optional value. */
                success(value?: T) {
                    if (terminated) {
                        return;
                    }
                    terminated = true;
                    if (value !== undefined && value !== null) {
                        queue.push(value);
                    }
                    queue.complete();
                },
                /** Terminates this Mono sink with an error. */
                error(error: unknown) {
                    if (terminated) {
                        return;
                    }
                    terminated = true;
                    queue.error(error);
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

    /** Emits `0` after the provided delay. */
    public static delay(duration: DurationInput, scheduler: Scheduler = Schedulers.timeout()): Mono<number> {
        return new Mono(async function* (signal) {
            await scheduleDelay(scheduler, duration, signal);
            yield 0;
        });
    }

    /** Completes when all provided publishers complete. */
    public static when(...sources: readonly PublisherInput<unknown>[]): Mono<void> {
        return new Mono(async function* (signal, context) {
            await Promise.all(
                sources.map(async source => {
                    for await (const _ of Flux.from(source).iterate(signal, context)) {
                        // ignore
                    }
                })
            );
            yield undefined;
        });
    }

    /** Completes when all provided publishers complete and delays failures until all terminate. */
    public static whenDelayError(...sources: readonly PublisherInput<unknown>[]): Mono<void> {
        return new Mono(async function* (signal, context) {
            const errors: unknown[] = [];
            await Promise.all(
                sources.map(async source => {
                    try {
                        for await (const _ of Flux.from(source).iterate(signal, context)) {
                            // ignore
                        }
                    } catch (error) {
                        errors.push(error);
                    }
                })
            );
            if (errors.length > 0) {
                throw new CompositeError(errors);
            }
            yield undefined;
        });
    }

    /** Relays the first source to signal value, completion or error. */
    public static override firstWithSignal<T>(...sources: readonly PublisherInput<T>[]): Mono<T> {
        return new Mono(signal => {
            const queue = new AsyncQueue<T>(signal);
            let settled = false;
            for (const source of sources) {
                void (async () => {
                    try {
                        const value = await firstValueFrom(Flux.from(source), signal);
                        if (!settled) {
                            settled = true;
                            if (value !== undefined) {
                                queue.push(value);
                            }
                            queue.complete();
                        }
                    } catch (error) {
                        if (!settled) {
                            settled = true;
                            queue.error(error);
                        }
                    }
                })();
            }
            if (sources.length === 0) {
                queue.complete();
            }
            return queue;
        });
    }

    /** Relays the first non-empty value from the provided sources. */
    public static override firstWithValue<T>(...sources: readonly PublisherInput<T>[]): Mono<T> {
        return new Mono(signal => {
            const queue = new AsyncQueue<T>(signal);
            let remaining = sources.length;
            const errors: unknown[] = [];
            for (const source of sources) {
                void (async () => {
                    try {
                        const value = await firstValueFrom(Flux.from(source), signal);
                        if (value !== undefined) {
                            queue.push(value);
                            queue.complete();
                        } else {
                            remaining -= 1;
                            if (remaining === 0) {
                                errors.length > 0 ? queue.error(new CompositeError(errors)) : queue.complete();
                            }
                        }
                    } catch (error) {
                        errors.push(error);
                        remaining -= 1;
                        if (remaining === 0) {
                            queue.error(new CompositeError(errors));
                        }
                    }
                })();
            }
            if (sources.length === 0) {
                queue.complete();
            }
            return queue;
        });
    }

    /** Zips Monos by combining one value from each source. */
    public static override zip<T extends readonly unknown[], R = T>(
        sources: { [K in keyof T]: PublisherInput<T[K]> },
        combinator?: (values: T) => R
    ): Mono<R> {
        return Flux.zip(sources, combinator).next();
    }

    /** Zips Monos by combining one value from each source while delaying source failures. */
    public static zipDelayError<T extends readonly unknown[], R = T>(
        sources: { [K in keyof T]: PublisherInput<T[K]> },
        combinator?: (values: T) => R
    ): Mono<R> {
        return new Mono(async function* (signal, context) {
            const values: unknown[] = [];
            const errors: unknown[] = [];
            await Promise.all(
                sources.map(async (source, index) => {
                    try {
                        values[index] = await firstValueFrom(Flux.from(source), signal);
                    } catch (error) {
                        errors.push(error);
                    }
                })
            );
            if (errors.length > 0) {
                throw new CompositeError(errors);
            }
            if (values.some(value => value === undefined)) {
                return;
            }
            const tuple = values as unknown as T;
            yield combinator ? combinator(tuple) : (tuple as unknown as R);
        });
    }

    /** Uses a resource for the duration of a derived Mono and cleans it up afterwards. */
    public static override using<T, D>(
        resourceSupplier: () => D,
        sourceFactory: (resource: D) => PublisherInput<T>,
        resourceCleanup?: (resource: D) => void
    ): Mono<T> {
        return Flux.using(resourceSupplier, sourceFactory, resourceCleanup).next();
    }

    /** Uses an asynchronously supplied resource and runs asynchronous cleanup publishers. */
    public static override usingWhen<T, D>(
        resourceSupplier: PublisherInput<D>,
        sourceFactory: (resource: D) => PublisherInput<T>,
        asyncComplete?: (resource: D) => PublisherInput<unknown>,
        asyncError?: (resource: D, error: unknown) => PublisherInput<unknown>,
        asyncCancel?: (resource: D) => PublisherInput<unknown>
    ): Mono<T> {
        return Flux.usingWhen(resourceSupplier, sourceFactory, asyncComplete, asyncError, asyncCancel).next();
    }

    /** Resolves to the Mono value, or undefined when empty. */
    public async block(): Promise<T | undefined> {
        return this.toPromise();
    }
}
