import {Publisher} from "@/publishers/Publisher";
import {Sink} from "@/sinks/Sink";
import {Flux} from "@/publishers/Flux";
import {PipePublisher} from "@/publishers/PipePublisher";
import {Subscriber} from "@/subscriptions/Subscriber";
import {Subscription} from "@/subscriptions/Subscription";
import {AbstractPipePublisher} from "@/publishers/internal/AbstractPipePublisher";

/**
 * A cold publisher of 0 or 1 items with full backpressure support.
 *
 * A `Mono` emits at most one value followed by `onComplete`, or signals `onError`.
 * Items flow only when the downstream {@link Subscription} issues `request(n)`.
 * The convenience `subscribe(onNext, onError, onComplete)` overloads automatically
 * request `1` so the value starts flowing without extra ceremony.
 *
 * Each `subscribe()` call creates an **independent** run of the pipeline ("cold" semantics).
 *
 * @typeParam T - The type of the single item emitted by this Mono.
 *
 * @example
 * ```typescript
 * Mono.just(42)
 *   .map(n => n * 2)
 *   .subscribe(v => console.log(v));
 * // 84
 * ```
 */
export class Mono<T> extends AbstractPipePublisher<T, Mono<T>> implements PipePublisher<T> {

    protected constructor(source: Publisher<T>) { super(source); }

    protected defaultDemand(): number { return 1; }

    protected wrapSource(source: Publisher<T>): Mono<T> {
        return new Mono<T>(source);
    }

    // ─────────────────────────── Static factories ────────────────────────────

    /**
     * Creates a `Mono` from a generator function that pushes a value imperatively.
     *
     * The generator receives a {@link Sink} and should call exactly one of:
     * - `sink.next(value)` — emits the value and completes.
     * - `sink.error(err)` — terminates with an error.
     * - `sink.complete()` — completes without emitting a value (empty `Mono`).
     *
     * Delivery is deferred until the downstream subscriber issues `request(n)`.
     * Rule 1.3 is respected — `onSubscribe` is delivered before the generator runs.
     *
     * @param generator - Function that drives the `Mono` via the provided `Sink<T>`.
     * @returns A cold `Mono<T>`.
     *
     * @example
     * ```typescript
     * Mono.generate<number>(sink => sink.next(42)).subscribe(v => console.log(v)); // 42
     * ```
     */
    public static generate<T>(generator: (sink: Sink<T>) => void): Mono<T> {
        return new Mono<T>({
            subscribe(subscriber: Subscriber<T>): Subscription {
                let cancelled = false;
                let demanded = false;
                let terminated = false;
                let pending: { value: T } | null = null;
                let pendingError: Error | null = null;

                const tryDeliver = () => {
                    if (cancelled || !demanded) return;
                    if (pending !== null) {
                        const v = pending.value;
                        pending = null;
                        subscriber.onNext(v);
                        if (!cancelled) subscriber.onComplete();
                    } else if (pendingError !== null) {
                        const e = pendingError;
                        pendingError = null;
                        subscriber.onError(e);
                    } else if (terminated) {
                        subscriber.onComplete();
                    }
                };

                const sink: Sink<T> = {
                    next(v: T) {
                        if (terminated || cancelled) return;
                        terminated = true;
                        if (demanded) {
                            subscriber.onNext(v);
                            if (!cancelled) subscriber.onComplete();
                        } else {
                            pending = { value: v };
                        }
                    },
                    error(err: Error) {
                        if (terminated || cancelled) return;
                        terminated = true;
                        if (demanded) {
                            subscriber.onError(err);
                        } else {
                            pendingError = err;
                        }
                    },
                    complete() {
                        if (terminated || cancelled) return;
                        terminated = true;
                        if (demanded) subscriber.onComplete();
                        // else: deliver onComplete on next request()
                    }
                };

                try {
                    generator(sink);
                } catch (e) {
                    if (!terminated) {
                        terminated = true;
                        pendingError = e instanceof Error ? e : new Error(String(e));
                    }
                }

                const subscription: Subscription = {
                    request(n: number) {
                        if (cancelled || demanded) return;
                        if (n <= 0) {
                            subscriber.onError(new Error(`request must be > 0, but was ${n}`));
                            return;
                        }
                        demanded = true;
                        tryDeliver();
                    },
                    unsubscribe() { cancelled = true; }
                };
                subscriber.onSubscribe(subscription);
                return subscription;
            }
        });
    }

    /**
     * Wraps an existing {@link Publisher} as a `Mono`.
     *
     * @param publisher - Any `Publisher<T>` to adapt.
     * @returns A `Mono<T>` backed by the given publisher.
     */
    public static from<T>(publisher: Publisher<T>): Mono<T> {
        return new Mono<T>(publisher);
    }

    /**
     * Creates a `Mono` that emits `value` then completes.
     *
     * @param value - The single item to emit.
     * @returns A cold `Mono<T>`.
     *
     * @example
     * ```typescript
     * Mono.just(42).subscribe(v => console.log(v)); // 42
     * ```
     */
    public static just<T>(value: T): Mono<T> {
        return Mono.generate(sink => sink.next(value));
    }

    /**
     * Creates a `Mono` that emits `value` if it is non-null and non-undefined,
     * or completes empty if `value` is `null` or `undefined`.
     *
     * @param value - The value to emit, or `null`/`undefined` for an empty Mono.
     * @returns A `Mono<T>` that emits the value or completes immediately.
     */
    public static justOrEmpty<T>(value: T | null | undefined): Mono<T> {
        return value != null ? Mono.just(value) : Mono.empty<T>();
    }

    /**
     * Creates a `Mono` that completes immediately without emitting any value.
     *
     * @returns An empty, completed `Mono<T>`.
     */
    public static empty<T = never>(): Mono<T> {
        return new Mono<T>({
            subscribe(subscriber: Subscriber<T>): Subscription {
                const sub = { request() {}, unsubscribe() {} };
                subscriber.onSubscribe(sub);
                subscriber.onComplete();
                return sub;
            }
        });
    }

    /**
     * Creates a `Mono` that signals `onError` immediately upon subscription.
     *
     * @param error - The error to signal (wrapped in an `Error` if not already one).
     * @returns An errored `Mono<T>`.
     */
    public static error<T = never>(error: unknown): Mono<T> {
        const err = error instanceof Error ? error : new Error(String(error));
        return new Mono<T>({
            subscribe(subscriber: Subscriber<T>): Subscription {
                const sub = { request() {}, unsubscribe() {} };
                subscriber.onSubscribe(sub);
                subscriber.onError(err);
                return sub;
            }
        });
    }

    /**
     * Creates a `Mono` that adapts a `Promise<T>`.
     *
     * The resolved value is emitted as the single item; a rejected promise signals `onError`.
     *
     * @param promise - The promise to adapt.
     * @returns A `Mono<T>` backed by the promise.
     *
     * @example
     * ```typescript
     * Mono.fromPromise(fetch('/api/data').then(r => r.json()))
     *   .subscribe(data => console.log(data));
     * ```
     */
    public static fromPromise<T>(promise: Promise<T>): Mono<T> {
        return Mono.generate(sink =>
            promise
                .then(value => sink.next(value))
                .catch(err => sink.error(err instanceof Error ? err : new Error(String(err))))
        );
    }

    /**
     * Lazily creates a new `Mono` per subscription by calling `factory`.
     *
     * Useful when the source depends on mutable state that should be captured
     * at subscribe time rather than at assembly time.
     *
     * @param factory - Called once per subscription to produce the actual `Mono<T>`.
     * @returns A lazy `Mono<T>`.
     */
    public static defer<T>(factory: () => Mono<T>): Mono<T> {
        return new Mono<T>({
            subscribe(subscriber: Subscriber<T>): Subscription {
                return factory().subscribe(subscriber);
            }
        });
    }

    /**
     * Alias for {@link Mono.generate} — imperative push interface.
     *
     * The generator receives a {@link Sink} and should call exactly one of:
     * `sink.next(value)`, `sink.error(err)`, or `sink.complete()`.
     *
     * @param generator - Function that drives the `Mono` via the provided `Sink<T>`.
     * @returns A cold `Mono<T>`.
     */
    public static create<T>(generator: (sink: Sink<T>) => void): Mono<T> {
        return Mono.generate(generator);
    }

    /**
     * Creates a `Mono<number>` that emits `0` after `ms` milliseconds then completes.
     *
     * Useful for introducing timed delays in reactive pipelines.
     *
     * @param ms - The delay in milliseconds.
     * @returns A `Mono<number>` that emits `0` after the delay.
     *
     * @example
     * ```typescript
     * Mono.delay(500).flatMap(() => Mono.just('hello')).subscribe(v => console.log(v));
     * ```
     */
    public static delay(ms: number): Mono<number> {
        return Mono.generate<number>(sink => {
            const id = setTimeout(() => sink.next(0), ms);
            // cancellation note: timeout fires and is a no-op if sink already terminated
            void id;
        });
    }

    /**
     * Creates a `Mono<T>` from a synchronous, potentially-throwing factory function.
     *
     * The factory is called **lazily** at subscription time (not at assembly time).
     * If the factory throws, the exception is forwarded as `onError`.
     *
     * @param fn - Synchronous factory invoked once per subscription.
     * @returns A cold `Mono<T>`.
     *
     * @example
     * ```typescript
     * Mono.fromCallable(() => JSON.parse(rawJson)).subscribe(v => console.log(v));
     * ```
     */
    public static fromCallable<T>(fn: () => T): Mono<T> {
        return Mono.generate<T>(sink => {
            try { sink.next(fn()); }
            catch (e) { sink.error(e instanceof Error ? e : new Error(String(e))); }
        });
    }

    /**
     * Races multiple `Mono`s — emits the first value to arrive, cancelling the rest.
     *
     * If a source completes without emitting, it is excluded from the race.
     * If **all** sources complete empty, the resulting `Mono` also completes empty.
     * If any source errors before another source emits, the error is propagated.
     *
     * @param sources - One or more `Mono<T>` sources to race.
     * @returns A `Mono<T>` that emits the first available value.
     *
     * @example
     * ```typescript
     * Mono.firstWithValue(slow, fast).subscribe(v => console.log(v)); // fast's value
     * ```
     */
    public static firstWithValue<T>(...sources: Mono<T>[]): Mono<T> {
        if (sources.length === 0) return Mono.empty<T>();
        return Mono.generate<T>(sink => {
            let done = false;
            let completedCount = 0;
            const subs: Subscription[] = [];

            for (const source of sources) {
                const sub = source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v: T) {
                        if (done) return;
                        done = true;
                        for (const s of subs) s.unsubscribe();
                        sink.next(v);
                    },
                    onError(e: Error) {
                        if (done) return;
                        done = true;
                        for (const s of subs) s.unsubscribe();
                        sink.error(e);
                    },
                    onComplete() {
                        if (done) return;
                        completedCount++;
                        if (completedCount === sources.length) {
                            done = true;
                            sink.complete();
                        }
                    }
                });
                subs.push(sub);
            }
            for (const sub of subs) sub.request(1);
        });
    }

    /**
     * Returns a `Mono<void>` that completes when **all** given publishers complete.
     *
     * Emitted values from the sources are ignored. If any source signals `onError`,
     * the error is propagated immediately and all other sources are cancelled.
     * If no sources are provided, completes immediately.
     *
     * @param sources - Zero or more publishers to wait on.
     * @returns A `Mono<void>` that completes once every source has completed.
     *
     * @example
     * ```typescript
     * Mono.when(Mono.delay(100), Mono.delay(200))
     *   .subscribe(undefined, undefined, () => console.log('all done'));
     * ```
     */
    public static when(...sources: Publisher<unknown>[]): Mono<void> {
        if (sources.length === 0) return Mono.empty<void>();
        return Mono.generate<void>(sink => {
            let done = false;
            let completedCount = 0;
            const subs: Subscription[] = [];

            for (const source of sources) {
                const sub = source.subscribe({
                    onSubscribe(_s) {},
                    onNext(_v) {},
                    onError(e: Error) {
                        if (done) return;
                        done = true;
                        for (const s of subs) s.unsubscribe();
                        sink.error(e);
                    },
                    onComplete() {
                        if (done) return;
                        completedCount++;
                        if (completedCount === sources.length) {
                            done = true;
                            sink.complete();
                        }
                    }
                });
                subs.push(sub);
            }
            for (const sub of subs) sub.request(Number.MAX_SAFE_INTEGER);
        });
    }

    // ─────────────────────── PipePublisher operators ─────────────────────────

    /**
     * Transforms the emitted value using `fn`, producing a `Mono<R>`.
     *
     * If `fn` throws, the exception is forwarded to `onError` and the stream terminates.
     *
     * @param fn - Mapping function applied to the emitted value.
     * @returns A new `Mono<R>`.
     *
     * @example
     * ```typescript
     * Mono.just(42).map(n => n.toString()).subscribe(v => console.log(v)); // '42'
     * ```
     */
    public map<R>(fn: (value: T) => R): Mono<R> {
        return new Mono<R>({
            subscribe: (subscriber: Subscriber<R>): Subscription => {
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) {
                        try { subscriber.onNext(fn(v)); }
                        catch (e) { subscriber.onError(e instanceof Error ? e : new Error(String(e))); }
                    },
                    onError(e) { subscriber.onError(e); },
                    onComplete() { subscriber.onComplete(); }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    /**
     * Transforms the emitted value with `fn`, completing empty if the result is `null` or `undefined`.
     *
     * @param fn - Mapping function; returning `null` or `undefined` produces an empty `Mono`.
     * @returns A `Mono<NonNullable<R>>` that completes empty when the mapping returns null.
     */
    public mapNotNull<R>(fn: (value: T) => R | null | undefined): Mono<NonNullable<R>> {
        return new Mono<NonNullable<R>>({
            subscribe: (subscriber: Subscriber<NonNullable<R>>): Subscription => {
                let done = false;
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) {
                        if (done) return;
                        try {
                            const result = fn(v);
                            if (result != null) subscriber.onNext(result as NonNullable<R>);
                            else { done = true; subscriber.onComplete(); }
                        } catch (e) {
                            done = true;
                            subscriber.onError(e instanceof Error ? e : new Error(String(e)));
                        }
                    },
                    onError(e) { if (!done) { done = true; subscriber.onError(e); } },
                    onComplete() { if (!done) { done = true; subscriber.onComplete(); } }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    /**
     * Maps the emitted value to an inner `Mono<R>` and flattens the result.
     *
     * When the source emits a value, `fn` is called and the resulting inner `Mono` is
     * subscribed to. The inner `Mono`'s value (or completion/error) is forwarded downstream.
     *
     * @param fn - Maps the emitted value to a `Mono<R>` or any `Publisher<R>`.
     * @returns A `Mono<R>` containing the result of the inner publisher.
     */
    public flatMap<R>(fn: (value: T) => Mono<R>): Mono<R>;
    public flatMap<R>(fn: (value: T) => Publisher<R>): Mono<R>;
    public flatMap<R>(fn: (value: T) => Publisher<R>): Mono<R> {
        return new Mono<R>({
            subscribe: (subscriber: Subscriber<R>): Subscription => {
                let outerSub: Subscription = { request() {}, unsubscribe() {} };
                let innerSub: Subscription | null = null;
                let cancelled = false;
                let demand = 0;

                const operatorSub: Subscription = {
                    request(n: number) {
                        if (cancelled) return;
                        if (n <= 0) {
                            subscriber.onError(new Error(`request must be > 0, but was ${n}`));
                            return;
                        }
                        demand = Math.min(demand + n, Number.MAX_SAFE_INTEGER);
                        if (innerSub) innerSub.request(n);
                    },
                    unsubscribe() {
                        cancelled = true;
                        outerSub.unsubscribe();
                        innerSub?.unsubscribe();
                    }
                };

                this.source.subscribe({
                    onSubscribe(sourceSub: Subscription) {
                        outerSub = sourceSub;
                        subscriber.onSubscribe(operatorSub);
                    },
                    onNext(v: T) {
                        if (cancelled) return;
                        let inner: Publisher<R>;
                        try { inner = fn(v); }
                        catch (e) {
                            subscriber.onError(e instanceof Error ? e : new Error(String(e)));
                            return;
                        }
                        innerSub = inner.subscribe({
                            onSubscribe(_s) {},
                            onNext(r) { subscriber.onNext(r); },
                            onError(e) { subscriber.onError(e); },
                            onComplete() { subscriber.onComplete(); }
                        });
                        if (demand > 0) innerSub.request(demand);
                    },
                    onError(e) { subscriber.onError(e); },
                    onComplete() {
                        if (!innerSub) subscriber.onComplete();
                    }
                });
                outerSub.request(1);

                return operatorSub;
            }
        });
    }

    /**
     * Passes the emitted value downstream only if `predicate` returns `true`;
     * otherwise completes the `Mono` without emitting.
     *
     * @param predicate - Synchronous test applied to the emitted value.
     * @returns A `Mono<T>` that completes empty when the predicate fails.
     */
    public filter(predicate: (value: T) => boolean): Mono<T> {
        return new Mono<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let done = false;
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) {
                        if (done) return;
                        try {
                            if (predicate(v)) subscriber.onNext(v);
                            else { done = true; subscriber.onComplete(); }
                        } catch (e) {
                            done = true;
                            subscriber.onError(e instanceof Error ? e : new Error(String(e)));
                        }
                    },
                    onError(e) { if (!done) { done = true; subscriber.onError(e); } },
                    onComplete() { if (!done) { done = true; subscriber.onComplete(); } }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    /**
     * Asynchronous filter: subscribes to `predicate(value)` and forwards the value
     * downstream only if the predicate publisher emits `true`; otherwise completes empty.
     *
     * @param predicate - Returns a `Publisher<boolean>` for the emitted value.
     * @returns A `Mono<T>` that completes empty when the predicate is not satisfied.
     */
    public filterWhen(predicate: (value: T) => Publisher<boolean>): Mono<T> {
        return new Mono<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let outerSub: Subscription = { request() {}, unsubscribe() {} };
                let innerSub: Subscription | null = null;
                let cancelled = false;

                const operatorSub: Subscription = {
                    request(n: number) {
                        if (cancelled) return;
                        if (n <= 0) {
                            subscriber.onError(new Error(`request must be > 0, but was ${n}`));
                            return;
                        }
                    },
                    unsubscribe() {
                        cancelled = true;
                        outerSub.unsubscribe();
                        innerSub?.unsubscribe();
                    }
                };

                this.source.subscribe({
                    onSubscribe(sourceSub: Subscription) {
                        outerSub = sourceSub;
                        subscriber.onSubscribe(operatorSub);
                    },
                    onNext(v: T) {
                        if (cancelled) return;
                        let pred: Publisher<boolean>;
                        try { pred = predicate(v); }
                        catch (e) {
                            subscriber.onError(e instanceof Error ? e : new Error(String(e)));
                            return;
                        }
                        let innerDone = false;
                        innerSub = pred.subscribe({
                            onSubscribe(_s) {},
                            onNext(passes) {
                                if (innerDone) return;
                                innerDone = true;
                                if (passes) subscriber.onNext(v);
                                else subscriber.onComplete();
                            },
                            onError(e) { if (!innerDone) { innerDone = true; subscriber.onError(e); } },
                            onComplete() { if (!innerDone) { innerDone = true; subscriber.onComplete(); } }
                        });
                        innerSub.request(1);
                    },
                    onError(e) { subscriber.onError(e); },
                    onComplete() { if (!innerSub) subscriber.onComplete(); }
                });
                outerSub.request(1);

                return operatorSub;
            }
        });
    }

    /**
     * Unsafe type cast — changes the declared element type to `R` without any
     * runtime conversion. Use only when you are certain the actual runtime type is compatible.
     *
     * @typeParam R - The target element type.
     * @returns This `Mono` re-typed as `Mono<R>`.
     */
    public cast<R>(): Mono<R> {
        return new Mono<R>(this.source as unknown as Publisher<R>);
    }

    /**
     * Executes a side-effect function after the stream ends for **any** reason —
     * normal completion or error. Unlike {@link Flux#doFinally}, this does not fire on
     * cancellation (a `Mono` cancel is semantically identical to an unneeded result).
     *
     * Any exception thrown by `fn` is silently swallowed.
     *
     * @param fn - Side-effect to run after any terminal event.
     * @returns A `Mono<T>` with the side effect attached.
     */
    public doFinally(fn: () => void): Mono<T> {
        return new Mono<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) { subscriber.onNext(v); },
                    onError(e) {
                        try { fn(); } catch (_) {}
                        subscriber.onError(e);
                    },
                    onComplete() {
                        try { fn(); } catch (_) {}
                        subscriber.onComplete();
                    }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    /**
     * Low-level escape hatch for bridging imperative push-based sources into a `Mono<R>`.
     *
     * The `producer` receives three callbacks and must call them to drive the stream.
     * `onRequest` and `onUnsubscribe` let the caller respond to downstream demand and
     * cancellation respectively.
     *
     * @param producer - Callback that receives `(onNext, onError, onComplete)` and drives the stream.
     * @param onRequest - Called when the downstream subscriber requests `n` more items.
     * @param onUnsubscribe - Called when the downstream subscriber cancels.
     * @returns A `Mono<R>` backed by the imperative producer.
     */
    public pipe<R>(
        producer: (onNext: (value: R) => void, onError: (error: Error) => void, onComplete: () => void) => void,
        onRequest: (request: number) => void,
        onUnsubscribe: () => void
    ): Mono<R> {
        return new Mono<R>({
            subscribe: (subscriber: Subscriber<R>): Subscription => {
                producer(
                    v => subscriber.onNext(v),
                    e => subscriber.onError(e),
                    () => subscriber.onComplete()
                );
                const sub = { request: onRequest, unsubscribe: onUnsubscribe };
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    // ─────────────────────── Mono-specific operators ─────────────────────────

    /**
     * Maps the emitted value to a multi-item publisher and returns the items as a `Flux<R>`.
     *
     * The inner publisher is subscribed to once the source emits its single value.
     * All items from the inner publisher are forwarded downstream. If the source is empty,
     * the resulting `Flux` also completes empty.
     *
     * @param mapper - Maps the emitted value to a `Flux<R>`, `Mono<R>`, or any `Publisher<R>`.
     * @returns A `Flux<R>` containing all items from the inner publisher.
     *
     * @example
     * ```typescript
     * Mono.just(3)
     *   .flatMapMany(n => Flux.range(1, n))
     *   .subscribe(v => console.log(v)); // 1  2  3
     * ```
     */
    public flatMapMany<R>(mapper: (value: T) => Flux<R>): Flux<R>;
    public flatMapMany<R>(mapper: (value: T) => Mono<R>): Flux<R>;
    public flatMapMany<R>(mapper: (value: T) => Publisher<R>): Flux<R>;
    public flatMapMany<R>(mapper: (value: T) => Publisher<R>): Flux<R> {
        return Flux.from<R>({
            subscribe: (subscriber: Subscriber<R>): Subscription => {
                let outerSub: Subscription = { request() {}, unsubscribe() {} };
                let innerSub: Subscription | null = null;
                let cancelled = false;
                let demand = 0;

                const operatorSub: Subscription = {
                    request(n: number) {
                        if (cancelled) return;
                        if (n <= 0) {
                            subscriber.onError(new Error(`request must be > 0, but was ${n}`));
                            return;
                        }
                        demand = Math.min(demand + n, Number.MAX_SAFE_INTEGER);
                        if (innerSub) innerSub.request(n);
                    },
                    unsubscribe() {
                        cancelled = true;
                        outerSub.unsubscribe();
                        innerSub?.unsubscribe();
                    }
                };

                this.source.subscribe({
                    onSubscribe(sourceSub: Subscription) {
                        outerSub = sourceSub;
                        subscriber.onSubscribe(operatorSub);
                    },
                    onNext(v: T) {
                        if (cancelled) return;
                        let inner: Publisher<R>;
                        try { inner = mapper(v); }
                        catch (e) {
                            subscriber.onError(e instanceof Error ? e : new Error(String(e)));
                            return;
                        }
                        innerSub = inner.subscribe({
                            onSubscribe(_s) {},
                            onNext(r) { subscriber.onNext(r); },
                            onError(e) { subscriber.onError(e); },
                            onComplete() { subscriber.onComplete(); }
                        });
                        if (demand > 0) innerSub.request(demand);
                    },
                    onError(e) { subscriber.onError(e); },
                    onComplete() { if (!innerSub) subscriber.onComplete(); }
                });
                outerSub.request(1);

                return operatorSub;
            }
        });
    }

    /**
     * Subscribes to this `Mono` and `other` concurrently and combines their values
     * into a `[T, R]` tuple once both emit.
     *
     * If either `Mono` completes empty, the resulting `Mono` also completes empty.
     * If either errors, the error is forwarded and the other is cancelled.
     *
     * @param other - The second `Mono` to zip with.
     * @returns A `Mono<[T, R]>` emitting the combined tuple.
     *
     * @example
     * ```typescript
     * Mono.just(1).zipWith(Mono.just('a')).subscribe(([n, s]) => console.log(n, s)); // 1 a
     * ```
     */
    public zipWith<R>(other: Mono<R>): Mono<[T, R]> {
        return Mono.generate<[T, R]>(sink => {
            let left: T | undefined;
            let right: R | undefined;
            let leftDone = false;
            let rightDone = false;
            let failed = false;

            const fail = (e: Error) => {
                if (failed) return;
                failed = true;
                leftSub?.unsubscribe();
                rightSub?.unsubscribe();
                sink.error(e);
            };

            const tryEmit = () => {
                if (leftDone && rightDone) {
                    sink.next([left as T, right as R]);
                }
            };

            let leftSub: Subscription;
            let rightSub: Subscription;

            leftSub = this.source.subscribe({
                onSubscribe(_s) {},
                onNext(v) { left = v; leftDone = true; tryEmit(); },
                onError(e) { fail(e); },
                onComplete() { if (!leftDone) { rightSub?.unsubscribe(); sink.complete(); } }
            });

            rightSub = other.subscribe({
                onSubscribe(_s) {},
                onNext(v) { right = v; rightDone = true; tryEmit(); },
                onError(e) { fail(e); },
                onComplete() { if (!rightDone) { leftSub?.unsubscribe(); sink.complete(); } }
            });

            leftSub.request(1);
            rightSub.request(1);
        });
    }

    /**
     * Emits the source value and uses `fn` to derive a second `Mono`, combining both into a tuple.
     *
     * The source value is passed to `fn` to produce the second `Mono`. Both values are then
     * combined into a `[T, R]` tuple. If either `Mono` is empty or errors, those signals
     * propagate to the subscriber.
     *
     * @param fn - Called with the emitted value to produce the second `Mono<R>`.
     * @returns A `Mono<[T, R]>` emitting both values as a tuple.
     *
     * @example
     * ```typescript
     * Mono.just(42).zipWhen(n => Mono.just(n.toString())).subscribe(([n, s]) => console.log(n, s));
     * // 42 '42'
     * ```
     */
    public zipWhen<R>(fn: (value: T) => Mono<R>): Mono<[T, R]> {
        return Mono.generate<[T, R]>(sink => {
            const outerSub = this.source.subscribe({
                onSubscribe(_s) {},
                onNext(v: T) {
                    let inner: Mono<R>;
                    try { inner = fn(v); }
                    catch (e) { sink.error(e instanceof Error ? e : new Error(String(e))); return; }

                    const innerSub = inner.subscribe({
                        onSubscribe(_s) {},
                        onNext(r) { sink.next([v, r]); },
                        onError(e) { sink.error(e); },
                        onComplete() { sink.complete(); }
                    });
                    innerSub.request(1);
                },
                onError(e) { sink.error(e); },
                onComplete() { sink.complete(); }
            });
            outerSub.request(1);
        });
    }

    /**
     * Emits `true` if this `Mono` emits a value, `false` if it completes empty.
     *
     * @returns A `Mono<boolean>`.
     */
    public hasElement(): Mono<boolean> {
        return Mono.generate<boolean>(sink => {
            const sub = this.source.subscribe({
                onSubscribe(_s) {},
                onNext(_v) { sub.unsubscribe(); sink.next(true); },
                onError(e) { sink.error(e); },
                onComplete() { sink.next(false); }
            });
            sub.request(1);
        });
    }

    /**
     * Side effect executed when the `Mono` emits its value.
     *
     * Does **not** affect the emitted value or stream lifecycle.
     * Exceptions thrown by `fn` are forwarded as `onError`.
     *
     * @param fn - Called with the emitted value.
     * @returns A `Mono<T>` with the side effect attached.
     *
     * @example
     * ```typescript
     * Mono.just(42).doOnSuccess(v => console.log('got', v)).subscribe();
     * ```
     */
    public doOnSuccess(fn: (value: T) => void): Mono<T> {
        return new Mono<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) {
                        try { fn(v); } catch (e) {
                            subscriber.onError(e instanceof Error ? e : new Error(String(e)));
                            return;
                        }
                        subscriber.onNext(v);
                    },
                    onError(e) { subscriber.onError(e); },
                    onComplete() { subscriber.onComplete(); }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    /**
     * Converts a terminal error into a normal `onComplete` signal.
     *
     * If a `predicate` is supplied, only errors for which the predicate returns `true`
     * are swallowed; others are re-propagated as-is.
     *
     * @param predicate - Optional filter; when absent all errors are converted.
     * @returns A `Mono<T>` that completes rather than errors (when predicate matches).
     *
     * @example
     * ```typescript
     * Mono.error(new Error('oops')).onErrorComplete().subscribe(undefined, undefined, () => console.log('done'));
     * ```
     */
    public onErrorComplete(predicate?: (e: Error) => boolean): Mono<T> {
        return new Mono<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) { subscriber.onNext(v); },
                    onError(e) {
                        if (!predicate || predicate(e)) subscriber.onComplete();
                        else subscriber.onError(e);
                    },
                    onComplete() { subscriber.onComplete(); }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    /**
     * Ignores any value emitted by this `Mono` and, upon normal completion,
     * emits `value` instead.
     *
     * Errors from the source are propagated unchanged.
     *
     * @param value - The value to emit after this `Mono` completes.
     * @returns A `Mono<R>` that emits `value` on source completion.
     *
     * @example
     * ```typescript
     * Mono.just('ignored').thenReturn(42).subscribe(v => console.log(v)); // 42
     * ```
     */
    public thenReturn<R>(value: R): Mono<R> {
        return new Mono<R>({
            subscribe: (subscriber: Subscriber<R>): Subscription => {
                let done = false;
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(_v) { /* swallow the emitted value */ },
                    onError(e) { if (!done) { done = true; subscriber.onError(e); } },
                    onComplete() {
                        if (!done) {
                            done = true;
                            subscriber.onNext(value);
                            subscriber.onComplete();
                        }
                    }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    /**
     * Ignores the value emitted by this `Mono` and, upon completion, subscribes to `other`
     * and forwards its result downstream.
     *
     * If `other` is omitted, returns a `Mono<void>` that completes when this `Mono` completes.
     * Errors from this `Mono` are propagated without subscribing to `other`.
     *
     * @param other - Optional `Mono<V>` to subscribe to after this `Mono` completes.
     * @returns `Mono<void>` when called with no argument; `Mono<V>` when `other` is provided.
     *
     * @example
     * ```typescript
     * // Sequencing: run A, then run B and return B's value
     * Mono.just('ignored').then(Mono.just(42)).subscribe(v => console.log(v)); // 42
     *
     * // Completion signal only
     * Mono.just('ignored').then().subscribe(undefined, undefined, () => console.log('done'));
     * ```
     */
    public then(): Mono<void>;
    public then<V>(other: Mono<V>): Mono<V>;
    public then<V>(other?: Mono<V>): Mono<void> | Mono<V> {
        if (!other) {
            return new Mono<void>({
                subscribe: (subscriber: Subscriber<void>): Subscription => {
                    let done = false;
                    const sub = this.source.subscribe({
                        onSubscribe(_s) {},
                        onNext(_v) {},
                        onError(e) { if (!done) { done = true; subscriber.onError(e); } },
                        onComplete() { if (!done) { done = true; subscriber.onComplete(); } }
                    });
                    subscriber.onSubscribe(sub);
                    return sub;
                }
            });
        }

        return new Mono<V>({
            subscribe: (subscriber: Subscriber<V>): Subscription => {
                let outerSub: Subscription = { request() {}, unsubscribe() {} };
                let innerSub: Subscription | null = null;
                let cancelled = false;
                let demand = 0;

                const operatorSub: Subscription = {
                    request(n: number) {
                        if (cancelled) return;
                        if (n <= 0) {
                            subscriber.onError(new Error(`request must be > 0, but was ${n}`));
                            return;
                        }
                        demand = Math.min(demand + n, Number.MAX_SAFE_INTEGER);
                        if (innerSub) innerSub.request(n);
                    },
                    unsubscribe() {
                        cancelled = true;
                        outerSub.unsubscribe();
                        innerSub?.unsubscribe();
                    }
                };

                this.source.subscribe({
                    onSubscribe(s) { outerSub = s; subscriber.onSubscribe(operatorSub); },
                    onNext(_v) {},
                    onError(e) { if (!cancelled) subscriber.onError(e); },
                    onComplete() {
                        if (cancelled) return;
                        innerSub = other.subscribe({
                            onSubscribe(_s) {},
                            onNext(v) { subscriber.onNext(v); },
                            onError(e) { subscriber.onError(e); },
                            onComplete() { subscriber.onComplete(); }
                        });
                        if (demand > 0) innerSub.request(demand);
                    }
                });
                outerSub.request(1);

                return operatorSub;
            }
        });
    }

    /**
     * Ignores the value (or absence of a value) emitted by this `Mono` and, once
     * the source completes, subscribes to the provided `Publisher<V>` and forwards
     * all of its items and terminal signal downstream as a `Flux<V>`.
     *
     * If the source signals an error the error is forwarded immediately and `other`
     * is never subscribed.
     *
     * This mirrors `Mono#thenMany` from Project Reactor.
     *
     * @param other - The publisher whose items should be delivered after this Mono completes.
     * @returns A `Flux<V>` that forwards all items from `other`.
     *
     * @example
     * ```typescript
     * Mono.just('init')
     *   .thenMany(Flux.just(1, 2, 3))
     *   .subscribe(v => console.log(v));
     * // 1  2  3
     * ```
     */
    public thenMany<V>(other: Publisher<V>): Flux<V> {
        return Flux.from<V>({
            subscribe: (subscriber: Subscriber<V>): Subscription => {
                let outerSub: Subscription = { request() {}, unsubscribe() {} };
                let innerSub: Subscription | null = null;
                let cancelled = false;
                let demand = 0;

                const operatorSub: Subscription = {
                    request(n: number) {
                        if (cancelled) return;
                        if (n <= 0) {
                            subscriber.onError(new Error(`request must be > 0, but was ${n}`));
                            return;
                        }
                        demand = Math.min(demand + n, Number.MAX_SAFE_INTEGER);
                        if (innerSub) innerSub.request(n);
                    },
                    unsubscribe() {
                        cancelled = true;
                        outerSub.unsubscribe();
                        innerSub?.unsubscribe();
                    }
                };

                this.source.subscribe({
                    onSubscribe(s) { outerSub = s; subscriber.onSubscribe(operatorSub); },
                    onNext(_v) {},
                    onError(e) { if (!cancelled) subscriber.onError(e); },
                    onComplete() {
                        if (cancelled) return;
                        innerSub = other.subscribe({
                            onSubscribe(_s) {},
                            onNext(v) { subscriber.onNext(v); },
                            onError(e) { subscriber.onError(e); },
                            onComplete() { subscriber.onComplete(); }
                        });
                        if (demand > 0) innerSub.request(demand);
                    }
                });
                outerSub.request(1);

                return operatorSub;
            }
        });
    }

    /**
     * Delays delivery of the emitted value by `ms` milliseconds.
     *
     * The delay is introduced **after** the source emits; completion and errors
     * are forwarded without delay.
     *
     * @param ms - Delay in milliseconds.
     * @returns A `Mono<T>` whose value arrives `ms` milliseconds later.
     *
     * @example
     * ```typescript
     * Mono.just('hello').delayElement(200).subscribe(v => console.log(v));
     * ```
     */
    public delayElement(ms: number): Mono<T> {
        return new Mono<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let timerId: ReturnType<typeof setTimeout> | null = null;
                let cancelled = false;
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) {
                        if (cancelled) return;
                        timerId = setTimeout(() => {
                            if (!cancelled) {
                                subscriber.onNext(v);
                                subscriber.onComplete();
                            }
                        }, ms);
                    },
                    onError(e) { subscriber.onError(e); },
                    onComplete() { /* wait for timer, or complete immediately if no value */ }
                });
                const proxy: Subscription = {
                    request(n) { sub.request(n); },
                    unsubscribe() {
                        cancelled = true;
                        if (timerId !== null) clearTimeout(timerId);
                        sub.unsubscribe();
                    }
                };
                subscriber.onSubscribe(proxy);
                return proxy;
            }
        });
    }

    /**
     * Holds the emitted value until a trigger publisher (returned by `triggerFn`) emits
     * or completes, then forwards the value downstream.
     *
     * If the trigger errors, the error is propagated and the value is discarded.
     * If the source is empty, completes immediately without invoking `triggerFn`.
     *
     * @param triggerFn - Receives the emitted value and returns a trigger `Publisher`.
     * @returns A `Mono<T>` that emits after the trigger fires.
     *
     * @example
     * ```typescript
     * Mono.just(42).delayUntil(() => Mono.delay(300)).subscribe(v => console.log(v));
     * ```
     */
    public delayUntil(triggerFn: (value: T) => Publisher<unknown>): Mono<T> {
        return new Mono<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let outerSub: Subscription = { request() {}, unsubscribe() {} };
                let innerSub: Subscription | null = null;
                let cancelled = false;

                const operatorSub: Subscription = {
                    request(n) { if (!cancelled) outerSub.request(n); },
                    unsubscribe() {
                        cancelled = true;
                        outerSub.unsubscribe();
                        innerSub?.unsubscribe();
                    }
                };

                this.source.subscribe({
                    onSubscribe(s) { outerSub = s; subscriber.onSubscribe(operatorSub); },
                    onNext(v: T) {
                        if (cancelled) return;
                        let trigger: Publisher<unknown>;
                        try { trigger = triggerFn(v); }
                        catch (e) {
                            subscriber.onError(e instanceof Error ? e : new Error(String(e)));
                            return;
                        }
                        let triggered = false;
                        innerSub = trigger.subscribe({
                            onSubscribe(_s) {},
                            onNext(_u) {
                                if (triggered || cancelled) return;
                                triggered = true;
                                innerSub?.unsubscribe();
                                subscriber.onNext(v);
                                subscriber.onComplete();
                            },
                            onError(e) { if (!cancelled) subscriber.onError(e); },
                            onComplete() {
                                if (!triggered && !cancelled) {
                                    triggered = true;
                                    subscriber.onNext(v);
                                    subscriber.onComplete();
                                }
                            }
                        });
                        innerSub.request(Number.MAX_SAFE_INTEGER);
                    },
                    onError(e) { if (!cancelled) subscriber.onError(e); },
                    onComplete() { if (!cancelled && !innerSub) subscriber.onComplete(); }
                });

                return operatorSub;
            }
        });
    }

    /**
     * Falls back to `other` if this `Mono` completes without emitting a value (empty).
     *
     * If this `Mono` emits a value or errors, `other` is never subscribed to.
     *
     * @param other - The fallback `Mono<T>` to subscribe to when the source is empty.
     * @returns A `Mono<T>` that emits from `other` when the source is empty.
     *
     * @example
     * ```typescript
     * Mono.empty<number>().or(Mono.just(99)).subscribe(v => console.log(v)); // 99
     * ```
     */
    public or(other: Mono<T>): Mono<T> {
        return new Mono<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let outerSub: Subscription = { request() {}, unsubscribe() {} };
                let innerSub: Subscription | null = null;
                let cancelled = false;
                let emitted = false;

                const operatorSub: Subscription = {
                    request(n) {
                        if (cancelled) return;
                        if (innerSub) innerSub.request(n);
                        else outerSub.request(n);
                    },
                    unsubscribe() {
                        cancelled = true;
                        outerSub.unsubscribe();
                        innerSub?.unsubscribe();
                    }
                };

                this.source.subscribe({
                    onSubscribe(s) { outerSub = s; subscriber.onSubscribe(operatorSub); },
                    onNext(v: T) {
                        if (cancelled) return;
                        emitted = true;
                        subscriber.onNext(v);
                    },
                    onError(e) { if (!cancelled) subscriber.onError(e); },
                    onComplete() {
                        if (cancelled || emitted) return;
                        innerSub = other.subscribe({
                            onSubscribe(_s) {},
                            onNext(v) { if (!cancelled) subscriber.onNext(v); },
                            onError(e) { if (!cancelled) subscriber.onError(e); },
                            onComplete() { if (!cancelled) subscriber.onComplete(); }
                        });
                        innerSub.request(1);
                    }
                });

                return operatorSub;
            }
        });
    }

    /**
     * Re-subscribes to this `Mono` on error, using a control publisher to decide
     * whether and when to retry.
     *
     * Each time the source errors, the error is pushed to the control stream returned
     * by `fn`. If that stream emits any item, the source is re-subscribed. If it
     * completes or errors, that terminal signal is forwarded downstream.
     *
     * @param fn - Receives a `Flux<Error>` of upstream errors; returns a control publisher.
     * @returns A `Mono<T>` that retries according to the control signal.
     *
     * @example
     * ```typescript
     * Mono.error(new Error('boom'))
     *   .retryWhen(errors => errors.take(3))
     *   .subscribe(undefined, e => console.error(e));
     * ```
     */
    public retryWhen(fn: (errors: Flux<Error>) => Publisher<unknown>): Mono<T> {
        return new Mono<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let cancelled = false;
                let currentSub: Subscription | null = null;
                let emitted = false;

                // Relay subscriber that the control stream holds
                let controlSub: { onNext: (e: Error) => void } | null = null;

                const errorRelay = Flux.from<Error>({
                    subscribe(relaySubscriber) {
                        controlSub = { onNext: (e) => relaySubscriber.onNext(e) };
                        const sub: Subscription = {
                            request(_n) {},
                            unsubscribe() { cancelled = true; }
                        };
                        relaySubscriber.onSubscribe(sub);
                        return sub;
                    }
                });

                const control = fn(errorRelay);

                let controlSubscriber: {
                    onNext: (v: unknown) => void;
                    onError: (e: Error) => void;
                    onComplete: () => void;
                };

                const attempt = () => {
                    if (cancelled || emitted) return;
                    currentSub = this.source.subscribe({
                        onSubscribe(_s) {},
                        onNext(v: T) {
                            if (cancelled || emitted) return;
                            emitted = true;
                            subscriber.onNext(v);
                            subscriber.onComplete();
                        },
                        onError(e: Error) {
                            if (cancelled) return;
                            controlSub?.onNext(e);
                        },
                        onComplete() {
                            if (!cancelled && !emitted) subscriber.onComplete();
                        }
                    });
                    currentSub.request(1);
                };

                const controlSub2 = control.subscribe({
                    onSubscribe(_s) {},
                    onNext(_v: unknown) { if (!cancelled && !emitted) attempt(); },
                    onError(e: Error) { if (!cancelled) subscriber.onError(e); },
                    onComplete() { if (!cancelled && !emitted) subscriber.onComplete(); }
                });
                controlSub2.request(Number.MAX_SAFE_INTEGER);

                controlSubscriber = {
                    onNext: (_v) => { if (!cancelled && !emitted) attempt(); },
                    onError: (e) => { if (!cancelled) subscriber.onError(e); },
                    onComplete: () => { if (!cancelled && !emitted) subscriber.onComplete(); }
                };
                void controlSubscriber;

                attempt();

                const operatorSub: Subscription = {
                    request(_n) {},
                    unsubscribe() {
                        cancelled = true;
                        currentSub?.unsubscribe();
                        controlSub2.unsubscribe();
                    }
                };
                subscriber.onSubscribe(operatorSub);
                return operatorSub;
            }
        });
    }

    /**
     * Alias for {@link Mono#toPromise} — returns a `Promise<T | null>` that resolves
     * with the emitted value (or `null` if the `Mono` is empty).
     *
     * @returns A `Promise<T | null>`.
     */
    public toFuture(): Promise<T | null> {
        return this.toPromise();
    }

    /**
     * Subscribes to this `Mono` and returns a `Promise<T | null>`.
     *
     * The promise resolves with the emitted value, or with `null` if the `Mono` completes empty.
     * The promise rejects if the `Mono` signals an error.
     *
     * @returns A `Promise<T | null>` that resolves when the `Mono` terminates.
     */
    public toPromise(): Promise<T | null> {
        return new Promise<T | null>((resolve, reject) => {
            let value: T | null = null;
            const sub = this.subscribe({
                onSubscribe(_s) {},
                onNext(v) { value = v; },
                onError(e) { reject(e); },
                onComplete() { resolve(value); }
            });
            sub.request(Number.MAX_SAFE_INTEGER);
        });
    }
}
