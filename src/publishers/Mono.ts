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
