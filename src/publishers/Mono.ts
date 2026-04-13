import {Publisher} from "@/publishers/Publisher";
import {Sink} from "@/sinks/Sink";
import {Flux} from "@/publishers/Flux";
import {PipePublisher} from "@/publishers/PipePublisher";
import {Subscriber} from "@/subscriptions/Subscriber";
import {Subscription} from "@/subscriptions/Subscription";
import {AbstractPipePublisher} from "@/publishers/internal/AbstractPipePublisher";

export class Mono<T> extends AbstractPipePublisher<T, Mono<T>> implements PipePublisher<T> {

    protected constructor(source: Publisher<T>) { super(source); }

    protected defaultDemand(): number { return 1; }

    protected wrapSource(source: Publisher<T>): Mono<T> {
        return new Mono<T>(source);
    }

    // ─────────────────────────── Static factories ────────────────────────────

    /**
     * Creates a Mono from a generator function receiving a Sink.
     * Calling sink.next(value) emits the value (implicitly completing the Mono).
     * Calling sink.error(err) terminates with error.
     * Calling sink.complete() completes the Mono empty.
     * Delivery is deferred until downstream requests.
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

    /** Wraps an existing Publisher as a Mono. */
    public static from<T>(publisher: Publisher<T>): Mono<T> {
        return new Mono<T>(publisher);
    }

    /** Emits a single value then completes. */
    public static just<T>(value: T): Mono<T> {
        return Mono.generate(sink => sink.next(value));
    }

    /** Emits the value if non-null/undefined, otherwise completes empty. */
    public static justOrEmpty<T>(value: T | null | undefined): Mono<T> {
        return value != null ? Mono.just(value) : Mono.empty<T>();
    }

    /** Completes immediately without emitting any value. */
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

    /** Terminates with an error immediately. */
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

    /** Creates a Mono from a Promise. Emits the resolved value or errors on rejection. */
    public static fromPromise<T>(promise: Promise<T>): Mono<T> {
        return Mono.generate(sink =>
            promise
                .then(value => sink.next(value))
                .catch(err => sink.error(err instanceof Error ? err : new Error(String(err))))
        );
    }

    /** Lazily creates a Mono per subscription using the factory. */
    public static defer<T>(factory: () => Mono<T>): Mono<T> {
        return new Mono<T>({
            subscribe(subscriber: Subscriber<T>): Subscription {
                return factory().subscribe(subscriber);
            }
        });
    }

    // ─────────────────────── PipePublisher operators ─────────────────────────

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

    /** Passes the value if predicate returns true, otherwise completes empty. */
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

    public cast<R>(): Mono<R> {
        return new Mono<R>(this.source as unknown as Publisher<R>);
    }

    /** Runs fn on any terminal signal (onComplete or onError). */
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

    /** Maps the value to a multi-value Publisher, returning a Flux. */
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

    /** Subscribes to both Monos concurrently and combines their single values into a tuple. */
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

    /** Emits the source value then applies fn to derive a second Mono, combining both into a tuple. */
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

    /** Returns Mono<true> if this Mono emits a value, Mono<false> if it completes empty. */
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

    /** Subscribes and returns a Promise resolving to the emitted value, or null if empty. */
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
