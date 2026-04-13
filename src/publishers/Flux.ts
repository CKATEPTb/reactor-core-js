import {PipePublisher} from "@/publishers/PipePublisher";
import {Publisher} from "@/publishers/Publisher";
import {Sink} from "@/sinks/Sink";
import {Mono} from "@/publishers/Mono";
import {Subscriber} from "@/subscriptions/Subscriber";
import {Subscription} from "@/subscriptions/Subscription";
import ReplayAllSink from "@/sinks/ReplayAllSink";
import {AbstractPipePublisher} from "@/publishers/internal/AbstractPipePublisher";
import {Schedulers} from "@/schedulers";

export class Flux<T> extends AbstractPipePublisher<T, Flux<T>> implements PipePublisher<T> {

    protected constructor(source: Publisher<T>) { super(source); }

    protected defaultDemand(): number { return Number.MAX_SAFE_INTEGER; }

    protected wrapSource(source: Publisher<T>): Flux<T> {
        return new Flux<T>(source);
    }

    // ─────────────────────────── Static factories ────────────────────────────

    /** Wraps an existing Publisher as a Flux. */
    public static from<T>(publisher: Publisher<T>): Flux<T> {
        return new Flux<T>(publisher);
    }

    /**
     * Creates a Flux from a generator that can call sink.next() multiple times,
     * then sink.complete() or sink.error(). Delivery respects backpressure.
     */
    public static generate<T>(generator: (sink: Sink<T>) => void): Flux<T> {
        return new Flux<T>({
            subscribe(subscriber: Subscriber<T>): Subscription {
                const buffer: T[] = [];
                let demand = 0;
                let terminated = false;
                let terminalDelivered = false;
                let terminalError: Error | null = null;
                let cancelled = false;
                let draining = false;

                const drain = () => {
                    if (draining || cancelled) return;
                    draining = true;
                    try {
                        while (demand > 0 && buffer.length > 0) {
                            demand--;
                            subscriber.onNext(buffer.shift()!);
                            if (cancelled) return;
                        }
                        if (!terminalDelivered && buffer.length === 0 && terminated) {
                            terminalDelivered = true;
                            terminalError
                                ? subscriber.onError(terminalError)
                                : subscriber.onComplete();
                        }
                    } finally {
                        draining = false;
                    }
                };

                const sink: Sink<T> = {
                    next(v: T) {
                        if (terminated || cancelled) return;
                        if (demand > 0) { demand--; subscriber.onNext(v); }
                        else buffer.push(v);
                    },
                    error(err: Error) {
                        if (terminated || cancelled) return;
                        terminated = true;
                        terminalError = err;
                        drain();
                    },
                    complete() {
                        if (terminated || cancelled) return;
                        terminated = true;
                        drain();
                    }
                };

                const subscription: Subscription = {
                    request(n: number) {
                        if (cancelled) return;
                        if (n <= 0) {
                            subscriber.onError(new Error(`request must be > 0, but was ${n}`));
                            return;
                        }
                        demand = Math.min(demand + n, Number.MAX_SAFE_INTEGER);
                        drain();
                    },
                    unsubscribe() {
                        cancelled = true;
                        buffer.length = 0;
                    }
                };

                // Rule 1.3: onSubscribe MUST be the first signal — call it before the generator.
                subscriber.onSubscribe(subscription);

                try { generator(sink); }
                catch (e) {
                    if (!terminated) {
                        terminated = true;
                        terminalError = e instanceof Error ? e : new Error(String(e));
                        drain();
                    }
                }

                return subscription;
            }
        });
    }

    /** Creates a Flux that emits all elements of the iterable in order. */
    public static fromIterable<T>(iterable: Iterable<T>): Flux<T> {
        return Flux.generate<T>(sink => {
            for (const item of iterable) sink.next(item);
            sink.complete();
        });
    }

    /** Creates a Flux emitting integers from [start, start + count). */
    public static range(start: number, count: number): Flux<number> {
        return Flux.generate<number>(sink => {
            for (let i = 0; i < count; i++) sink.next(start + i);
            sink.complete();
        });
    }

    /** Completes immediately without emitting any value. */
    public static empty<T = never>(): Flux<T> {
        return new Flux<T>({
            subscribe(subscriber: Subscriber<T>): Subscription {
                const sub = { request() {}, unsubscribe() {} };
                subscriber.onSubscribe(sub);
                subscriber.onComplete();
                return sub;
            }
        });
    }

    /** Lazily creates a Flux per subscription using the factory. */
    public static defer<T>(factory: () => Flux<T>): Flux<T> {
        return new Flux<T>({
            subscribe(subscriber: Subscriber<T>): Subscription {
                return factory().subscribe(subscriber);
            }
        });
    }

    /** Creates a Flux that emits the provided items in order. */
    public static just<T>(...items: T[]): Flux<T> {
        return Flux.fromIterable(items);
    }

    /** Creates a Flux that immediately signals an error. */
    public static error<T = never>(error: Error): Flux<T> {
        return new Flux<T>({
            subscribe(subscriber: Subscriber<T>): Subscription {
                const sub = { request() {}, unsubscribe() {} };
                subscriber.onSubscribe(sub);
                subscriber.onError(error);
                return sub;
            }
        });
    }

    /** Creates a Flux that never emits any signal. */
    public static never<T = never>(): Flux<T> {
        return new Flux<T>({
            subscribe(subscriber: Subscriber<T>): Subscription {
                const sub = { request() {}, unsubscribe() {} };
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    // ─────────────────── PipePublisher operators ──────────────────

    public map<R>(fn: (value: T) => R): Flux<R> {
        return new Flux<R>({
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

    public mapNotNull<R>(fn: (value: T) => R | null | undefined): Flux<NonNullable<R>> {
        return new Flux<NonNullable<R>>({
            subscribe: (subscriber: Subscriber<NonNullable<R>>): Subscription => {
                let sourceSub!: Subscription;
                const sub = this.source.subscribe({
                    onSubscribe(s) { sourceSub = s; },
                    onNext(v) {
                        try {
                            const r = fn(v);
                            if (r != null) subscriber.onNext(r as NonNullable<R>);
                            else sourceSub.request(1); // replenish: null item skipped
                        } catch (e) {
                            subscriber.onError(e instanceof Error ? e : new Error(String(e)));
                        }
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
     * Flexible per-element transformation: handler may emit 0 or 1 item per input via sink.next(),
     * or signal error/complete. Emitting more than once per input is ignored (0-or-1 semantics,
     * matching reactor-core SynchronousSink). When 0 items are emitted, upstream is replenished
     * with request(1) to compensate for the skipped demand.
     */
    public handle<R>(handler: (value: T, sink: Sink<R>) => void): Flux<R> {
        return new Flux<R>({
            subscribe: (subscriber: Subscriber<R>): Subscription => {
                let sourceSub!: Subscription;
                let done = false;
                const sub = this.source.subscribe({
                    onSubscribe(s) { sourceSub = s; },
                    onNext(v) {
                        if (done) return;
                        let emitted = false;
                        const sinkWrapper: Sink<R> = {
                            next(r) {
                                if (!emitted && !done) { emitted = true; subscriber.onNext(r); }
                            },
                            error(e) { if (!done) { done = true; subscriber.onError(e); } },
                            complete() { if (!done) { done = true; subscriber.onComplete(); } }
                        };
                        try { handler(v, sinkWrapper); }
                        catch (e) { if (!done) { done = true; subscriber.onError(e instanceof Error ? e : new Error(String(e))); } }
                        // Replenish: item was skipped (0 emissions) — request one more from upstream
                        if (!done && !emitted) sourceSub.request(1);
                    },
                    onError(e) { if (!done) subscriber.onError(e); },
                    onComplete() { if (!done) subscriber.onComplete(); }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    /** Merge-style flatMap: subscribes to each inner publisher concurrently as values arrive. */
    public flatMap<R>(fn: (value: T) => Flux<R>): Flux<R>;
    public flatMap<R>(fn: (value: T) => Mono<R>): Flux<R>;
    public flatMap<R>(fn: (value: T) => Publisher<R>): Flux<R>;
    public flatMap<R>(fn: (value: T) => Publisher<R>): Flux<R> {
        return new Flux<R>({
            subscribe: (subscriber: Subscriber<R>): Subscription => {
                let outerSub: Subscription = { request() {}, unsubscribe() {} };
                const innerSubs = new Set<Subscription>();
                let cancelled = false;
                let terminated = false;
                let outerDone = false;
                let demand = 0;

                // Cancel all upstream sources and signal terminal to downstream exactly once.
                const terminate = (err?: Error) => {
                    if (terminated) return;
                    terminated = true;
                    outerSub.unsubscribe();
                    for (const s of innerSubs) s.unsubscribe();
                    innerSubs.clear();
                    if (err) subscriber.onError(err);
                    else subscriber.onComplete();
                };

                const operatorSub: Subscription = {
                    request(n: number) {
                        if (cancelled || terminated) return;
                        if (n <= 0) { terminate(new Error(`request must be > 0, but was ${n}`)); return; }
                        demand = Math.min(demand + n, Number.MAX_SAFE_INTEGER);
                        for (const s of innerSubs) s.request(n);
                    },
                    unsubscribe() {
                        cancelled = true;
                        outerSub.unsubscribe();
                        for (const s of innerSubs) s.unsubscribe();
                        innerSubs.clear();
                    }
                };

                this.source.subscribe({
                    onSubscribe(sourceSub: Subscription) {
                        outerSub = sourceSub;
                        subscriber.onSubscribe(operatorSub);
                    },
                    onNext(v: T) {
                        if (cancelled || terminated) return;
                        let inner: Publisher<R>;
                        try { inner = fn(v); }
                        catch (e) { terminate(e instanceof Error ? e : new Error(String(e))); return; }
                        const innerSub = inner.subscribe({
                            onSubscribe(_s) {},
                            onNext(r) { if (!cancelled && !terminated) subscriber.onNext(r); },
                            onError(e) { terminate(e); },
                            onComplete() {
                                innerSubs.delete(innerSub);
                                if (!terminated && outerDone && innerSubs.size === 0) terminate();
                            }
                        });
                        innerSubs.add(innerSub);
                        if (demand > 0) innerSub.request(demand);
                    },
                    onError(e) { terminate(e); },
                    onComplete() {
                        outerDone = true;
                        if (!terminated && innerSubs.size === 0) terminate();
                    }
                });
                outerSub.request(Number.MAX_SAFE_INTEGER);

                return operatorSub;
            }
        });
    }

    /** Sequential flatMap: subscribes to each inner publisher one at a time, in order. */
    public concatMap<R>(fn: (value: T) => Flux<R>): Flux<R>;
    public concatMap<R>(fn: (value: T) => Mono<R>): Flux<R>;
    public concatMap<R>(fn: (value: T) => Publisher<R>): Flux<R>;
    public concatMap<R>(fn: (value: T) => Publisher<R>): Flux<R> {
        return new Flux<R>({
            subscribe: (subscriber: Subscriber<R>): Subscription => {
                let outerSub!: Subscription;
                let innerSub: Subscription | null = null;
                let cancelled = false;
                let terminated = false;
                let outerDone = false;

                const terminate = (err?: Error) => {
                    if (terminated) return;
                    terminated = true;
                    outerSub?.unsubscribe();
                    innerSub?.unsubscribe();
                    innerSub = null;
                    if (err) subscriber.onError(err);
                    else subscriber.onComplete();
                };

                const operatorSub: Subscription = {
                    request(n: number) {
                        if (cancelled || terminated) return;
                        if (n <= 0) { terminate(new Error(`request must be > 0, but was ${n}`)); return; }
                        if (innerSub) innerSub.request(n);
                        else outerSub.request(1);
                    },
                    unsubscribe() { cancelled = true; outerSub?.unsubscribe(); innerSub?.unsubscribe(); }
                };

                this.source.subscribe({
                    onSubscribe(s: Subscription) {
                        outerSub = s;
                        subscriber.onSubscribe(operatorSub);
                    },
                    onNext(v: T) {
                        if (cancelled || terminated) return;
                        let inner: Publisher<R>;
                        try { inner = fn(v); }
                        catch (e) { terminate(e instanceof Error ? e : new Error(String(e))); return; }
                        inner.subscribe({
                            onSubscribe(s) { innerSub = s; s.request(Number.MAX_SAFE_INTEGER); },
                            onNext(r) { if (!cancelled && !terminated) subscriber.onNext(r); },
                            onError(e) { terminate(e); },
                            onComplete() {
                                if (cancelled || terminated) return;
                                innerSub = null;
                                if (outerDone) terminate();
                                else outerSub.request(1);
                            }
                        });
                    },
                    onError(e) { terminate(e); },
                    onComplete() {
                        outerDone = true;
                        if (innerSub === null) terminate();
                    }
                });

                return operatorSub;
            }
        });
    }

    /** Like flatMap but cancels the previous inner subscription when a new outer value arrives. */
    public switchMap<R>(fn: (value: T) => Flux<R>): Flux<R>;
    public switchMap<R>(fn: (value: T) => Mono<R>): Flux<R>;
    public switchMap<R>(fn: (value: T) => Publisher<R>): Flux<R>;
    public switchMap<R>(fn: (value: T) => Publisher<R>): Flux<R> {
        return new Flux<R>({
            subscribe: (subscriber: Subscriber<R>): Subscription => {
                let outerSub!: Subscription;
                let innerSub: Subscription | null = null;
                let cancelled = false;
                let terminated = false;
                let outerDone = false;
                let demand = 0;
                let generation = 0;

                const terminate = (err?: Error) => {
                    if (terminated) return;
                    terminated = true;
                    outerSub?.unsubscribe();
                    innerSub?.unsubscribe();
                    innerSub = null;
                    if (err) subscriber.onError(err);
                    else subscriber.onComplete();
                };

                const operatorSub: Subscription = {
                    request(n: number) {
                        if (cancelled || terminated) return;
                        if (n <= 0) { terminate(new Error(`request must be > 0, but was ${n}`)); return; }
                        demand = Math.min(demand + n, Number.MAX_SAFE_INTEGER);
                        if (innerSub) innerSub.request(n);
                    },
                    unsubscribe() { cancelled = true; outerSub?.unsubscribe(); innerSub?.unsubscribe(); }
                };

                this.source.subscribe({
                    onSubscribe(s: Subscription) {
                        outerSub = s;
                        subscriber.onSubscribe(operatorSub);
                    },
                    onNext(v: T) {
                        if (cancelled || terminated) return;
                        innerSub?.unsubscribe();
                        innerSub = null;
                        const myGen = ++generation;
                        let inner: Publisher<R>;
                        try { inner = fn(v); }
                        catch (e) { terminate(e instanceof Error ? e : new Error(String(e))); return; }
                        inner.subscribe({
                            onSubscribe(s) {
                                if (generation !== myGen || cancelled || terminated) { s.unsubscribe(); return; }
                                innerSub = s;
                                if (demand > 0) s.request(demand);
                            },
                            onNext(r) { if (generation === myGen && !cancelled && !terminated) subscriber.onNext(r); },
                            onError(e) { if (generation === myGen) terminate(e); },
                            onComplete() {
                                if (generation !== myGen || cancelled || terminated) return;
                                innerSub = null;
                                if (outerDone) terminate();
                            }
                        });
                    },
                    onError(e) { terminate(e); },
                    onComplete() {
                        outerDone = true;
                        if (innerSub === null) terminate();
                    }
                });
                outerSub.request(Number.MAX_SAFE_INTEGER);

                return operatorSub;
            }
        });
    }

    public filter(predicate: (value: T) => boolean): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let sourceSub!: Subscription;
                const sub = this.source.subscribe({
                    onSubscribe(s) { sourceSub = s; },
                    onNext(v) {
                        try {
                            if (predicate(v)) subscriber.onNext(v);
                            else sourceSub.request(1); // replenish: item skipped
                        } catch (e) { subscriber.onError(e instanceof Error ? e : new Error(String(e))); }
                    },
                    onError(e) { subscriber.onError(e); },
                    onComplete() { subscriber.onComplete(); }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    /** Async filter: for each item, subscribe to the predicate publisher; forward item only if it emits true. */
    public filterWhen(predicate: (value: T) => Publisher<boolean>): Flux<T> {
        return this.concatMap(v =>
            Flux.from(predicate(v))
                .filter(pass => pass)
                .map(() => v)
        );
    }

    public cast<R>(): Flux<R> { return new Flux<R>(this.source as unknown as Publisher<R>); }

    /** Emit only the first n items, then cancel and complete. */
    public take(n: number): Flux<T> {
        if (n <= 0) return Flux.empty<T>();
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let sourceSub!: Subscription;
                let count = 0;
                let done = false;

                const operatorSub: Subscription = {
                    request(r: number) { if (!done) sourceSub?.request(r); },
                    unsubscribe() { done = true; sourceSub?.unsubscribe(); }
                };

                this.source.subscribe({
                    onSubscribe(s: Subscription) {
                        sourceSub = s;
                        subscriber.onSubscribe(operatorSub);
                    },
                    onNext(v: T) {
                        if (done) return;
                        subscriber.onNext(v);
                        if (++count >= n) {
                            done = true;
                            sourceSub.unsubscribe();
                            subscriber.onComplete();
                        }
                    },
                    onError(e: Error) { if (!done) subscriber.onError(e); },
                    onComplete() { if (!done) subscriber.onComplete(); }
                });

                return operatorSub;
            }
        });
    }

    /** Forward items while predicate returns true; complete as soon as it returns false. */
    public takeWhile(predicate: (value: T) => boolean): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let sourceSub!: Subscription;
                let done = false;

                const operatorSub: Subscription = {
                    request(n: number) { if (!done) sourceSub?.request(n); },
                    unsubscribe() { done = true; sourceSub?.unsubscribe(); }
                };

                this.source.subscribe({
                    onSubscribe(s: Subscription) {
                        sourceSub = s;
                        subscriber.onSubscribe(operatorSub);
                    },
                    onNext(v: T) {
                        if (done) return;
                        try {
                            if (predicate(v)) {
                                subscriber.onNext(v);
                            } else {
                                done = true;
                                sourceSub.unsubscribe();
                                subscriber.onComplete();
                            }
                        } catch (e) {
                            done = true;
                            subscriber.onError(e instanceof Error ? e : new Error(String(e)));
                        }
                    },
                    onError(e: Error) { if (!done) subscriber.onError(e); },
                    onComplete() { if (!done) subscriber.onComplete(); }
                });

                return operatorSub;
            }
        });
    }

    /** Forward items until the trigger Publisher emits, then cancel and complete. */
    public takeUntilOther(trigger: Publisher<unknown>): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let sourceSub!: Subscription;
                let triggerSub: Subscription;
                let done = false;

                const operatorSub: Subscription = {
                    request(n: number) { if (!done) sourceSub?.request(n); },
                    unsubscribe() { done = true; sourceSub?.unsubscribe(); triggerSub?.unsubscribe(); }
                };

                triggerSub = trigger.subscribe({
                    onSubscribe(s) { triggerSub = s; s.request(1); },
                    onNext(_v) {
                        if (done) return;
                        done = true;
                        sourceSub?.unsubscribe();
                        subscriber.onComplete();
                    },
                    onError(e) { if (!done) { done = true; subscriber.onError(e); } },
                    onComplete() {}
                });

                this.source.subscribe({
                    onSubscribe(s: Subscription) {
                        sourceSub = s;
                        subscriber.onSubscribe(operatorSub);
                    },
                    onNext(v: T) { if (!done) subscriber.onNext(v); },
                    onError(e: Error) { if (!done) subscriber.onError(e); },
                    onComplete() { if (!done) { done = true; triggerSub?.unsubscribe(); subscriber.onComplete(); } }
                });

                return operatorSub;
            }
        });
    }

    /** Emit defaultValue if source completes without emitting any item. */
    public defaultIfEmpty(value: T): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let hasValue = false;
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) { hasValue = true; subscriber.onNext(v); },
                    onError(e) { subscriber.onError(e); },
                    onComplete() {
                        if (!hasValue) subscriber.onNext(value);
                        subscriber.onComplete();
                    }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    /** Resubscribe to source on error, up to maxRetries times (default: unlimited). */
    public retry(maxRetries: number = Number.MAX_SAFE_INTEGER): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let currentSub: Subscription = { request() {}, unsubscribe() {} };
                let demand = 0;
                let cancelled = false;
                let retries = 0;
                // Generation counter prevents stale onSubscribe callbacks (from earlier failed
                // attempts) from overwriting currentSub after recursive attempt() calls return.
                let generation = 0;

                const operatorSub: Subscription = {
                    request(n: number) {
                        if (cancelled) return;
                        if (n <= 0) { subscriber.onError(new Error(`request must be > 0, but was ${n}`)); return; }
                        demand = Math.min(demand + n, Number.MAX_SAFE_INTEGER);
                        currentSub.request(n);
                    },
                    unsubscribe() { cancelled = true; currentSub.unsubscribe(); }
                };

                const attempt = () => {
                    const myGen = ++generation;
                    this.source.subscribe({
                        onSubscribe(s: Subscription) {
                            // Only accept if this is still the active attempt
                            if (myGen !== generation) { s.unsubscribe(); return; }
                            currentSub = s;
                            if (demand > 0) s.request(demand);
                        },
                        onNext(v: T) { if (!cancelled && myGen === generation) subscriber.onNext(v); },
                        onError(e: Error) {
                            if (cancelled || myGen !== generation) return;
                            if (retries < maxRetries) { retries++; attempt(); }
                            else subscriber.onError(e);
                        },
                        onComplete() { if (!cancelled && myGen === generation) subscriber.onComplete(); }
                    });
                };

                subscriber.onSubscribe(operatorSub);
                attempt();

                return operatorSub;
            }
        });
    }

    // ──────────────────── Scan / Zip ────────────────────────────────

    /** Emit running accumulation starting with the first item as initial accumulator. */
    public scan(reducer: (acc: T, next: T) => T): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let acc: T | undefined;
                let hasAcc = false;
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) {
                        acc = hasAcc ? reducer(acc as T, v) : v;
                        hasAcc = true;
                        subscriber.onNext(acc);
                    },
                    onError(e) { subscriber.onError(e); },
                    onComplete() { subscriber.onComplete(); }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    /** Emit running accumulation with an explicit seed from the factory. */
    public scanWith<A>(seedFactory: () => A, reducer: (acc: A, next: T) => A): Flux<A> {
        return new Flux<A>({
            subscribe: (subscriber: Subscriber<A>): Subscription => {
                let acc = seedFactory();
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) { acc = reducer(acc, v); subscriber.onNext(acc); },
                    onError(e) { subscriber.onError(e); },
                    onComplete() { subscriber.onComplete(); }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    /** Pair-wise combine this Flux with another Publisher using the combiner function. */
    public zipWith<R, V>(other: Publisher<R>, combiner: (a: T, b: R) => V): Flux<V> {
        return new Flux<V>({
            subscribe: (subscriber: Subscriber<V>): Subscription => {
                const leftQueue: T[] = [];
                const rightQueue: R[] = [];
                let leftDone = false;
                let rightDone = false;
                let cancelled = false;
                let terminated = false;

                let leftSub: Subscription = { request() {}, unsubscribe() {} };
                let rightSub: Subscription = { request() {}, unsubscribe() {} };

                const terminate = (err?: Error) => {
                    if (terminated) return;
                    terminated = true;
                    leftSub.unsubscribe();
                    rightSub.unsubscribe();
                    if (err) subscriber.onError(err);
                    else subscriber.onComplete();
                };

                const operatorSub: Subscription = {
                    request(n: number) {
                        if (cancelled || terminated) return;
                        if (n <= 0) { terminate(new Error(`request must be > 0, but was ${n}`)); return; }
                        leftSub.request(n);
                        rightSub.request(n);
                    },
                    unsubscribe() { cancelled = true; leftSub.unsubscribe(); rightSub.unsubscribe(); }
                };

                const tryEmit = () => {
                    while (leftQueue.length > 0 && rightQueue.length > 0) {
                        if (cancelled || terminated) return;
                        const l = leftQueue.shift()!;
                        const r = rightQueue.shift()!;
                        try { subscriber.onNext(combiner(l, r)); }
                        catch (e) { terminate(e instanceof Error ? e : new Error(String(e))); return; }
                    }
                    if ((leftDone && leftQueue.length === 0) || (rightDone && rightQueue.length === 0)) {
                        terminate();
                    }
                };

                this.source.subscribe({
                    onSubscribe(s: Subscription) {
                        leftSub = s;
                        subscriber.onSubscribe(operatorSub);
                    },
                    onNext(v: T) { if (!cancelled && !terminated) { leftQueue.push(v); tryEmit(); } },
                    onError(e: Error) { terminate(e); },
                    onComplete() { if (!cancelled && !terminated) { leftDone = true; tryEmit(); } }
                });

                rightSub = other.subscribe({
                    onSubscribe(s: Subscription) { rightSub = s; },
                    onNext(v: R) { if (!cancelled && !terminated) { rightQueue.push(v); tryEmit(); } },
                    onError(e: Error) { terminate(e); },
                    onComplete() { if (!cancelled && !terminated) { rightDone = true; tryEmit(); } }
                });

                return operatorSub;
            }
        });
    }

    // ──────────────────── Side effects ───────────────────────────────

    public doOnComplete(fn: () => void): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) { subscriber.onNext(v); },
                    onError(e) { subscriber.onError(e); },
                    onComplete() { try { fn(); } catch (_) {} subscriber.onComplete(); }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    /** Side effect run on either onComplete or onError. */
    public doOnTerminate(fn: () => void): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) { subscriber.onNext(v); },
                    onError(e) { try { fn(); } catch (_) {} subscriber.onError(e); },
                    onComplete() { try { fn(); } catch (_) {} subscriber.onComplete(); }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    /** Side effect run on cancellation (unsubscribe). */
    public doOnCancel(fn: () => void): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const inner = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) { subscriber.onNext(v); },
                    onError(e) { subscriber.onError(e); },
                    onComplete() { subscriber.onComplete(); }
                });
                const sub: Subscription = {
                    request(n) { inner.request(n); },
                    unsubscribe() { try { fn(); } catch (_) {} inner.unsubscribe(); }
                };
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    public doFinally(fn: () => void): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const inner = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) { subscriber.onNext(v); },
                    onError(e) { try { fn(); } catch (_) {} subscriber.onError(e); },
                    onComplete() { try { fn(); } catch (_) {} subscriber.onComplete(); }
                });
                const sub: Subscription = {
                    request(n) { inner.request(n); },
                    unsubscribe() { try { fn(); } catch (_) {} inner.unsubscribe(); }
                };
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    // ──────────────────── Scheduling ────────────────────────────────

    public pipe<R>(
        producer: (onNext: (value: R) => void, onError: (error: Error) => void, onComplete: () => void) => void,
        onRequest: (request: number) => void,
        onUnsubscribe: () => void
    ): Flux<R> {
        return new Flux<R>({
            subscribe: (subscriber: Subscriber<R>): Subscription => {
                producer(v => subscriber.onNext(v), e => subscriber.onError(e), () => subscriber.onComplete());
                const sub = { request: onRequest, unsubscribe: onUnsubscribe };
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    // ──────────────────── Flux-specific operators ────────────────────────

    /** Take first item as a Mono (empty if source is empty). */
    public first(): Mono<T> {
        return Mono.generate<T>(sink => {
            const sub = this.source.subscribe({
                onSubscribe(_s) {},
                onNext(v) { sub.unsubscribe(); sink.next(v); },
                onError(e) { sink.error(e); },
                onComplete() { sink.complete(); }
            });
            sub.request(1);
        });
    }

    /** Take the last item as a Mono (empty if source is empty). */
    public last(): Mono<T> {
        return Mono.generate<T>(sink => {
            let last: T | undefined;
            let hasValue = false;
            const sub = this.source.subscribe({
                onSubscribe(_s) {},
                onNext(v) { last = v; hasValue = true; },
                onError(e) { sink.error(e); },
                onComplete() {
                    if (hasValue) sink.next(last as T);
                    else sink.complete();
                }
            });
            sub.request(Number.MAX_SAFE_INTEGER);
        });
    }

    public count(): Mono<number> {
        return Mono.generate<number>(sink => {
            let n = 0;
            const sub = this.source.subscribe({
                onSubscribe(_s) {},
                onNext(_v) { n++; },
                onError(e) { sink.error(e); },
                onComplete() { sink.next(n); }
            });
            sub.request(Number.MAX_SAFE_INTEGER);
        });
    }

    public hasElements(): Mono<boolean> {
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

    /** Emit true if any item matches the predicate. Short-circuits on first match. */
    public any(predicate: (value: T) => boolean): Mono<boolean> {
        return Mono.generate<boolean>(sink => {
            const sub = this.source.subscribe({
                onSubscribe(_s) {},
                onNext(v) {
                    try {
                        if (predicate(v)) { sub.unsubscribe(); sink.next(true); }
                    } catch (e) { sink.error(e instanceof Error ? e : new Error(String(e))); }
                },
                onError(e) { sink.error(e); },
                onComplete() { sink.next(false); }
            });
            sub.request(Number.MAX_SAFE_INTEGER);
        });
    }

    /** Emit true if all items match the predicate. Short-circuits on first mismatch. */
    public all(predicate: (value: T) => boolean): Mono<boolean> {
        return Mono.generate<boolean>(sink => {
            const sub = this.source.subscribe({
                onSubscribe(_s) {},
                onNext(v) {
                    try {
                        if (!predicate(v)) { sub.unsubscribe(); sink.next(false); }
                    } catch (e) { sink.error(e instanceof Error ? e : new Error(String(e))); }
                },
                onError(e) { sink.error(e); },
                onComplete() { sink.next(true); }
            });
            sub.request(Number.MAX_SAFE_INTEGER);
        });
    }

    /** Emit true if no items match the predicate. */
    public none(predicate: (value: T) => boolean): Mono<boolean> {
        return this.any(predicate).map(v => !v);
    }

    /** Get the item at the given zero-based index. Completes empty if index is out of range (or emits defaultValue). */
    public elementAt(index: number, defaultValue?: T): Mono<T> {
        return Mono.generate<T>(sink => {
            let i = 0;
            const sub = this.source.subscribe({
                onSubscribe(_s) {},
                onNext(v) {
                    if (i++ === index) { sub.unsubscribe(); sink.next(v); }
                },
                onError(e) { sink.error(e); },
                onComplete() {
                    if (defaultValue !== undefined) sink.next(defaultValue);
                    else sink.complete();
                }
            });
            sub.request(Number.MAX_SAFE_INTEGER);
        });
    }

    /** Collect all items into an array and emit as a Mono. */
    public collect(_force: boolean = false): Mono<T[]> {
        return Mono.generate<T[]>(sink => {
            const items: T[] = [];
            const sub = this.source.subscribe({
                onSubscribe(_s) {},
                onNext(v) { items.push(v); },
                onError(e) { sink.error(e); },
                onComplete() { sink.next(items); }
            });
            sub.request(Number.MAX_SAFE_INTEGER);
        });
    }

    /** Alias for collect(). */
    public collectList(): Mono<T[]> {
        return this.collect();
    }

    /** Collect all items and emit them sorted. */
    public sort(comparator?: (a: T, b: T) => number): Flux<T> {
        return Flux.defer(() =>
            Flux.from(this.collect().map(arr => {
                arr.sort(comparator);
                return arr;
            }).flatMapMany(arr => Flux.fromIterable(arr)))
        );
    }

    /** Collect items into arrays of maxSize, emitting each full batch (and the last partial batch on complete). */
    public buffer(maxSize: number): Flux<T[]> {
        return new Flux<T[]>({
            subscribe: (subscriber: Subscriber<T[]>): Subscription => {
                let batch: T[] = [];
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) {
                        batch.push(v);
                        if (batch.length >= maxSize) {
                            subscriber.onNext(batch);
                            batch = [];
                        }
                    },
                    onError(e) { subscriber.onError(e); },
                    onComplete() {
                        if (batch.length > 0) subscriber.onNext(batch);
                        subscriber.onComplete();
                    }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    /** Cache all emitted items and replay them to each new subscriber. Connects to source on first subscription. */
    public cache(): Flux<T> {
        const sink = new ReplayAllSink<T>();
        let connected = false;

        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                if (!connected) {
                    connected = true;
                    this.source.subscribe({
                        onSubscribe(s) { s.request(Number.MAX_SAFE_INTEGER); },
                        onNext(v) { sink.next(v); },
                        onError(e) { sink.error(e); },
                        onComplete() { sink.complete(); }
                    });
                }
                return sink.subscribe(subscriber);
            }
        });
    }

    public indexed(): Flux<[number, T]> {
        return new Flux<[number, T]>({
            subscribe: (subscriber: Subscriber<[number, T]>): Subscription => {
                let index = 0;
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) { subscriber.onNext([index++, v]); },
                    onError(e) { subscriber.onError(e); },
                    onComplete() { subscriber.onComplete(); }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    public skip(n: number): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let skipped = 0;
                let sourceSub!: Subscription;
                const sub = this.source.subscribe({
                    onSubscribe(s) { sourceSub = s; },
                    onNext(v) {
                        if (skipped++ < n) sourceSub.request(1); // replenish: item skipped
                        else subscriber.onNext(v);
                    },
                    onError(e) { subscriber.onError(e); },
                    onComplete() { subscriber.onComplete(); }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    public skipWhile(predicate: (value: T) => boolean): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let skipping = true;
                let sourceSub!: Subscription;
                const sub = this.source.subscribe({
                    onSubscribe(s) { sourceSub = s; },
                    onNext(v) {
                        if (skipping && predicate(v)) { sourceSub.request(1); return; }
                        skipping = false;
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

    public skipUntil(other: Publisher<unknown>): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let gating = true;
                let primarySub: Subscription;
                const triggerSub = other.subscribe({
                    onSubscribe(_s) {},
                    onNext(_v) { gating = false; triggerSub.unsubscribe(); },
                    onError(e) { subscriber.onError(e); },
                    onComplete() {}
                });
                triggerSub.request(1);
                primarySub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) { if (!gating) subscriber.onNext(v); else primarySub.request(1); },
                    onError(e) { subscriber.onError(e); },
                    onComplete() { subscriber.onComplete(); }
                });
                subscriber.onSubscribe({
                    request(n) { primarySub.request(n); },
                    unsubscribe() { primarySub.unsubscribe(); triggerSub.unsubscribe(); }
                });
                return { request(n) { primarySub.request(n); }, unsubscribe() { primarySub.unsubscribe(); triggerSub.unsubscribe(); } };
            }
        });
    }

    public distinct(): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const seen = new Set<T>();
                let sourceSub!: Subscription;
                const sub = this.source.subscribe({
                    onSubscribe(s) { sourceSub = s; },
                    onNext(v) { if (!seen.has(v)) { seen.add(v); subscriber.onNext(v); } else sourceSub.request(1); },
                    onError(e) { subscriber.onError(e); },
                    onComplete() { subscriber.onComplete(); }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    public distinctUntilChanged(comparator: (a: T, b: T) => boolean = (a, b) => a !== b): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let prev: T | undefined;
                let first = true;
                let sourceSub!: Subscription;
                const sub = this.source.subscribe({
                    onSubscribe(s) { sourceSub = s; },
                    onNext(v) {
                        if (first || comparator(prev as T, v)) {
                            first = false;
                            prev = v;
                            subscriber.onNext(v);
                        } else sourceSub.request(1);
                    },
                    onError(e) { subscriber.onError(e); },
                    onComplete() { subscriber.onComplete(); }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    public delayElements(ms: number): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) { Schedulers.delay(ms).schedule(() => subscriber.onNext(v)); },
                    onError(e) { subscriber.onError(e); },
                    onComplete() { subscriber.onComplete(); }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    public concatWith(other: Publisher<T>): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let primarySub: Subscription = { request() {}, unsubscribe() {} };
                let otherSub: Subscription | null = null;
                let cancelled = false;
                let demand = 0;

                const operatorSub: Subscription = {
                    request(n) {
                        if (cancelled) return;
                        if (n <= 0) { subscriber.onError(new Error(`request must be > 0, but was ${n}`)); return; }
                        demand = Math.min(demand + n, Number.MAX_SAFE_INTEGER);
                        if (otherSub) otherSub.request(n); else primarySub.request(n);
                    },
                    unsubscribe() { cancelled = true; primarySub.unsubscribe(); otherSub?.unsubscribe(); }
                };

                this.source.subscribe({
                    onSubscribe(sourceSub: Subscription) {
                        primarySub = sourceSub;
                        subscriber.onSubscribe(operatorSub);
                    },
                    onNext(v) { subscriber.onNext(v); },
                    onError(e) { subscriber.onError(e); },
                    onComplete() {
                        if (cancelled) return;
                        otherSub = other.subscribe({
                            onSubscribe(otherSourceSub: Subscription) {
                                otherSub = otherSourceSub;
                                if (demand > 0) otherSub.request(demand);
                            },
                            onNext(v) { subscriber.onNext(v); },
                            onError(e) { subscriber.onError(e); },
                            onComplete() { subscriber.onComplete(); }
                        });
                    }
                });

                return operatorSub;
            }
        });
    }

    public mergeWith(other: Publisher<T>): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let doneCount = 0;
                let cancelled = false;
                let terminated = false;

                let primarySub: Subscription = { request() {}, unsubscribe() {} };
                let otherSubRef: Subscription = { request() {}, unsubscribe() {} };

                const terminate = (err?: Error) => {
                    if (terminated) return;
                    terminated = true;
                    primarySub.unsubscribe();
                    otherSubRef.unsubscribe();
                    if (err) subscriber.onError(err);
                    else subscriber.onComplete();
                };

                const done = () => { if (!terminated && ++doneCount === 2) terminate(); };

                const operatorSub: Subscription = {
                    request(n) {
                        if (cancelled || terminated) return;
                        if (n <= 0) { terminate(new Error(`request must be > 0, but was ${n}`)); return; }
                        primarySub.request(n);
                        otherSubRef.request(n);
                    },
                    unsubscribe() { cancelled = true; primarySub.unsubscribe(); otherSubRef.unsubscribe(); }
                };

                this.source.subscribe({
                    onSubscribe(sourceSub: Subscription) {
                        primarySub = sourceSub;
                        subscriber.onSubscribe(operatorSub);
                    },
                    onNext(v) { if (!cancelled && !terminated) subscriber.onNext(v); },
                    onError(e) { terminate(e); },
                    onComplete() { done(); }
                });

                otherSubRef = other.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) { if (!cancelled && !terminated) subscriber.onNext(v); },
                    onError(e) { terminate(e); },
                    onComplete() { done(); }
                });

                return operatorSub;
            }
        });
    }

    public reduce(reducer: (acc: T, next: T) => T): Mono<T> {
        return Mono.generate<T>(sink => {
            let acc: T | undefined;
            let hasValue = false;
            const sub = this.source.subscribe({
                onSubscribe(_s) {},
                onNext(v) { acc = hasValue ? reducer(acc as T, v) : v; hasValue = true; },
                onError(e) { sink.error(e); },
                onComplete() {
                    if (hasValue) sink.next(acc as T);
                    else sink.complete();
                }
            });
            sub.request(Number.MAX_SAFE_INTEGER);
        });
    }

    public reduceWith<A>(seedFactory: () => A, reducer: (acc: A, next: T) => A): Mono<A> {
        return Mono.generate<A>(sink => {
            let acc = seedFactory();
            const sub = this.source.subscribe({
                onSubscribe(_s) {},
                onNext(v) { acc = reducer(acc, v); },
                onError(e) { sink.error(e); },
                onComplete() { sink.next(acc); }
            });
            sub.request(Number.MAX_SAFE_INTEGER);
        });
    }

    public then(): Mono<void> {
        return Mono.generate<void>(sink => {
            const sub = this.source.subscribe({
                onSubscribe(_s) {},
                onNext(_v) {},
                onError(e) { sink.error(e); },
                onComplete() { sink.next(undefined); }
            });
            sub.request(Number.MAX_SAFE_INTEGER);
        });
    }

    public thenEmpty(other: Publisher<unknown>): Mono<void> {
        return this.then().flatMap(() => Mono.from<void>({
            subscribe(subscriber: Subscriber<void>): Subscription {
                const sub = other.subscribe({
                    onSubscribe(_s) {},
                    onNext(_v) {},
                    onError(e) { subscriber.onError(e); },
                    onComplete() { subscriber.onNext(undefined); subscriber.onComplete(); }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        }));
    }
}
