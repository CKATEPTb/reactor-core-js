import {PipePublisher} from "@/publishers/PipePublisher";
import {Publisher} from "@/publishers/Publisher";
import {Sink} from "@/sinks/Sink";
import {Mono} from "@/publishers/Mono";
import {Subscriber} from "@/subscriptions/Subscriber";
import {Subscription} from "@/subscriptions/Subscription";
import ReplayAllSink from "@/sinks/ReplayAllSink";
import {AbstractPipePublisher} from "@/publishers/internal/AbstractPipePublisher";
import {Schedulers} from "@/schedulers";
import {Signal} from "@/publishers/Signal";
import {FluxSink} from "@/publishers/FluxSink";

/**
 * A cold publisher of 0 to N items with full backpressure support.
 *
 * Items flow only when the downstream {@link Subscription} issues `request(n)`.
 * The convenience `subscribe(onNext, onError, onComplete)` overloads
 * automatically request `Number.MAX_SAFE_INTEGER` so items start arriving
 * without extra ceremony.
 *
 * Each `subscribe()` call creates an **independent** run of the pipeline
 * ("cold" semantics). Use {@link Flux.cache} or {@link Sinks} to share a
 * single run across multiple subscribers.
 *
 * @typeParam T - The type of items emitted by this Flux.
 *
 * @example
 * ```typescript
 * Flux.just(1, 2, 3)
 *   .map(n => n * 2)
 *   .filter(n => n > 2)
 *   .subscribe(v => console.log(v));
 * // 4  6
 * ```
 */
export class Flux<T> extends AbstractPipePublisher<T, Flux<T>> implements PipePublisher<T> {

    protected constructor(source: Publisher<T>) { super(source); }

    protected defaultDemand(): number { return Number.MAX_SAFE_INTEGER; }

    protected wrapSource(source: Publisher<T>): Flux<T> {
        return new Flux<T>(source);
    }

    // ─────────────────────────── Static factories ────────────────────────────

    /**
     * Wraps an existing {@link Publisher} as a `Flux`.
     *
     * @param publisher - Any `Publisher<T>` to adapt.
     * @returns A `Flux<T>` backed by the given publisher.
     */
    public static from<T>(publisher: Publisher<T>): Flux<T> {
        return new Flux<T>(publisher);
    }

    /**
     * Creates a `Flux` from a generator function that pushes values imperatively.
     *
     * The generator receives a {@link Sink} and may call `sink.next(v)` any number
     * of times, then `sink.complete()` or `sink.error(e)`.  Delivery is
     * **backpressure-aware**: items pushed when demand is zero are buffered and
     * delivered as downstream issues `request(n)`.
     *
     * Rule 1.3 is respected — `onSubscribe` is delivered to the subscriber
     * **before** the generator runs.
     *
     * @param generator - Function that pushes items via the provided `Sink<T>`.
     * @returns A cold `Flux<T>`.
     *
     * @example
     * ```typescript
     * Flux.generate<number>(sink => {
     *   sink.next(1);
     *   sink.next(2);
     *   sink.complete();
     * }).subscribe(v => console.log(v)); // 1  2
     * ```
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

    /**
     * Creates a `Flux` that emits every element of an `Iterable` in order, then completes.
     *
     * @param iterable - Any `Iterable<T>` (Array, Set, Map, generator, etc.).
     * @returns A cold `Flux<T>`.
     *
     * @example
     * ```typescript
     * Flux.fromIterable(['a', 'b', 'c']).subscribe(v => console.log(v));
     * // a  b  c
     * ```
     */
    public static fromIterable<T>(iterable: Iterable<T>): Flux<T> {
        return Flux.generate<T>(sink => {
            for (const item of iterable) sink.next(item);
            sink.complete();
        });
    }

    /**
     * Creates a `Flux` that emits `count` sequential integers starting at `start`.
     *
     * @param start - First integer to emit.
     * @param count - Number of integers to emit.
     * @returns A cold `Flux<number>` emitting `[start, start + count)`.
     *
     * @example
     * ```typescript
     * Flux.range(0, 5).subscribe(v => console.log(v)); // 0 1 2 3 4
     * ```
     */
    public static range(start: number, count: number): Flux<number> {
        return Flux.generate<number>(sink => {
            for (let i = 0; i < count; i++) sink.next(start + i);
            sink.complete();
        });
    }

    /**
     * Creates a `Flux` that completes immediately without emitting any items.
     *
     * @returns An empty, completed `Flux<T>`.
     */
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

    /**
     * Lazily creates a new `Flux` per subscription by calling `factory`.
     *
     * Useful when the source depends on mutable state that should be captured
     * at subscribe time rather than at assembly time.
     *
     * @param factory - Called once per subscription to produce the actual `Flux<T>`.
     * @returns A lazy `Flux<T>`.
     */
    public static defer<T>(factory: () => Flux<T>): Flux<T> {
        return new Flux<T>({
            subscribe(subscriber: Subscriber<T>): Subscription {
                return factory().subscribe(subscriber);
            }
        });
    }

    /**
     * Creates a `Flux` that emits the provided items in order, then completes.
     *
     * @param items - Zero or more items to emit.
     * @returns A cold `Flux<T>`.
     *
     * @example
     * ```typescript
     * Flux.just(1, 2, 3).subscribe(v => console.log(v)); // 1  2  3
     * ```
     */
    public static just<T>(...items: T[]): Flux<T> {
        return Flux.fromIterable(items);
    }

    /**
     * Creates a `Flux` that signals `onError` immediately upon subscription.
     *
     * @param error - The error to signal.
     * @returns An errored `Flux<T>`.
     */
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

    /**
     * Creates a `Flux` that never emits any signal — no items, no error, no completion.
     * Useful as a placeholder or to represent an infinite, empty stream.
     *
     * @returns A `Flux<T>` that stays subscribed forever.
     */
    public static never<T = never>(): Flux<T> {
        return new Flux<T>({
            subscribe(subscriber: Subscriber<T>): Subscription {
                const sub = { request() {}, unsubscribe() {} };
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    /**
     * Creates a `Flux` from a generator function that **pushes** values imperatively
     * via a {@link FluxSink}.
     *
     * Unlike {@link Flux.generate} (which is request-pull), `create` is designed for
     * **push-based** sources (EventEmitter, WebSocket, DOM events, Node.js streams).
     * Items pushed when downstream demand is zero are **buffered** and delivered as demand
     * becomes available.
     *
     * Register `onRequest`, `onCancel`, and `onDispose` callbacks on the sink to respond
     * to downstream lifecycle events.
     *
     * @param emitter - Called once per subscription; receives a `FluxSink<T>` for pushing values.
     * @returns A cold `Flux<T>`.
     *
     * @example
     * ```typescript
     * const emitter = new EventEmitter();
     * const flux = Flux.create<string>(sink => {
     *   emitter.on('data', v => sink.next(v));
     *   emitter.on('end',  () => sink.complete());
     *   sink.onCancel(() => emitter.removeAllListeners());
     * });
     * ```
     */
    public static create<T>(emitter: (sink: FluxSink<T>) => void): Flux<T> {
        return new Flux<T>({
            subscribe(subscriber: Subscriber<T>): Subscription {
                const buffer: T[] = [];
                let demand = 0;
                let cancelled = false;
                let terminated = false;
                let terminalError: Error | null = null;
                let draining = false;

                const requestCbs: Array<(n: number) => void> = [];
                const cancelCbs: Array<() => void> = [];
                const disposeCbs: Array<() => void> = [];

                const fireDispose = () => { for (const fn of disposeCbs) try { fn(); } catch (_) {} };

                const drain = () => {
                    if (draining || cancelled) return;
                    draining = true;
                    try {
                        while (demand > 0 && buffer.length > 0 && !cancelled) {
                            demand--;
                            subscriber.onNext(buffer.shift()!);
                        }
                        if (terminated && buffer.length === 0 && !cancelled) {
                            if (terminalError) subscriber.onError(terminalError);
                            else subscriber.onComplete();
                        }
                    } finally { draining = false; }
                };

                const sink: FluxSink<T> = {
                    next(value: T) {
                        if (cancelled || terminated) return;
                        buffer.push(value);
                        drain();
                    },
                    error(error: Error) {
                        if (cancelled || terminated) return;
                        terminated = true;
                        terminalError = error;
                        fireDispose();
                        drain();
                    },
                    complete() {
                        if (cancelled || terminated) return;
                        terminated = true;
                        fireDispose();
                        drain();
                    },
                    get requested() { return demand; },
                    onRequest(fn) { requestCbs.push(fn); return sink; },
                    onCancel(fn) { cancelCbs.push(fn); return sink; },
                    onDispose(fn) { disposeCbs.push(fn); return sink; }
                };

                const sub: Subscription = {
                    request(n: number) {
                        if (cancelled || (terminated && buffer.length === 0)) return;
                        if (n <= 0) { subscriber.onError(new Error(`request must be > 0, but was ${n}`)); return; }
                        demand = Math.min(demand + n, Number.MAX_SAFE_INTEGER);
                        for (const fn of requestCbs) try { fn(n); } catch (_) {}
                        drain();
                    },
                    unsubscribe() {
                        if (cancelled) return;
                        cancelled = true;
                        buffer.length = 0;
                        for (const fn of cancelCbs) try { fn(); } catch (_) {}
                        fireDispose();
                    }
                };

                // Rule 1.3: onSubscribe is the first signal delivered to the subscriber.
                subscriber.onSubscribe(sub);
                try { emitter(sink); }
                catch (e) { if (!terminated && !cancelled) sink.error(e instanceof Error ? e : new Error(String(e))); }
                return sub;
            }
        });
    }

    /**
     * Creates a `Flux` that acquires a resource, uses it to build a source publisher,
     * and guarantees the resource is cleaned up when the stream terminates or is cancelled.
     *
     * The cleanup runs after `onComplete`, `onError`, or `unsubscribe()` — whichever
     * happens first.
     *
     * @param resourceSupplier - Called once per subscription to acquire the resource.
     * @param sourceFactory - Builds the source publisher from the resource.
     * @param cleanup - Called with the resource after the stream terminates.
     * @returns A `Flux<T>` with guaranteed resource cleanup.
     *
     * @example
     * ```typescript
     * Flux.using(
     *   () => openFile('data.txt'),
     *   file => Flux.fromIterable(file.readLines()),
     *   file => file.close()
     * ).subscribe(line => console.log(line));
     * ```
     */
    public static using<T, R>(
        resourceSupplier: () => R,
        sourceFactory: (resource: R) => Publisher<T>,
        cleanup: (resource: R) => void
    ): Flux<T> {
        return Flux.defer(() => {
            const resource = resourceSupplier();
            return Flux.from(sourceFactory(resource))
                .doFinally(() => { try { cleanup(resource); } catch (_) {} });
        });
    }

    /**
     * Merges N publishers by subscribing to all of them concurrently and interleaving
     * their items as they arrive.
     *
     * The stream completes when **all** sources complete.
     * Any error from any source terminates the merged stream immediately.
     *
     * @param sources - Publishers to merge.
     * @returns A `Flux<T>` that emits items from all sources concurrently.
     *
     * @example
     * ```typescript
     * Flux.merge(Flux.just(1, 2), Flux.just(3, 4)).subscribe(v => console.log(v));
     * // 1 2 3 4 (order may vary for async sources)
     * ```
     */
    public static merge<T>(...sources: Publisher<T>[]): Flux<T> {
        if (sources.length === 0) return Flux.empty<T>();
        if (sources.length === 1) return Flux.from(sources[0]);
        return new Flux<T>({
            subscribe(subscriber: Subscriber<T>): Subscription {
                const n = sources.length;
                let doneCount = 0;
                let cancelled = false;
                let terminated = false;
                // Populated synchronously before onSubscribe is called (RS 1.9 guarantees
                // that Publisher.subscribe calls onSubscribe synchronously).
                const subs: Subscription[] = new Array(n);

                const terminate = (err?: Error) => {
                    if (terminated) return;
                    terminated = true;
                    for (const s of subs) s?.unsubscribe();
                    if (err) subscriber.onError(err);
                    else subscriber.onComplete();
                };

                const operatorSub: Subscription = {
                    request(count: number) {
                        if (cancelled || terminated) return;
                        if (count <= 0) { terminate(new Error(`request must be > 0, but was ${count}`)); return; }
                        for (const s of subs) s.request(count);
                    },
                    unsubscribe() { cancelled = true; for (const s of subs) s?.unsubscribe(); }
                };

                // Subscribe to all sources first so subs[] is fully populated before
                // subscriber.onSubscribe is called — this prevents losing demand that
                // the subscriber issues immediately inside onSubscribe (RS rule 2.7 /
                // convenience overloads call request() from onSubscribe).
                for (let i = 0; i < n; i++) {
                    let inner!: Subscription;
                    sources[i].subscribe({
                        onSubscribe(s: Subscription) { inner = s; },
                        onNext(v: T) { if (!cancelled && !terminated) subscriber.onNext(v); },
                        onError(e: Error) { terminate(e); },
                        onComplete() { if (++doneCount === n) terminate(); }
                    });
                    subs[i] = inner;
                }

                subscriber.onSubscribe(operatorSub);
                return operatorSub;
            }
        });
    }

    /**
     * Subscribes to all `sources` concurrently and emits the **first value** produced
     * by any of them. All other subscriptions are cancelled immediately after the winner emits.
     *
     * If all sources complete without emitting a value, the result completes empty.
     * Errors from non-winning sources are ignored once a winner is found.
     *
     * @param sources - Publishers to race.
     * @returns A `Flux<T>` that emits the first value from any source.
     *
     * @example
     * ```typescript
     * // Take whichever request responds first
     * Flux.firstWithValue(fetchFromCDN(), fetchFromOrigin())
     *   .subscribe(data => console.log(data));
     * ```
     */
    public static firstWithValue<T>(...sources: Publisher<T>[]): Flux<T> {
        return new Flux<T>({
            subscribe(subscriber: Subscriber<T>): Subscription {
                if (sources.length === 0) {
                    const sub = { request() {}, unsubscribe() {} };
                    subscriber.onSubscribe(sub);
                    subscriber.onComplete();
                    return sub;
                }

                const n = sources.length;
                let won = false;
                let cancelled = false;
                let finished = 0;
                let lastError: Error | null = null;
                const subs: Subscription[] = new Array(n);

                const operatorSub: Subscription = {
                    request(count: number) { if (!cancelled) for (const s of subs) s.request(count); },
                    unsubscribe() { cancelled = true; for (const s of subs) s?.unsubscribe(); }
                };

                // Collect all subscriptions before calling subscriber.onSubscribe so that
                // demand issued inside onSubscribe reaches every source.
                for (let i = 0; i < n; i++) {
                    const idx = i;
                    let inner!: Subscription;
                    sources[idx].subscribe({
                        onSubscribe(s: Subscription) { inner = s; },
                        onNext(v: T) {
                            if (cancelled || won) return;
                            won = true;
                            for (let j = 0; j < n; j++) { if (j !== idx) subs[j]?.unsubscribe(); }
                            subscriber.onNext(v);
                            subscriber.onComplete();
                        },
                        onError(e: Error) {
                            if (cancelled || won) return;
                            lastError = e;
                            if (++finished === n) subscriber.onError(lastError!);
                        },
                        onComplete() {
                            if (cancelled || won) return;
                            if (++finished === n) subscriber.onComplete();
                        }
                    });
                    subs[idx] = inner;
                }

                subscriber.onSubscribe(operatorSub);
                return operatorSub;
            }
        });
    }

    /**
     * Emits an ever-incrementing counter (0, 1, 2, …) at a fixed `ms` rate.
     *
     * Items are **dropped** when downstream has no outstanding demand (rather than
     * buffered), matching Reactor's `Flux.interval` drop-on-overflow behaviour.
     * The stream never completes on its own — cancel the subscription to stop it.
     *
     * @param ms - Interval duration in milliseconds.
     * @returns An infinite `Flux<number>`.
     *
     * @example
     * ```typescript
     * Flux.interval(1000).take(3).subscribe(n => console.log(n)); // 0  1  2
     * ```
     */
    public static interval(ms: number): Flux<number> {
        return new Flux<number>({
            subscribe(subscriber: Subscriber<number>): Subscription {
                let demand = 0;
                let counter = 0;
                let cancelled = false;
                const id = setInterval(() => {
                    if (cancelled) return;
                    if (demand > 0) {
                        demand--;
                        subscriber.onNext(counter++);
                    }
                    // demand === 0 → drop the tick (matches Reactor's interval behaviour)
                }, ms);
                const sub: Subscription = {
                    request(n: number) {
                        if (cancelled) return;
                        if (n <= 0) { subscriber.onError(new Error(`request must be > 0, but was ${n}`)); return; }
                        demand = Math.min(demand + n, Number.MAX_SAFE_INTEGER);
                    },
                    unsubscribe() { cancelled = true; clearInterval(id); }
                };
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    /**
     * Combines items from 2 sources positionally via `combiner`.
     * Emits one combined value for each position; stops when the shorter source exhausts.
     */
    public static zip<A, B, R>(
        sources: [Publisher<A>, Publisher<B>],
        combiner: (a: A, b: B) => R
    ): Flux<R>;

    /**
     * Combines items from 3 sources positionally via `combiner`.
     */
    public static zip<A, B, C, R>(
        sources: [Publisher<A>, Publisher<B>, Publisher<C>],
        combiner: (a: A, b: B, c: C) => R
    ): Flux<R>;

    /**
     * Combines items from 4 sources positionally via `combiner`.
     */
    public static zip<A, B, C, D, R>(
        sources: [Publisher<A>, Publisher<B>, Publisher<C>, Publisher<D>],
        combiner: (a: A, b: B, c: C, d: D) => R
    ): Flux<R>;

    /**
     * General N-source positional zip.
     *
     * Subscribes to each source concurrently, using a prefetch of 1 per source.
     * Once every source has provided one item for a given position, `combiner` is
     * called with those items and the result is emitted downstream.  The stream
     * completes when any source completes and has no buffered items left.
     *
     * @param sources - Publishers to zip.
     * @param combiner - Combines one item from each source at the same position.
     * @returns A `Flux<R>` of combined values.
     *
     * @example
     * ```typescript
     * Flux.zip([Flux.just(1, 2), Flux.just('a', 'b')], (n, s) => `${n}${s}`)
     *   .subscribe(v => console.log(v)); // '1a'  '2b'
     * ```
     */
    public static zip<T, R>(
        sources: Publisher<T>[],
        combiner: (...args: T[]) => R
    ): Flux<R>;

    public static zip(
        sources: Publisher<unknown>[],
        combiner: (...args: unknown[]) => unknown
    ): Flux<unknown> {
        if (sources.length === 0) return Flux.empty();
        return new Flux<unknown>({
            subscribe(subscriber: Subscriber<unknown>): Subscription {
                const n = sources.length;
                let demand = 0;
                let cancelled = false;
                let terminated = false;
                const queues: unknown[][] = Array.from({ length: n }, () => []);
                const completed: boolean[] = new Array(n).fill(false);
                const subs: Subscription[] = new Array(n);

                const tryEmit = () => {
                    while (!cancelled && !terminated && demand > 0 && queues.every(q => q.length > 0)) {
                        demand--;
                        const args = queues.map(q => q.shift()!);
                        let result: unknown;
                        try { result = combiner(...args); }
                        catch (e) {
                            terminated = true;
                            for (const s of subs) s?.unsubscribe();
                            subscriber.onError(e instanceof Error ? e : new Error(String(e)));
                            return;
                        }
                        subscriber.onNext(result);
                        // Re-fetch 1 from each source that just provided an item
                        for (let i = 0; i < n; i++) {
                            if (!completed[i]) subs[i].request(1);
                        }
                    }
                    // Complete when any source is exhausted (queue empty + completed)
                    if (!terminated && completed.some((done, i) => done && queues[i].length === 0)) {
                        terminated = true;
                        for (const s of subs) s?.unsubscribe();
                        subscriber.onComplete();
                    }
                };

                const operatorSub: Subscription = {
                    request(count: number) {
                        if (cancelled || terminated) return;
                        if (count <= 0) { subscriber.onError(new Error(`request must be > 0, but was ${count}`)); return; }
                        demand = Math.min(demand + count, Number.MAX_SAFE_INTEGER);
                        tryEmit();
                    },
                    unsubscribe() { cancelled = true; for (const s of subs) s?.unsubscribe(); }
                };

                for (let i = 0; i < n; i++) {
                    const idx = i;
                    let inner!: Subscription;
                    sources[idx].subscribe({
                        onSubscribe(s: Subscription) { inner = s; },
                        onNext(v: unknown) {
                            if (cancelled || terminated) return;
                            queues[idx].push(v);
                            tryEmit();
                        },
                        onError(e: Error) {
                            if (cancelled || terminated) return;
                            terminated = true;
                            for (const s of subs) s?.unsubscribe();
                            subscriber.onError(e);
                        },
                        onComplete() {
                            if (cancelled || terminated) return;
                            completed[idx] = true;
                            tryEmit(); // triggers completion check
                        }
                    });
                    subs[idx] = inner;
                }

                subscriber.onSubscribe(operatorSub);
                // Prefetch 1 from each source
                for (let i = 0; i < n; i++) subs[i].request(1);
                return operatorSub;
            }
        }) as Flux<unknown>;
    }

    /**
     * Combines the **latest** value from each source whenever any source emits.
     *
     * Does not emit until all sources have produced at least one value.
     * Continues emitting with each new item from any source, using the latest values
     * from all others.  Sources run at unbounded speed; downstream demand controls delivery.
     *
     * @param sources - Publishers to combine.
     * @param combiner - Called with the latest value from each source.
     * @returns A `Flux<R>` of combined values.
     *
     * @example
     * ```typescript
     * Flux.combineLatest([Flux.just(1, 2), Flux.just('a', 'b')], (n, s) => `${n}${s}`)
     *   .subscribe(v => console.log(v)); // '1a'  '2a'  '2b'  (order depends on timing)
     * ```
     */
    public static combineLatest<A, B, R>(
        sources: [Publisher<A>, Publisher<B>],
        combiner: (a: A, b: B) => R
    ): Flux<R>;

    public static combineLatest<A, B, C, R>(
        sources: [Publisher<A>, Publisher<B>, Publisher<C>],
        combiner: (a: A, b: B, c: C) => R
    ): Flux<R>;

    public static combineLatest<A, B, C, D, R>(
        sources: [Publisher<A>, Publisher<B>, Publisher<C>, Publisher<D>],
        combiner: (a: A, b: B, c: C, d: D) => R
    ): Flux<R>;

    public static combineLatest<T, R>(
        sources: Publisher<T>[],
        combiner: (...args: T[]) => R
    ): Flux<R>;

    public static combineLatest(
        sources: Publisher<unknown>[],
        combiner: (...args: unknown[]) => unknown
    ): Flux<unknown> {
        if (sources.length === 0) return Flux.empty();
        return new Flux<unknown>({
            subscribe(subscriber: Subscriber<unknown>): Subscription {
                const n = sources.length;
                let demand = 0;
                let cancelled = false;
                let terminated = false;
                const pending: unknown[] = [];
                const latest: unknown[] = new Array(n).fill(undefined);
                const hasValue: boolean[] = new Array(n).fill(false);
                const completed: boolean[] = new Array(n).fill(false);
                const subs: Subscription[] = new Array(n);

                const flush = () => {
                    while (!cancelled && !terminated && demand > 0 && pending.length > 0) {
                        demand--;
                        subscriber.onNext(pending.shift()!);
                    }
                    if (!terminated && completed.every(Boolean) && pending.length === 0) {
                        terminated = true;
                        subscriber.onComplete();
                    }
                };

                const operatorSub: Subscription = {
                    request(count: number) {
                        if (cancelled || terminated) return;
                        if (count <= 0) { subscriber.onError(new Error(`request must be > 0, but was ${count}`)); return; }
                        demand = Math.min(demand + count, Number.MAX_SAFE_INTEGER);
                        flush();
                    },
                    unsubscribe() { cancelled = true; for (const s of subs) s?.unsubscribe(); }
                };

                for (let i = 0; i < n; i++) {
                    const idx = i;
                    let inner!: Subscription;
                    sources[idx].subscribe({
                        onSubscribe(s: Subscription) { inner = s; },
                        onNext(v: unknown) {
                            if (cancelled || terminated) return;
                            latest[idx] = v;
                            hasValue[idx] = true;
                            if (hasValue.every(Boolean)) {
                                let result: unknown;
                                try { result = combiner(...latest); }
                                catch (e) {
                                    terminated = true;
                                    for (const s of subs) s?.unsubscribe();
                                    subscriber.onError(e instanceof Error ? e : new Error(String(e)));
                                    return;
                                }
                                pending.push(result);
                                flush();
                            }
                        },
                        onError(e: Error) {
                            if (cancelled || terminated) return;
                            terminated = true;
                            for (const s of subs) s?.unsubscribe();
                            subscriber.onError(e);
                        },
                        onComplete() {
                            if (cancelled || terminated) return;
                            completed[idx] = true;
                            flush(); // may trigger completion if all done
                        }
                    });
                    subs[idx] = inner;
                }

                subscriber.onSubscribe(operatorSub);
                // Sources run unbounded; combined output is buffered by pending[] and
                // released as downstream demand arrives.
                for (let i = 0; i < n; i++) subs[i].request(Number.MAX_SAFE_INTEGER);
                return operatorSub;
            }
        }) as Flux<unknown>;
    }

    // ─────────────────── PipePublisher operators ──────────────────

    /**
     * Transforms each item using `fn`, producing a `Flux<R>`.
     *
     * If `fn` throws, the exception is forwarded to `onError` and the stream terminates.
     *
     * @param fn - Mapping function applied to every item.
     * @returns A new `Flux<R>`.
     *
     * @example
     * ```typescript
     * Flux.just(1, 2, 3).map(n => n * 10).subscribe(v => console.log(v));
     * // 10  20  30
     * ```
     */
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

    /**
     * Transforms each item with `fn`, silently discarding `null`/`undefined` results.
     *
     * When an item is discarded, upstream demand is replenished by 1 so the
     * downstream demand accounting stays correct.
     *
     * @param fn - Mapping function; returning `null` or `undefined` skips the item.
     * @returns A `Flux<NonNullable<R>>` with nulls filtered out.
     *
     * @example
     * ```typescript
     * Flux.just('hello', '', 'world')
     *   .mapNotNull(s => s.length > 0 ? s : null)
     *   .subscribe(v => console.log(v));
     * // hello  world
     * ```
     */
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
     * Fine-grained per-item transform: the `handler` receives each item together with
     * a {@link Sink} and may emit **0 or 1** result items.
     *
     * - Calling `sink.next(r)` once emits `r` downstream.
     * - Calling `sink.next(r)` more than once is silently ignored (0-or-1 semantics).
     * - Calling `sink.complete()` or `sink.error(e)` terminates the stream early.
     * - If the handler emits nothing (0 items), upstream demand is replenished by 1.
     *
     * This is the Flux equivalent of `reactor-core`'s `handle(BiConsumer<T, SynchronousSink<R>>)`.
     *
     * @param handler - Called for each upstream item with the item and an output `Sink<R>`.
     * @returns A `Flux<R>`.
     *
     * @example
     * ```typescript
     * // Convert strings to numbers, skip non-numeric entries
     * Flux.just('1', 'two', '3').handle<number>((s, sink) => {
     *   const n = parseInt(s);
     *   if (!isNaN(n)) sink.next(n);
     * }).subscribe(v => console.log(v));
     * // 1  3
     * ```
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

    /**
     * Maps each item to an inner publisher and merges all inner publishers concurrently
     * into a single `Flux<R>` (**merge** semantics).
     *
     * Inner publishers are subscribed to as soon as their corresponding outer item arrives.
     * Items from different inner publishers can interleave. The resulting stream completes
     * when the outer source completes **and** all inner publishers complete. Any error
     * (from outer or any inner) immediately terminates the stream.
     *
     * @param fn - Maps each item to a `Flux<R>`, `Mono<R>`, or any `Publisher<R>`.
     * @returns A merged `Flux<R>`.
     *
     * @example
     * ```typescript
     * Flux.just(1, 2, 3)
     *   .flatMap(n => Mono.just(n * 10))
     *   .subscribe(v => console.log(v));
     * // 10  20  30  (order may vary with async inner publishers)
     * ```
     */
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

    /**
     * Maps each item to an inner publisher and subscribes to them **sequentially**,
     * preserving order (**concat** semantics).
     *
     * The next inner publisher is subscribed to only after the previous one completes.
     * This guarantees item ordering but has no concurrency.
     *
     * @param fn - Maps each item to a `Flux<R>`, `Mono<R>`, or any `Publisher<R>`.
     * @returns An ordered, sequential `Flux<R>`.
     *
     * @example
     * ```typescript
     * Flux.just(1, 2, 3)
     *   .concatMap(n => Mono.just(`item-${n}`))
     *   .subscribe(v => console.log(v));
     * // item-1  item-2  item-3
     * ```
     */
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

    /**
     * Maps each item to an inner publisher, **cancelling** the previous inner subscription
     * whenever a new outer item arrives (**switch** semantics).
     *
     * Only the most recently started inner publisher is active at any point. This is
     * useful for patterns like "search as you type" where stale results should be
     * discarded when a newer request supersedes them.
     *
     * @param fn - Maps each item to a `Flux<R>`, `Mono<R>`, or any `Publisher<R>`.
     * @returns A `Flux<R>` that tracks only the latest inner publisher.
     */
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

    /**
     * Passes only items for which `predicate` returns `true`.
     *
     * Each dropped item replenishes upstream demand by 1 to keep the total
     * outstanding demand accurate.
     *
     * @param predicate - Synchronous test applied to each item.
     * @returns A filtered `Flux<T>`.
     *
     * @example
     * ```typescript
     * Flux.just(1, 2, 3, 4).filter(n => n % 2 === 0).subscribe(v => console.log(v));
     * // 2  4
     * ```
     */
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

    /**
     * Asynchronous filter: for each item, subscribes to `predicate(item)` and forwards
     * the item downstream only if the predicate publisher emits `true`.
     *
     * Implemented via {@link concatMap} — predicates run sequentially.
     *
     * @param predicate - Returns a `Publisher<boolean>` for each item.
     * @returns A filtered `Flux<T>`.
     */
    public filterWhen(predicate: (value: T) => Publisher<boolean>): Flux<T> {
        return this.concatMap(v =>
            Flux.from(predicate(v))
                .filter(pass => pass)
                .map(() => v)
        );
    }

    /**
     * Unsafe type cast — changes the declared element type to `R` without any
     * runtime conversion.  Use only when you are certain the actual runtime
     * type is compatible.
     *
     * @typeParam R - The target element type.
     * @returns This `Flux` re-typed as `Flux<R>`.
     */
    public cast<R>(): Flux<R> { return new Flux<R>(this.source as unknown as Publisher<R>); }

    /**
     * Emits at most `n` items, then cancels the upstream subscription and completes.
     *
     * If `n ≤ 0`, returns an empty `Flux` immediately.
     *
     * @param n - Maximum number of items to emit.
     * @returns A bounded `Flux<T>`.
     */
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

    /**
     * Emits items while `predicate(item)` returns `true`.
     * Completes (and cancels upstream) on the first item for which it returns `false`.
     *
     * @param predicate - Tested synchronously for each item.
     * @returns A `Flux<T>` that stops on the first `false`.
     */
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

    /**
     * Forwards items from this `Flux` until `trigger` emits its first item (or completes),
     * then cancels the source and completes downstream.
     *
     * If `trigger` signals an error, that error is forwarded to the subscriber.
     *
     * @param trigger - A `Publisher` whose first emission ends this stream.
     * @returns A `Flux<T>` that stops when the trigger fires.
     */
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
    /**
     * Emits `value` if the source completes without having emitted any items,
     * then completes.  If the source does emit items, they pass through unchanged.
     *
     * @param value - Fallback item to emit when the source is empty.
     * @returns A `Flux<T>` that is never empty.
     */
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

    /**
     * Re-subscribes to the source on `onError`, up to `maxRetries` times.
     *
     * Accumulated downstream demand from previous attempts is preserved across retries.
     * If the error persists after all retries are exhausted, it is forwarded to downstream.
     *
     * @param maxRetries - Maximum number of retry attempts (default: `Number.MAX_SAFE_INTEGER`).
     * @returns A `Flux<T>` with automatic retry on error.
     */
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

    /**
     * Emits a running accumulation of the stream.
     *
     * The first item is emitted as-is and becomes the initial accumulator.
     * Each subsequent item is combined with the running accumulator using `reducer`,
     * and the result is emitted downstream.
     *
     * @param reducer - Combines the running accumulator with the next item.
     * @returns A `Flux<T>` of intermediate accumulation results.
     *
     * @example
     * ```typescript
     * Flux.just(1, 2, 3, 4).scan((acc, n) => acc + n).subscribe(v => console.log(v));
     * // 1  3  6  10
     * ```
     */
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

    /**
     * Emits a running accumulation of the stream with an explicit seed value.
     *
     * `seedFactory` is called once per subscription to produce the initial accumulator.
     * Each upstream item is folded into the accumulator and the intermediate result is
     * emitted downstream.
     *
     * @param seedFactory - Called per subscription to produce the initial accumulator of type `A`.
     * @param reducer - Combines the current accumulator with the next item.
     * @returns A `Flux<A>` of intermediate accumulation results.
     *
     * @example
     * ```typescript
     * Flux.just(1, 2, 3).scanWith(() => 0, (acc, n) => acc + n).subscribe(v => console.log(v));
     * // 1  3  6
     * ```
     */
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

    /**
     * Pair-wise combines items from this `Flux` and `other` using `combiner`.
     *
     * Items are matched by position: the first item from each source is combined, then the second, etc.
     * The stream completes when either source completes. If either source errors, the error is forwarded.
     *
     * @param other - The second publisher to zip with.
     * @param combiner - Combines one item from each source into a result item.
     * @returns A `Flux<V>` of combined pairs.
     *
     * @example
     * ```typescript
     * Flux.just(1, 2, 3)
     *   .zipWith(Flux.just('a', 'b', 'c'), (n, s) => `${n}${s}`)
     *   .subscribe(v => console.log(v));
     * // 1a  2b  3c
     * ```
     */
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

    /**
     * Executes a side-effect function when the source completes normally.
     *
     * The function runs just before `onComplete` is forwarded to the downstream subscriber.
     * Any exception thrown by `fn` is silently swallowed.
     *
     * @param fn - Side-effect to run on normal completion.
     * @returns A `Flux<T>` with the side effect attached.
     */
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

    /**
     * Executes a side-effect function when the source terminates — either normally (`onComplete`)
     * or with an error (`onError`).
     *
     * The function runs before the terminal signal is forwarded downstream.
     * Any exception thrown by `fn` is silently swallowed.
     *
     * @param fn - Side-effect to run on any terminal signal.
     * @returns A `Flux<T>` with the side effect attached.
     */
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

    /**
     * Executes a side-effect function when the downstream subscriber cancels
     * (calls `unsubscribe()`).
     *
     * Any exception thrown by `fn` is silently swallowed.
     *
     * @param fn - Side-effect to run on cancellation.
     * @returns A `Flux<T>` with the side effect attached.
     */
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

    /**
     * Executes a side-effect function after the stream ends for **any** reason —
     * normal completion, error, or cancellation.
     *
     * Any exception thrown by `fn` is silently swallowed.
     *
     * @param fn - Side-effect to run after any terminal event or cancellation.
     * @returns A `Flux<T>` with the side effect attached.
     */
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

    /**
     * Low-level escape hatch for bridging imperative push-based sources.
     *
     * The `producer` receives three callbacks and must call them to drive the stream.
     * `onRequest` and `onUnsubscribe` let the caller respond to downstream demand and
     * cancellation respectively.
     *
     * @param producer - Callback that receives `(onNext, onError, onComplete)` and drives the stream.
     * @param onRequest - Called when the downstream subscriber requests `n` more items.
     * @param onUnsubscribe - Called when the downstream subscriber cancels.
     * @returns A `Flux<R>` backed by the imperative producer.
     */
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

    /**
     * Returns the first item of this `Flux` as a `Mono`.
     *
     * If the source is empty, the resulting `Mono` completes without emitting a value.
     * The source subscription is cancelled immediately after the first item is received.
     *
     * @returns A `Mono<T>` that emits the first item, or completes empty.
     */
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

    /**
     * Returns the last item of this `Flux` as a `Mono`.
     *
     * The source is fully consumed before the value is emitted.
     * If the source is empty, the resulting `Mono` completes without emitting a value.
     *
     * @returns A `Mono<T>` that emits the last item, or completes empty.
     */
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

    /**
     * Counts all items emitted by this `Flux` and emits the total as a `Mono<number>`.
     *
     * The source is fully consumed before the count is emitted.
     *
     * @returns A `Mono<number>` emitting the number of items.
     */
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

    /**
     * Emits `true` if this `Flux` emits at least one item, `false` if it completes empty.
     *
     * The source subscription is cancelled as soon as the first item is observed.
     *
     * @returns A `Mono<boolean>`.
     */
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

    /**
     * Emits `true` if any item matches `predicate`, `false` if none do.
     *
     * Short-circuits: cancels the source and emits `true` on the first matching item.
     * If `predicate` throws, the error is forwarded to the subscriber.
     *
     * @param predicate - Test applied to each item.
     * @returns A `Mono<boolean>`.
     */
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

    /**
     * Emits `true` if every item matches `predicate`, `false` if any item does not.
     *
     * Short-circuits: cancels the source and emits `false` on the first non-matching item.
     * If `predicate` throws, the error is forwarded to the subscriber.
     *
     * @param predicate - Test applied to each item.
     * @returns A `Mono<boolean>`.
     */
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

    /**
     * Emits `true` if no item matches `predicate`, `false` if any item does.
     *
     * This is the logical negation of {@link any}.
     *
     * @param predicate - Test applied to each item.
     * @returns A `Mono<boolean>`.
     */
    public none(predicate: (value: T) => boolean): Mono<boolean> {
        return this.any(predicate).map(v => !v);
    }

    /**
     * Returns the item at zero-based `index` as a `Mono<T>`.
     *
     * If the source completes before reaching `index`, emits `defaultValue` if provided,
     * otherwise completes empty.
     *
     * @param index - Zero-based position of the desired item.
     * @param defaultValue - Optional fallback value emitted if the index is out of range.
     * @returns A `Mono<T>`.
     */
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

    /**
     * Collects all items into an array and emits the complete array as a `Mono<T[]>`.
     *
     * The source is fully consumed before the array is emitted.
     *
     * @returns A `Mono<T[]>` containing all emitted items.
     */
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

    /**
     * Alias for {@link collect}.
     *
     * @returns A `Mono<T[]>` containing all emitted items.
     */
    public collectList(): Mono<T[]> {
        return this.collect();
    }

    /**
     * Collects all items, sorts them, and re-emits them as a `Flux<T>`.
     *
     * Uses the native `Array.sort` algorithm. When no `comparator` is provided the
     * default lexicographic sort order is used.
     *
     * @param comparator - Optional comparison function (same signature as `Array.sort`).
     * @returns A `Flux<T>` that emits items in sorted order after the source completes.
     */
    public sort(comparator?: (a: T, b: T) => number): Flux<T> {
        return Flux.defer(() =>
            Flux.from(this.collect().map(arr => {
                arr.sort(comparator);
                return arr;
            }).flatMapMany(arr => Flux.fromIterable(arr)))
        );
    }

    /**
     * Collects items into fixed-size arrays and emits each batch as a `T[]`.
     *
     * A batch is emitted as soon as it reaches `maxSize` items.
     * The final (potentially partial) batch is emitted when the source completes.
     *
     * @param maxSize - Maximum number of items per batch.
     * @returns A `Flux<T[]>` of item batches.
     *
     * @example
     * ```typescript
     * Flux.range(1, 5).buffer(2).subscribe(v => console.log(v));
     * // [1, 2]  [3, 4]  [5]
     * ```
     */
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

    /**
     * Caches all emitted items and replays them to each new subscriber.
     *
     * The source is subscribed to on the **first** downstream subscription (lazy connect).
     * Subsequent subscribers receive a replay of all previously emitted items from the cache.
     *
     * @returns A cached, replayable `Flux<T>`.
     */
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

    /**
     * Pairs each item with its zero-based index, emitting `[index, item]` tuples.
     *
     * @returns A `Flux<[number, T]>` where the first element is the zero-based index.
     *
     * @example
     * ```typescript
     * Flux.just('a', 'b', 'c').indexed().subscribe(([i, v]) => console.log(i, v));
     * // 0 a  1 b  2 c
     * ```
     */
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

    /**
     * Skips the first `n` items from the source, then forwards the rest.
     *
     * Skipped items replenish upstream demand so the overall demand accounting stays correct.
     *
     * @param n - Number of leading items to skip.
     * @returns A `Flux<T>` without the first `n` items.
     */
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

    /**
     * Skips items while `predicate` returns `true`, then forwards all subsequent items
     * regardless of the predicate.
     *
     * Skipped items replenish upstream demand. Once the predicate returns `false`, it is
     * never evaluated again.
     *
     * @param predicate - Tested against each item until it returns `false`.
     * @returns A `Flux<T>` that starts forwarding items after the first predicate failure.
     */
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

    /**
     * Gates items from this `Flux` until `other` emits its first item.
     *
     * Items arriving before the gate opens are silently dropped (demand is replenished).
     * If `other` signals an error, the error is forwarded to downstream.
     *
     * @param other - A `Publisher` whose first emission opens the gate.
     * @returns A `Flux<T>` that starts forwarding items once `other` emits.
     */
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

    /**
     * Filters out duplicate items, forwarding only items not seen before.
     *
     * Uses a `Set` with reference equality (`===`) to track seen values.
     * Duplicates replenish upstream demand.
     *
     * @returns A `Flux<T>` with all duplicate items removed.
     */
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

    /**
     * Suppresses consecutive duplicate items.
     *
     * An item is forwarded only when it is different from the immediately preceding item
     * as determined by `comparator`. The default comparator uses `!==`.
     * Suppressed items replenish upstream demand.
     *
     * @param comparator - Returns `true` when two consecutive items are considered different
     *                     (default: `(a, b) => a !== b`).
     * @returns A `Flux<T>` without consecutive duplicates.
     *
     * @example
     * ```typescript
     * Flux.just(1, 1, 2, 2, 3).distinctUntilChanged().subscribe(v => console.log(v));
     * // 1  2  3
     * ```
     */
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

    /**
     * Delays the delivery of each item by `ms` milliseconds.
     *
     * Items are processed **one at a time**: the next item is requested from the source
     * only after the previous delayed item has been delivered downstream.  This guarantees
     * items are spaced at least `ms` apart and that `onComplete` fires only after the last
     * delayed item has been delivered.
     *
     * Errors are forwarded immediately without delay, cancelling any pending timer.
     *
     * @param ms - Delay in milliseconds between consecutive item deliveries.
     * @returns A `Flux<T>` where each item is delayed by `ms` before being forwarded.
     */
    public delayElements(ms: number): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let sourceSub: Subscription = { request() {}, unsubscribe() {} };
                let pending: { cancel(): void } | null = null;
                let cancelled = false;
                let demand = 0;
                let sourceCompleted = false;

                const operatorSub: Subscription = {
                    request(n: number) {
                        if (cancelled) return;
                        if (n <= 0) { subscriber.onError(new Error(`request must be > 0, but was ${n}`)); return; }
                        const wasZero = demand === 0;
                        demand = Math.min(demand + n, Number.MAX_SAFE_INTEGER);
                        // Only pull from source if no item is currently in-flight (timer pending).
                        if (wasZero && pending === null) sourceSub.request(1);
                    },
                    unsubscribe() {
                        cancelled = true;
                        pending?.cancel();
                        pending = null;
                        sourceSub.unsubscribe();
                    }
                };

                this.source.subscribe({
                    onSubscribe(s: Subscription) {
                        sourceSub = s;
                        subscriber.onSubscribe(operatorSub);
                    },
                    onNext(v: T) {
                        pending = Schedulers.delay(ms).schedule(() => {
                            pending = null;
                            if (cancelled) return;
                            demand--;
                            subscriber.onNext(v);
                            if (cancelled) return;
                            if (sourceCompleted) {
                                subscriber.onComplete();
                            } else if (demand > 0) {
                                sourceSub.request(1);
                            }
                        });
                    },
                    onError(e: Error) {
                        pending?.cancel();
                        pending = null;
                        subscriber.onError(e);
                    },
                    onComplete() {
                        sourceCompleted = true;
                        // If no item is in-flight, complete immediately.
                        // Otherwise the pending timer will deliver the last item and then complete.
                        if (pending === null && !cancelled) subscriber.onComplete();
                    }
                });

                return operatorSub;
            }
        });
    }

    /**
     * Appends `other` to this `Flux`, subscribing to `other` only after this source completes.
     *
     * Items from this source are emitted first, in order.  Once this source completes,
     * items from `other` are emitted.  If either source errors, the error is forwarded
     * and `other` is not subscribed to.
     *
     * @param other - The publisher to concatenate after this source.
     * @returns A `Flux<T>` that emits items from both sources sequentially.
     */
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

    /**
     * Merges this `Flux` with `other`, subscribing to both concurrently.
     *
     * Items from both sources are interleaved as they arrive. The resulting stream
     * completes when **both** sources complete. Any error from either source terminates
     * the merged stream immediately.
     *
     * @param other - The publisher to merge with this source.
     * @returns A `Flux<T>` that emits items from both sources concurrently.
     */
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

    /**
     * Reduces the stream to a single value using `reducer`, emitting the final result as a `Mono<T>`.
     *
     * The first item becomes the initial accumulator. If the source is empty, the resulting
     * `Mono` completes without emitting a value.
     *
     * @param reducer - Combines the running accumulator with the next item.
     * @returns A `Mono<T>` emitting the final reduced value.
     *
     * @example
     * ```typescript
     * Flux.just(1, 2, 3, 4).reduce((acc, n) => acc + n).subscribe(v => console.log(v));
     * // 10
     * ```
     */
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

    /**
     * Reduces the stream to a single value of type `A`, using an explicit seed, and emits it as `Mono<A>`.
     *
     * `seedFactory` is called once per subscription to produce the initial accumulator.
     * Unlike {@link reduce}, this always emits a value even if the source is empty (the seed).
     *
     * @param seedFactory - Produces the initial accumulator value per subscription.
     * @param reducer - Combines the running accumulator with the next item.
     * @returns A `Mono<A>` emitting the final reduced value.
     *
     * @example
     * ```typescript
     * Flux.just(1, 2, 3)
     *   .reduceWith(() => '', (acc, n) => acc + n)
     *   .subscribe(v => console.log(v)); // '123'
     * ```
     */
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

    /**
     * Ignores all items from this `Flux` and emits a single `void` value on completion.
     *
     * Useful when you care only about the completion signal, not about the emitted items.
     * If the source errors, the error is forwarded.
     *
     * @returns A `Mono<void>` that signals when this `Flux` completes.
     */
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

    /**
     * Ignores all items from this `Flux`, then subscribes to `other` after this source completes,
     * draining `other` completely and returning a `Mono<void>`.
     *
     * If this source or `other` errors, the error is forwarded.
     *
     * @param other - A `Publisher` to subscribe to after this source completes.
     * @returns A `Mono<void>` that completes once `other` completes.
     */
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

    // ──────────────────── Advanced / Flux-specific operators ─────────────────

    /**
     * Re-subscribes to the source when the publisher returned by `fn` emits,
     * enabling controlled retry with strategies like exponential back-off.
     *
     * `fn` receives a `Flux<Error>` — the stream of errors emitted by each failed attempt.
     * Each emission from the returned publisher triggers a retry.
     * When the returned publisher completes or errors, the retry loop stops.
     *
     * @param fn - Receives an error stream and returns a control publisher.
     * @returns A `Flux<T>` with dynamic retry behaviour.
     *
     * @example
     * ```typescript
     * // Exponential back-off: wait 2^n × 100ms between retries, max 3 retries
     * flux.retryWhen(errors =>
     *   errors.zipWith(Flux.range(1, 3), (_, n) => n)
     *         .flatMap(n => Mono.delay(n * 100))
     * );
     * ```
     */
    public retryWhen(fn: (errors: Flux<Error>) => Publisher<unknown>): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const errBuf: Error[] = [];
                let errSub: Subscriber<Error> | null = null;
                let errDemand = 0;

                const pushErr = (e: Error) => {
                    errBuf.push(e);
                    drainErrs();
                };
                const drainErrs = () => {
                    while (errDemand > 0 && errBuf.length > 0 && errSub) {
                        errDemand--;
                        errSub.onNext(errBuf.shift()!);
                    }
                };

                const errorFlux = new Flux<Error>({
                    subscribe(sub: Subscriber<Error>): Subscription {
                        errSub = sub;
                        const relay: Subscription = {
                            request(n) { errDemand = Math.min(errDemand + n, Number.MAX_SAFE_INTEGER); drainErrs(); },
                            unsubscribe() { errSub = null; }
                        };
                        sub.onSubscribe(relay);
                        return relay;
                    }
                });

                let currentSub: Subscription = { request() {}, unsubscribe() {} };
                let demand = 0;
                let cancelled = false;
                let terminated = false;

                const operatorSub: Subscription = {
                    request(n: number) {
                        if (cancelled || terminated) return;
                        if (n <= 0) { subscriber.onError(new Error(`request must be > 0, but was ${n}`)); return; }
                        demand = Math.min(demand + n, Number.MAX_SAFE_INTEGER);
                        currentSub.request(n);
                    },
                    unsubscribe() { cancelled = true; currentSub.unsubscribe(); }
                };

                const attempt = () => {
                    this.source.subscribe({
                        onSubscribe(s: Subscription) { currentSub = s; if (demand > 0) s.request(demand); },
                        onNext(v: T) { if (!cancelled && !terminated) subscriber.onNext(v); },
                        onError(e: Error) { if (!cancelled && !terminated) pushErr(e); },
                        onComplete() {
                            if (!cancelled && !terminated) {
                                terminated = true;
                                errSub?.onComplete();
                                subscriber.onComplete();
                            }
                        }
                    });
                };

                fn(errorFlux).subscribe({
                    onSubscribe(s) { s.request(Number.MAX_SAFE_INTEGER); },
                    onNext(_v) { if (!cancelled && !terminated) { currentSub.unsubscribe(); attempt(); } },
                    onError(e: Error) { if (!cancelled && !terminated) { terminated = true; subscriber.onError(e); } },
                    onComplete() { if (!cancelled && !terminated) { terminated = true; subscriber.onComplete(); } }
                });

                subscriber.onSubscribe(operatorSub);
                attempt();
                return operatorSub;
            }
        });
    }

    /**
     * Re-subscribes to the source each time it completes normally, as controlled by
     * the publisher returned by `fn` (**polling** semantics).
     *
     * `fn` receives a `Flux<void>` — one emission per completion. Each emission triggers
     * a repeat. When the control publisher completes or errors, the loop stops.
     *
     * @param fn - Receives a completion-signal stream and returns a control publisher.
     * @returns A `Flux<T>` that repeats under the control of `fn`.
     *
     * @example
     * ```typescript
     * // Poll every second, stop after 5 times
     * fetchLatest()
     *   .toFlux()
     *   .repeatWhen(completes => completes.zipWith(Flux.range(1, 5), (_, n) => n)
     *                                     .flatMap(() => Mono.delay(1000)));
     * ```
     */
    public repeatWhen(fn: (completes: Flux<void>) => Publisher<unknown>): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const compBuf: void[] = [];
                let compSub: Subscriber<void> | null = null;
                let compDemand = 0;
                let drainingComps = false;

                const drainComps = () => {
                    if (drainingComps) return;
                    drainingComps = true;
                    try {
                        while (!terminated && !cancelled && compDemand > 0 && compBuf.length > 0 && compSub) {
                            compDemand--;
                            compSub.onNext(compBuf.shift()!);
                        }
                    } finally { drainingComps = false; }
                };

                const pushComp = () => {
                    compBuf.push(undefined);
                    drainComps();
                };

                const completeFlux = new Flux<void>({
                    subscribe(sub: Subscriber<void>): Subscription {
                        compSub = sub;
                        const relay: Subscription = {
                            request(n) { compDemand = Math.min(compDemand + n, Number.MAX_SAFE_INTEGER); drainComps(); },
                            unsubscribe() { compSub = null; }
                        };
                        sub.onSubscribe(relay);
                        return relay;
                    }
                });

                let currentSub: Subscription = { request() {}, unsubscribe() {} };
                let demand = 0;
                let cancelled = false;
                let terminated = false;

                const operatorSub: Subscription = {
                    request(n: number) {
                        if (cancelled || terminated) return;
                        if (n <= 0) { subscriber.onError(new Error(`request must be > 0, but was ${n}`)); return; }
                        demand = Math.min(demand + n, Number.MAX_SAFE_INTEGER);
                        currentSub.request(n);
                    },
                    unsubscribe() { cancelled = true; currentSub.unsubscribe(); }
                };

                const attempt = () => {
                    this.source.subscribe({
                        onSubscribe(s: Subscription) { currentSub = s; if (demand > 0) s.request(demand); },
                        onNext(v: T) { if (!cancelled && !terminated) subscriber.onNext(v); },
                        onError(e: Error) { if (!cancelled && !terminated) { terminated = true; subscriber.onError(e); } },
                        onComplete() { if (!cancelled && !terminated) pushComp(); }
                    });
                };

                fn(completeFlux).subscribe({
                    onSubscribe(s) { s.request(Number.MAX_SAFE_INTEGER); },
                    onNext(_v) { if (!cancelled && !terminated) { currentSub.unsubscribe(); attempt(); } },
                    onError(e: Error) { if (!cancelled && !terminated) { terminated = true; subscriber.onError(e); } },
                    onComplete() { if (!cancelled && !terminated) { terminated = true; subscriber.onComplete(); } }
                });

                subscriber.onSubscribe(operatorSub);
                attempt();
                return operatorSub;
            }
        });
    }

    /**
     * Groups items by a key derived from `keyFn`, emitting a {@link GroupedFlux} per
     * unique key. Each `GroupedFlux` is itself a `Flux<T>` exposing the group `key`.
     *
     * The source is consumed at full speed. Items are routed to the corresponding group's
     * buffer and replayed to any subscriber of that group.
     *
     * @param keyFn - Extracts the group key from each item.
     * @returns A `Flux<GroupedFlux<K, T>>` emitting one group per unique key.
     *
     * @example
     * ```typescript
     * Flux.just(1, 2, 3, 4, 5)
     *   .groupBy(n => n % 2 === 0 ? 'even' : 'odd')
     *   .flatMap(group => group.collectList().map(items => ({ key: group.key, items })))
     *   .subscribe(v => console.log(v));
     * // { key: 'odd', items: [1, 3, 5] }
     * // { key: 'even', items: [2, 4] }
     * ```
     */
    public groupBy<K>(keyFn: (value: T) => K): Flux<GroupedFlux<K, T>> {
        return new Flux<GroupedFlux<K, T>>({
            subscribe: (subscriber: Subscriber<GroupedFlux<K, T>>): Subscription => {
                const groups = new Map<K, ReplayAllSink<T>>();
                let cancelled = false;
                let sourceSub: Subscription = { request() {}, unsubscribe() {} };

                const operatorSub: Subscription = {
                    request(_n: number) { /* source runs at full speed */ },
                    unsubscribe() { cancelled = true; sourceSub.unsubscribe(); }
                };

                this.source.subscribe({
                    onSubscribe(s: Subscription) {
                        sourceSub = s;
                        s.request(Number.MAX_SAFE_INTEGER);
                        subscriber.onSubscribe(operatorSub);
                    },
                    onNext(v: T) {
                        if (cancelled) return;
                        let key: K;
                        try { key = keyFn(v); }
                        catch (e) { subscriber.onError(e instanceof Error ? e : new Error(String(e))); return; }
                        let sink = groups.get(key);
                        if (!sink) {
                            sink = new ReplayAllSink<T>();
                            groups.set(key, sink);
                            subscriber.onNext(Object.assign(Flux.from<T>(sink), { key }) as GroupedFlux<K, T>);
                        }
                        sink.next(v);
                    },
                    onError(e: Error) {
                        if (cancelled) return;
                        for (const g of groups.values()) g.error(e);
                        subscriber.onError(e);
                    },
                    onComplete() {
                        if (cancelled) return;
                        for (const g of groups.values()) g.complete();
                        subscriber.onComplete();
                    }
                });

                return operatorSub;
            }
        });
    }

    /**
     * Buffers items into arrays emitted when either `maxSize` items accumulate
     * **or** `ms` milliseconds elapse since the last flush — whichever comes first.
     *
     * @param maxSize - Maximum items per batch.
     * @param ms - Maximum time (ms) to wait before flushing a partial batch.
     * @returns A `Flux<T[]>` of batches.
     *
     * @example
     * ```typescript
     * // Batch up to 10 items or flush every 500ms
     * eventStream.bufferTimeout(10, 500).subscribe(batch => sendBatch(batch));
     * ```
     */
    public bufferTimeout(maxSize: number, ms: number): Flux<T[]> {
        return new Flux<T[]>({
            subscribe: (subscriber: Subscriber<T[]>): Subscription => {
                let batch: T[] = [];
                let timer: ReturnType<typeof setTimeout> | null = null;
                let cancelled = false;

                const flush = () => {
                    if (timer !== null) { clearTimeout(timer); timer = null; }
                    if (batch.length > 0 && !cancelled) {
                        const toEmit = batch;
                        batch = [];
                        subscriber.onNext(toEmit);
                    }
                };

                const resetTimer = () => {
                    if (timer !== null) clearTimeout(timer);
                    timer = setTimeout(() => { if (!cancelled) flush(); }, ms);
                };

                const sourceSub = this.source.subscribe({
                    onSubscribe(s: Subscription) { s.request(Number.MAX_SAFE_INTEGER); },
                    onNext(v: T) {
                        if (cancelled) return;
                        batch.push(v);
                        if (batch.length >= maxSize) flush();
                        else resetTimer();
                    },
                    onError(e: Error) {
                        if (!cancelled) { if (timer !== null) clearTimeout(timer); subscriber.onError(e); }
                    },
                    onComplete() {
                        if (!cancelled) {
                            if (timer !== null) { clearTimeout(timer); timer = null; }
                            if (batch.length > 0) subscriber.onNext(batch);
                            subscriber.onComplete();
                        }
                    }
                });

                const sub: Subscription = {
                    request(n: number) { sourceSub.request(n); },
                    unsubscribe() { cancelled = true; if (timer !== null) clearTimeout(timer); sourceSub.unsubscribe(); }
                };
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    /**
     * Samples this `Flux`, emitting the **most recent** item whenever `trigger` emits.
     *
     * If no item has arrived since the last sample, the trigger emission is ignored.
     * Useful for rate-limiting with external signals (scroll, resize, `requestAnimationFrame`).
     *
     * @param trigger - Controls when to sample; its value is ignored.
     * @returns A `Flux<T>` emitting the latest value at each trigger tick.
     */
    public sample(trigger: Publisher<unknown>): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let latest: T | undefined;
                let hasValue = false;
                let cancelled = false;
                let sourceSub: Subscription = { request() {}, unsubscribe() {} };
                let triggerSub: Subscription = { request() {}, unsubscribe() {} };

                const operatorSub: Subscription = {
                    request(n: number) { if (!cancelled) sourceSub.request(n); },
                    unsubscribe() { cancelled = true; sourceSub.unsubscribe(); triggerSub.unsubscribe(); }
                };

                triggerSub = trigger.subscribe({
                    onSubscribe(s: Subscription) { triggerSub = s; s.request(Number.MAX_SAFE_INTEGER); },
                    onNext(_v) {
                        if (cancelled || !hasValue) return;
                        const val = latest!;
                        hasValue = false;
                        latest = undefined;
                        subscriber.onNext(val);
                    },
                    onError(e: Error) { if (!cancelled) subscriber.onError(e); },
                    onComplete() {}
                });

                this.source.subscribe({
                    onSubscribe(s: Subscription) { sourceSub = s; subscriber.onSubscribe(operatorSub); },
                    onNext(v: T) { if (!cancelled) { latest = v; hasValue = true; } },
                    onError(e: Error) { if (!cancelled) subscriber.onError(e); },
                    onComplete() { if (!cancelled) subscriber.onComplete(); }
                });

                return operatorSub;
            }
        });
    }

    /**
     * For each item, subscribes to `triggerFn(item)` and waits for it to emit or complete
     * before forwarding the item downstream (**delayUntil** semantics).
     *
     * Triggers are executed sequentially (concatMap semantics). The trigger's value
     * is ignored — only its timing matters.
     *
     * @param triggerFn - Returns a publisher whose first signal releases the item.
     * @returns A `Flux<T>` where each item is held until its trigger fires.
     *
     * @example
     * ```typescript
     * Flux.just(1, 2, 3)
     *   .delayUntil(() => Mono.delay(100))
     *   .subscribe(v => console.log(v)); // each item delayed 100ms
     * ```
     */
    public delayUntil(triggerFn: (value: T) => Publisher<unknown>): Flux<T> {
        return this.concatMap(v =>
            Flux.from<T>({
                subscribe(subscriber: Subscriber<T>): Subscription {
                    let done = false;
                    let innerSub: Subscription = { request() {}, unsubscribe() {} };
                    innerSub = triggerFn(v).subscribe({
                        onSubscribe(s: Subscription) { innerSub = s; s.request(Number.MAX_SAFE_INTEGER); },
                        onNext(_r) {
                            if (!done) {
                                done = true;
                                innerSub.unsubscribe();
                                subscriber.onNext(v);
                                subscriber.onComplete();
                            }
                        },
                        onError(e: Error) { if (!done) { done = true; subscriber.onError(e); } },
                        onComplete() {
                            if (!done) { done = true; subscriber.onNext(v); subscriber.onComplete(); }
                        }
                    });
                    subscriber.onSubscribe(innerSub);
                    return innerSub;
                }
            })
        );
    }

    /**
     * Recursively expands each item using `fn`, emitting both the original items and
     * all items produced by the expansion (**breadth-first** order).
     *
     * Useful for **pagination**: call `fn` with the current page to fetch the next,
     * until `fn` returns empty.
     *
     * @param fn - Receives an item; returns a publisher of zero or more new items to expand.
     * @returns A `Flux<T>` of all original and expanded items.
     *
     * @example
     * ```typescript
     * // Fetch all pages recursively
     * fetchPage(1)
     *   .toFlux()
     *   .expand(page => page.hasNext ? fetchPage(page.nextId).toFlux() : Flux.empty())
     *   .subscribe(page => process(page));
     * ```
     */
    public expand(fn: (value: T) => Publisher<T>): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const pending: T[] = [];
                let active = false;
                let sourceDone = false;
                let cancelled = false;
                let terminated = false;
                let demand = 0;

                const terminate = (err?: Error) => {
                    if (terminated) return;
                    terminated = true;
                    if (err) subscriber.onError(err);
                    else subscriber.onComplete();
                };

                const drain = () => {
                    if (active || cancelled || terminated) return;
                    if (demand <= 0 || pending.length === 0) {
                        if (sourceDone && pending.length === 0) terminate();
                        return;
                    }
                    active = true;
                    const v = pending.shift()!;
                    demand--;
                    subscriber.onNext(v);
                    if (cancelled || terminated) { active = false; return; }

                    let inner: Publisher<T>;
                    try { inner = fn(v); }
                    catch (e) { active = false; terminate(e instanceof Error ? e : new Error(String(e))); return; }

                    inner.subscribe({
                        onSubscribe(s: Subscription) { s.request(Number.MAX_SAFE_INTEGER); },
                        onNext(r: T) { if (!cancelled && !terminated) pending.push(r); },
                        onError(e: Error) { active = false; terminate(e); },
                        onComplete() { active = false; drain(); }
                    });
                };

                const operatorSub: Subscription = {
                    request(n: number) {
                        if (cancelled || terminated) return;
                        if (n <= 0) { terminate(new Error(`request must be > 0, but was ${n}`)); return; }
                        demand = Math.min(demand + n, Number.MAX_SAFE_INTEGER);
                        drain();
                    },
                    unsubscribe() { cancelled = true; }
                };

                this.source.subscribe({
                    onSubscribe(s: Subscription) {
                        s.request(Number.MAX_SAFE_INTEGER);
                        subscriber.onSubscribe(operatorSub);
                    },
                    onNext(v: T) { if (!cancelled && !terminated) { pending.push(v); drain(); } },
                    onError(e: Error) { terminate(e); },
                    onComplete() { sourceDone = true; if (!active && pending.length === 0) terminate(); }
                });

                return operatorSub;
            }
        });
    }

    /**
     * Recursively expands each item using `fn`, emitting both the original items and
     * all items produced by the expansion (**depth-first** order).
     *
     * Unlike {@link expand} (breadth-first), each item's children are emitted immediately
     * after the item itself, before any sibling items are processed. This mirrors a
     * recursive pre-order tree traversal.
     *
     * @param fn - Receives an item; returns a publisher of zero or more new items to expand.
     * @returns A `Flux<T>` of all original and expanded items in depth-first order.
     *
     * @example
     * ```typescript
     * // Tree: 1 → [2, 3], 2 → [4, 5], others → empty
     * Flux.just(1)
     *   .expandDeep(n =>
     *     n === 1 ? Flux.just(2, 3) :
     *     n === 2 ? Flux.just(4, 5) :
     *     Flux.empty()
     *   )
     *   .subscribe(v => console.log(v));
     * // 1 2 4 5 3  (depth-first pre-order)
     * ```
     */
    public expandDeep(fn: (value: T) => Publisher<T>): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const pending: T[] = [];
                let active = false;
                let sourceDone = false;
                let cancelled = false;
                let terminated = false;
                let demand = 0;

                const terminate = (err?: Error) => {
                    if (terminated) return;
                    terminated = true;
                    if (err) subscriber.onError(err);
                    else subscriber.onComplete();
                };

                const drain = () => {
                    if (active || cancelled || terminated) return;
                    if (demand <= 0 || pending.length === 0) {
                        if (sourceDone && pending.length === 0) terminate();
                        return;
                    }
                    active = true;
                    const v = pending.shift()!;
                    demand--;
                    subscriber.onNext(v);
                    if (cancelled || terminated) { active = false; return; }

                    let inner: Publisher<T>;
                    try { inner = fn(v); }
                    catch (e) { active = false; terminate(e instanceof Error ? e : new Error(String(e))); return; }

                    const children: T[] = [];
                    inner.subscribe({
                        onSubscribe(s: Subscription) { s.request(Number.MAX_SAFE_INTEGER); },
                        onNext(r: T) { if (!cancelled && !terminated) children.push(r); },
                        onError(e: Error) { active = false; terminate(e); },
                        onComplete() {
                            // Depth-first: prepend children so they are processed before siblings.
                            if (!cancelled && !terminated) pending.unshift(...children);
                            active = false;
                            drain();
                        }
                    });
                };

                const operatorSub: Subscription = {
                    request(n: number) {
                        if (cancelled || terminated) return;
                        if (n <= 0) { terminate(new Error(`request must be > 0, but was ${n}`)); return; }
                        demand = Math.min(demand + n, Number.MAX_SAFE_INTEGER);
                        drain();
                    },
                    unsubscribe() { cancelled = true; }
                };

                this.source.subscribe({
                    onSubscribe(s: Subscription) {
                        s.request(Number.MAX_SAFE_INTEGER);
                        subscriber.onSubscribe(operatorSub);
                    },
                    onNext(v: T) { if (!cancelled && !terminated) { pending.push(v); drain(); } },
                    onError(e: Error) { terminate(e); },
                    onComplete() { sourceDone = true; if (!active && pending.length === 0) terminate(); }
                });

                return operatorSub;
            }
        });
    }

    /**
     * Pairs each item with the time elapsed (in ms) since the **previous** item
     * (or since subscription for the first item).
     *
     * @returns A `Flux<[number, T]>` where the first element is elapsed milliseconds.
     *
     * @example
     * ```typescript
     * Flux.interval(100).take(3).elapsed().subscribe(([ms, n]) => console.log(ms, n));
     * // ~100 0   ~100 1   ~100 2
     * ```
     */
    public elapsed(): Flux<[number, T]> {
        return new Flux<[number, T]>({
            subscribe: (subscriber: Subscriber<[number, T]>): Subscription => {
                let lastTime = Date.now();
                const sub = this.source.subscribe({
                    onSubscribe(_s) { lastTime = Date.now(); },
                    onNext(v: T) {
                        const now = Date.now();
                        const ms = now - lastTime;
                        lastTime = now;
                        subscriber.onNext([ms, v]);
                    },
                    onError(e: Error) { subscriber.onError(e); },
                    onComplete() { subscriber.onComplete(); }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    /**
     * Attaches a `Date.now()` timestamp (in ms since epoch) to each item.
     *
     * @returns A `Flux<[number, T]>` where the first element is the Unix timestamp in ms.
     */
    public timestamp(): Flux<[number, T]> {
        return this.map(v => [Date.now(), v] as [number, T]);
    }

    /**
     * Wraps each signal (next, error, complete) into a {@link Signal} data object,
     * emitting them all as `onNext` items on a `Flux<Signal<T>>`.
     *
     * After materializing, the resulting `Flux` always completes normally (errors become
     * `Signal.error` items rather than `onError` signals).
     * Use {@link dematerialize} to convert back.
     *
     * @returns A `Flux<Signal<T>>` where every event is represented as a data item.
     */
    public materialize(): Flux<Signal<T>> {
        return new Flux<Signal<T>>({
            subscribe: (subscriber: Subscriber<Signal<T>>): Subscription => {
                let done = false;
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v: T) { if (!done) subscriber.onNext(Signal.next(v)); },
                    onError(e: Error) {
                        if (done) return;
                        done = true;
                        subscriber.onNext(Signal.error<T>(e));
                        subscriber.onComplete();
                    },
                    onComplete() {
                        if (done) return;
                        done = true;
                        subscriber.onNext(Signal.complete<T>());
                        subscriber.onComplete();
                    }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    /**
     * Unwraps a `Flux<Signal<R>>` back into a regular `Flux<R>`, restoring
     * `error` and `complete` signals from their materialized form.
     *
     * This is the inverse of {@link materialize}.
     *
     * @returns A `Flux<R>` with signals restored from the `Signal` wrappers.
     */
    public dematerialize<R>(this: Flux<Signal<R>>): Flux<R> {
        return new Flux<R>({
            subscribe: (subscriber: Subscriber<R>): Subscription => {
                let done = false;
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(signal: Signal<R>) {
                        if (done) return;
                        if (signal.kind === 'next') subscriber.onNext(signal.value);
                        else if (signal.kind === 'error') { done = true; subscriber.onError(signal.error); }
                        else { done = true; subscriber.onComplete(); }
                    },
                    onError(e: Error) { if (!done) { done = true; subscriber.onError(e); } },
                    onComplete() { if (!done) { done = true; subscriber.onComplete(); } }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    /**
     * Buffers all upstream items regardless of downstream demand, delivering them
     * when demand becomes available.
     *
     * If `maxSize` is exceeded, an overflow error is signalled downstream.
     *
     * @param maxSize - Maximum buffer capacity (default: unbounded).
     * @returns A `Flux<T>` with explicit backpressure buffering.
     */
    public onBackpressureBuffer(maxSize: number = Number.MAX_SAFE_INTEGER): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const buffer: T[] = [];
                let demand = 0;
                let cancelled = false;
                let terminated = false;
                let terminalError: Error | null = null;
                let draining = false;

                const drain = () => {
                    if (draining || cancelled) return;
                    draining = true;
                    try {
                        while (demand > 0 && buffer.length > 0 && !cancelled) {
                            demand--;
                            subscriber.onNext(buffer.shift()!);
                        }
                        if (terminated && buffer.length === 0 && !cancelled) {
                            if (terminalError) subscriber.onError(terminalError);
                            else subscriber.onComplete();
                        }
                    } finally { draining = false; }
                };

                const sourceSub = this.source.subscribe({
                    onSubscribe(s: Subscription) { s.request(Number.MAX_SAFE_INTEGER); },
                    onNext(v: T) {
                        if (cancelled) return;
                        if (buffer.length >= maxSize) {
                            subscriber.onError(new Error(`onBackpressureBuffer overflow (maxSize=${maxSize})`));
                            return;
                        }
                        buffer.push(v);
                        drain();
                    },
                    onError(e: Error) { if (!cancelled) { terminated = true; terminalError = e; drain(); } },
                    onComplete() { if (!cancelled) { terminated = true; drain(); } }
                });

                const sub: Subscription = {
                    request(n: number) {
                        if (cancelled || (terminated && buffer.length === 0)) return;
                        if (n <= 0) { subscriber.onError(new Error(`request must be > 0, but was ${n}`)); return; }
                        demand = Math.min(demand + n, Number.MAX_SAFE_INTEGER);
                        drain();
                    },
                    unsubscribe() { cancelled = true; buffer.length = 0; sourceSub.unsubscribe(); }
                };
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    /**
     * Drops upstream items that arrive when downstream has no outstanding demand.
     *
     * An optional `onDrop` callback is invoked for each dropped item.
     *
     * @param onDrop - Optional callback called with each dropped item.
     * @returns A `Flux<T>` with drop backpressure strategy.
     */
    public onBackpressureDrop(onDrop?: (value: T) => void): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let demand = 0;
                let cancelled = false;

                const sourceSub = this.source.subscribe({
                    onSubscribe(s: Subscription) { s.request(Number.MAX_SAFE_INTEGER); },
                    onNext(v: T) {
                        if (cancelled) return;
                        if (demand > 0) { demand--; subscriber.onNext(v); }
                        else { try { onDrop?.(v); } catch (_) {} }
                    },
                    onError(e: Error) { if (!cancelled) subscriber.onError(e); },
                    onComplete() { if (!cancelled) subscriber.onComplete(); }
                });

                const sub: Subscription = {
                    request(n: number) {
                        if (cancelled) return;
                        if (n <= 0) { subscriber.onError(new Error(`request must be > 0, but was ${n}`)); return; }
                        demand = Math.min(demand + n, Number.MAX_SAFE_INTEGER);
                    },
                    unsubscribe() { cancelled = true; sourceSub.unsubscribe(); }
                };
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    /**
     * Keeps only the **latest** upstream item when downstream has no demand;
     * older buffered items are overwritten as new ones arrive.
     *
     * @returns A `Flux<T>` with latest-value backpressure strategy.
     */
    public onBackpressureLatest(): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let latest: T | undefined;
                let hasLatest = false;
                let demand = 0;
                let cancelled = false;
                let terminated = false;
                let terminalError: Error | null = null;

                const drain = () => {
                    if (cancelled) return;
                    if (demand > 0 && hasLatest) {
                        demand--;
                        const v = latest!;
                        hasLatest = false;
                        latest = undefined;
                        subscriber.onNext(v);
                    }
                    if (terminated && !hasLatest && !cancelled) {
                        if (terminalError) subscriber.onError(terminalError);
                        else subscriber.onComplete();
                    }
                };

                const sourceSub = this.source.subscribe({
                    onSubscribe(s: Subscription) { s.request(Number.MAX_SAFE_INTEGER); },
                    onNext(v: T) { if (!cancelled) { latest = v; hasLatest = true; drain(); } },
                    onError(e: Error) { if (!cancelled) { terminated = true; terminalError = e; drain(); } },
                    onComplete() { if (!cancelled) { terminated = true; drain(); } }
                });

                const sub: Subscription = {
                    request(n: number) {
                        if (cancelled || (terminated && !hasLatest)) return;
                        if (n <= 0) { subscriber.onError(new Error(`request must be > 0, but was ${n}`)); return; }
                        demand = Math.min(demand + n, Number.MAX_SAFE_INTEGER);
                        drain();
                    },
                    unsubscribe() { cancelled = true; sourceSub.unsubscribe(); }
                };
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    /**
     * Requests items from upstream in batches of `n`, replenishing when 75% of
     * the batch has been consumed (**prefetch** / **limitRate** semantics).
     *
     * Prevents requesting all items at once from expensive sources.
     *
     * @param n - Prefetch batch size.
     * @returns A `Flux<T>` with controlled upstream demand.
     */
    public limitRate(n: number): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const prefetch = Math.max(1, n);
                const replenish = Math.max(1, Math.floor(prefetch * 0.75));
                let sourceSub!: Subscription;
                let pending = 0;
                let consumed = 0;
                let cancelled = false;

                const requestMore = () => {
                    const toRequest = prefetch - pending;
                    if (toRequest > 0) { pending += toRequest; sourceSub.request(toRequest); }
                };

                const operatorSub: Subscription = {
                    request(count: number) {
                        if (cancelled) return;
                        if (count <= 0) { subscriber.onError(new Error(`request must be > 0, but was ${count}`)); return; }
                        requestMore();
                    },
                    unsubscribe() { cancelled = true; sourceSub?.unsubscribe(); }
                };

                this.source.subscribe({
                    onSubscribe(s: Subscription) { sourceSub = s; subscriber.onSubscribe(operatorSub); },
                    onNext(v: T) {
                        if (cancelled) return;
                        pending = Math.max(0, pending - 1);
                        consumed++;
                        subscriber.onNext(v);
                        if (consumed >= replenish) { consumed = 0; requestMore(); }
                    },
                    onError(e: Error) { if (!cancelled) subscriber.onError(e); },
                    onComplete() { if (!cancelled) subscriber.onComplete(); }
                });

                return operatorSub;
            }
        });
    }

    /**
     * Filters items by runtime type, passing only items that are instances of `constructor`.
     *
     * @typeParam R - The target subtype.
     * @param constructor - The class/constructor to filter by.
     * @returns A `Flux<R>` containing only items that pass `instanceof`.
     *
     * @example
     * ```typescript
     * class Cat { meow() {} }
     * class Dog { bark() {} }
     * Flux.just(new Cat(), new Dog(), new Cat())
     *   .ofType(Cat)
     *   .subscribe(cat => cat.meow());
     * ```
     */
    public ofType<R extends T>(constructor: new (...args: unknown[]) => R): Flux<R> {
        return this.filter((v): v is R => v instanceof constructor).cast<R>();
    }

    /**
     * Applies a reusable operator function `fn` to this `Flux`, deferred per subscription.
     *
     * `fn` receives this `Flux<T>` (as a `Publisher<T>`) and returns a new `Publisher<R>`.
     * The transformation is re-evaluated on each `subscribe()` call (deferred assembly).
     *
     * @param fn - Operator function to apply.
     * @returns A `Flux<R>` produced by `fn`.
     *
     * @example
     * ```typescript
     * const retryThrice = <T>(source: Publisher<T>) => Flux.from(source).retry(3);
     * Flux.just(1, 2).transformDeferred(retryThrice).subscribe(v => console.log(v));
     * ```
     */
    public transformDeferred<R>(fn: (flux: Publisher<T>) => Publisher<R>): Flux<R> {
        return Flux.defer(() => Flux.from(fn(this)));
    }
}

// ────────────────────────── GroupedFlux ──────────────────────────────────────

/**
 * A `Flux<T>` tagged with a group `key`.
 *
 * Produced by {@link Flux#groupBy} — subscribe to it exactly like any `Flux<T>`;
 * the extra `key` property identifies which group this stream belongs to.
 *
 * Implemented as a plain type intersection rather than a subclass: the runtime
 * object is an ordinary `Flux<T>` with a `key` property attached via
 * `Object.assign`, so no extra prototype chain is required.
 *
 * @typeParam K - The type of the group key.
 * @typeParam T - The type of items in the group.
 */
export type GroupedFlux<K, T> = Flux<T> & { readonly key: K };
