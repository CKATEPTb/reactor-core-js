import {PipePublisher} from "@/publishers/PipePublisher";
import {Publisher} from "@/publishers/Publisher";
import {Sink} from "@/sinks/Sink";
import {Mono} from "@/publishers/Mono";
import {Subscriber} from "@/subscriptions/Subscriber";
import {Subscription} from "@/subscriptions/Subscription";
import {Scheduler} from "@/schedulers/Scheduler";

export class Flux<T> implements PipePublisher<T> {

    protected constructor(private readonly source: Publisher<T>) {}

    subscribe(subscriber: Subscriber<T>): Subscription {
        return this.source.subscribe(subscriber);
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
                        if (buffer.length === 0 && terminated) {
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

                try { generator(sink); }
                catch (e) {
                    if (!terminated) {
                        terminated = true;
                        terminalError = e instanceof Error ? e : new Error(String(e));
                    }
                }

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
                subscriber.onSubscribe(subscription);
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
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) {
                        try {
                            const r = fn(v);
                            if (r != null) subscriber.onNext(r as NonNullable<R>);
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

    public flatMap<R>(fn: (value: T) => Publisher<R>): Flux<R> {
        // Merge-style flatMap: subscribes to each inner publisher as values arrive
        return new Flux<R>({
            subscribe: (subscriber: Subscriber<R>): Subscription => {
                let outerSub: Subscription = { request() {}, unsubscribe() {} };
                const innerSubs = new Set<Subscription>();
                let cancelled = false;
                let outerDone = false;
                let demand = 0;

                const operatorSub: Subscription = {
                    request(n: number) {
                        if (cancelled) return;
                        if (n <= 0) {
                            subscriber.onError(new Error(`request must be > 0, but was ${n}`));
                            return;
                        }
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
                        if (cancelled) return;
                        let inner: Publisher<R>;
                        try { inner = fn(v); }
                        catch (e) { subscriber.onError(e instanceof Error ? e : new Error(String(e))); return; }
                        const innerSub = inner.subscribe({
                            onSubscribe(_s) {},
                            onNext(r) { subscriber.onNext(r); },
                            onError(e) { subscriber.onError(e); },
                            onComplete() {
                                innerSubs.delete(innerSub);
                                if (outerDone && innerSubs.size === 0) subscriber.onComplete();
                            }
                        });
                        innerSubs.add(innerSub);
                        if (demand > 0) innerSub.request(demand);
                    },
                    onError(e) { subscriber.onError(e); },
                    onComplete() {
                        outerDone = true;
                        if (innerSubs.size === 0) subscriber.onComplete();
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
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) {
                        try { if (predicate(v)) subscriber.onNext(v); }
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

    public filterWhen(predicate: (value: T) => Publisher<boolean>): Flux<T> { throw new Error('Not implemented'); }
    public cast<R>(): Flux<R> { return new Flux<R>(this.source as unknown as Publisher<R>); }

    public switchIfEmpty(alternative: Publisher<T>): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let primarySub: Subscription = { request() {}, unsubscribe() {} };
                let altSub: Subscription | null = null;
                let hasValue = false;
                let cancelled = false;
                let demand = 0;

                const operatorSub: Subscription = {
                    request(n: number) {
                        if (cancelled) return;
                        if (n <= 0) { subscriber.onError(new Error(`request must be > 0, but was ${n}`)); return; }
                        demand = Math.min(demand + n, Number.MAX_SAFE_INTEGER);
                        if (altSub) altSub.request(n); else primarySub.request(n);
                    },
                    unsubscribe() { cancelled = true; primarySub.unsubscribe(); altSub?.unsubscribe(); }
                };

                this.source.subscribe({
                    onSubscribe(sourceSub: Subscription) {
                        primarySub = sourceSub;
                        subscriber.onSubscribe(operatorSub);
                    },
                    onNext(v) { hasValue = true; subscriber.onNext(v); },
                    onError(e) { subscriber.onError(e); },
                    onComplete() {
                        if (hasValue || cancelled) { subscriber.onComplete(); return; }
                        altSub = alternative.subscribe({
                            onSubscribe(altSourceSub: Subscription) {
                                altSub = altSourceSub;
                                if (demand > 0) altSub.request(demand);
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

    public onErrorReturn(replacement: Publisher<T>): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let primarySub: Subscription = { request() {}, unsubscribe() {} };
                let altSub: Subscription | null = null;
                let cancelled = false;
                let demand = 0;

                const operatorSub: Subscription = {
                    request(n: number) {
                        if (cancelled) return;
                        if (n <= 0) { subscriber.onError(new Error(`request must be > 0, but was ${n}`)); return; }
                        demand = Math.min(demand + n, Number.MAX_SAFE_INTEGER);
                        if (altSub) altSub.request(n); else primarySub.request(n);
                    },
                    unsubscribe() { cancelled = true; primarySub.unsubscribe(); altSub?.unsubscribe(); }
                };

                this.source.subscribe({
                    onSubscribe(sourceSub: Subscription) {
                        primarySub = sourceSub;
                        subscriber.onSubscribe(operatorSub);
                    },
                    onNext(v) { subscriber.onNext(v); },
                    onError(_e) {
                        if (cancelled) return;
                        altSub = replacement.subscribe({
                            onSubscribe(altSourceSub: Subscription) {
                                altSub = altSourceSub;
                                if (demand > 0) altSub.request(demand);
                            },
                            onNext(v) { subscriber.onNext(v); },
                            onError(e) { subscriber.onError(e); },
                            onComplete() { subscriber.onComplete(); }
                        });
                    },
                    onComplete() { subscriber.onComplete(); }
                });

                return operatorSub;
            }
        });
    }

    public onErrorContinue(predicate: (error: Error) => boolean): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) { subscriber.onNext(v); },
                    onError(e) { if (predicate(e)) subscriber.onComplete(); else subscriber.onError(e); },
                    onComplete() { subscriber.onComplete(); }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    public doFirst(fn: () => void): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let first = true;
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) { if (first) { first = false; try { fn(); } catch (_) {} } subscriber.onNext(v); },
                    onError(e) { subscriber.onError(e); },
                    onComplete() { subscriber.onComplete(); }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    public doOnNext(fn: (value: T) => void): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) { try { fn(v); } catch (_) {} subscriber.onNext(v); },
                    onError(e) { subscriber.onError(e); },
                    onComplete() { subscriber.onComplete(); }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    public doOnError(fn: (error: Error) => void): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) { subscriber.onNext(v); },
                    onError(e) { try { fn(e); } catch (_) {} subscriber.onError(e); },
                    onComplete() { subscriber.onComplete(); }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    public doFinally(fn: () => void): Flux<T> {
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

    public doOnSubscribe(fn: (subscription: Subscription) => void): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) { subscriber.onNext(v); },
                    onError(e) { subscriber.onError(e); },
                    onComplete() { subscriber.onComplete(); }
                });
                try { fn(sub); } catch (_) {}
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    public publishOn(scheduler: Scheduler): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) { scheduler.schedule(() => subscriber.onNext(v)); },
                    onError(e) { scheduler.schedule(() => subscriber.onError(e)); },
                    onComplete() { scheduler.schedule(() => subscriber.onComplete()); }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    public subscribeOn(scheduler: Scheduler): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let sub: Subscription | null = null;
                let pending = 0;
                let cancelled = false;

                const proxy: Subscription = {
                    request(n: number) {
                        if (cancelled) return;
                        if (n <= 0) { subscriber.onError(new Error(`request must be > 0, but was ${n}`)); return; }
                        if (sub) sub.request(n); else pending = Math.min(pending + n, Number.MAX_SAFE_INTEGER);
                    },
                    unsubscribe() { cancelled = true; sub?.unsubscribe(); }
                };

                subscriber.onSubscribe(proxy);

                scheduler.schedule(() => {
                    if (cancelled) return;
                    sub = this.source.subscribe({
                        onSubscribe(_s) {},
                        onNext(v) { subscriber.onNext(v); },
                        onError(e) { subscriber.onError(e); },
                        onComplete() { subscriber.onComplete(); }
                    });
                    if (pending > 0) sub.request(pending);
                });

                return proxy;
            }
        });
    }

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

    // ──────────────────────── Flux-specific operators ────────────────────────

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
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) { if (skipped++ < n) return; subscriber.onNext(v); },
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
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) {
                        if (skipping && predicate(v)) return;
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
                    onNext(v) { if (!gating) subscriber.onNext(v); },
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
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) { if (!seen.has(v)) { seen.add(v); subscriber.onNext(v); } },
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
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) {
                        if (first || comparator(prev as T, v)) {
                            first = false;
                            prev = v;
                            subscriber.onNext(v);
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

    public delayElements(ms: number): Flux<T> {
        return new Flux<T>({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) { setTimeout(() => subscriber.onNext(v), ms); },
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
                const done = () => { if (++doneCount === 2) subscriber.onComplete(); };

                let primarySub: Subscription = { request() {}, unsubscribe() {} };
                let otherSubRef: Subscription = { request() {}, unsubscribe() {} };

                const operatorSub: Subscription = {
                    request(n) {
                        if (cancelled) return;
                        if (n <= 0) { subscriber.onError(new Error(`request must be > 0, but was ${n}`)); return; }
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
                    onNext(v) { subscriber.onNext(v); },
                    onError(e) { subscriber.onError(e); },
                    onComplete() { done(); }
                });

                otherSubRef = other.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) { subscriber.onNext(v); },
                    onError(e) { subscriber.onError(e); },
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
