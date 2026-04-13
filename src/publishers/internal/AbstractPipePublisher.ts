import {Publisher} from "@/publishers/Publisher";
import {Subscriber} from "@/subscriptions/Subscriber";
import {Subscription} from "@/subscriptions/Subscription";
import {Scheduler} from "@/schedulers/Scheduler";

/**
 * Base class for Flux and Mono providing the common operator implementations
 * that are identical across both types. Operators that preserve T use the
 * abstract `wrapSource` factory so that subclasses always return their own
 * concrete type (Flux → Flux, Mono → Mono) without any unsafe casts at
 * call-sites.
 *
 * Operators NOT included here (different semantics or change the type):
 *  - filter / filterWhen  — demand-replenishment differs between Flux and Mono
 *  - doFinally            — Flux also fires fn on cancel; Mono does not
 *  - map / flatMap / cast / pipe / mapNotNull — change the type T → R
 */
export abstract class AbstractPipePublisher<T, Self> implements Publisher<T> {

    protected constructor(protected readonly source: Publisher<T>) {}

    /**
     * Default demand issued in the auto-request onSubscribe when using the
     * convenience subscribe() overloads (no explicit Subscriber provided).
     * Mono overrides this to 1; Flux overrides it to Number.MAX_SAFE_INTEGER.
     */
    protected abstract defaultDemand(): number;

    /**
     * Subscribe with a full Subscriber (existing contract — full demand control).
     */
    subscribe(subscriber: Subscriber<T>): Subscription;

    /**
     * Convenience overload — any combination of callbacks may be omitted.
     * An auto-request of `defaultDemand()` is issued on subscribe so items
     * start flowing immediately without a manual request() call.
     *
     * If `onError` is not provided, unhandled errors are re-thrown so they
     * surface as uncaught exceptions rather than being silently swallowed.
     *
     * @example
     * // Mono — requests 1 automatically
     * Mono.just(42).subscribe(v => console.log(v));
     *
     * // Flux — requests unbounded automatically
     * Flux.range(0, 5).subscribe(v => console.log(v), err => console.error(err));
     */
    subscribe(onNext?: (value: T) => void, onError?: (error: Error) => void, onComplete?: () => void): Subscription;

    subscribe(
        subscriberOrOnNext?: Subscriber<T> | ((value: T) => void),
        onError?: (error: Error) => void,
        onComplete?: () => void,
    ): Subscription {
        // Full Subscriber object — delegate unchanged (caller owns demand).
        if (subscriberOrOnNext !== undefined && typeof subscriberOrOnNext === 'object') {
            return this.source.subscribe(subscriberOrOnNext);
        }

        // Convenience path: wrap callbacks in a LambdaSubscriber that auto-requests.
        const onNext = subscriberOrOnNext as ((value: T) => void) | undefined;
        const demand = this.defaultDemand();
        return this.source.subscribe({
            onSubscribe(s) { s.request(demand); },
            onNext:     onNext   ?? (() => {}),
            onError:    onError  ?? ((e) => { throw e; }),
            onComplete: onComplete ?? (() => {}),
        });
    }

    /**
     * Wraps a publisher in the concrete subclass type.
     * Called internally by every operator that preserves T.
     */
    protected abstract wrapSource(source: Publisher<T>): Self;

    // ──────────────────────── Error handling ────────────────────────────────

    public switchIfEmpty(alternative: Publisher<T>): Self {
        return this.wrapSource({
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

    public onErrorReturn(replacement: Publisher<T>): Self {
        return this.wrapSource({
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

    public onErrorContinue(predicate: (error: Error) => boolean): Self {
        return this.wrapSource({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) { subscriber.onNext(v); },
                    onError(e) {
                        if (predicate(e)) subscriber.onComplete();
                        else subscriber.onError(e);
                    },
                    onComplete() { subscriber.onComplete(); }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    // ──────────────────────── Side effects ──────────────────────────────────

    public doFirst(fn: () => void): Self {
        return this.wrapSource({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let first = true;
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v) {
                        if (first) { first = false; try { fn(); } catch (_) {} }
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

    public doOnNext(fn: (value: T) => void): Self {
        return this.wrapSource({
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

    public doOnError(fn: (error: Error) => void): Self {
        return this.wrapSource({
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

    public doOnSubscribe(fn: (subscription: Subscription) => void): Self {
        return this.wrapSource({
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

    // ──────────────────────── Scheduling ────────────────────────────────────

    public publishOn(scheduler: Scheduler): Self {
        return this.wrapSource({
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

    public subscribeOn(scheduler: Scheduler): Self {
        return this.wrapSource({
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
}
