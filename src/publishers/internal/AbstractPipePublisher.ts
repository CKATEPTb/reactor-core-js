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

    /**
     * Falls back to `alternative` if this source completes without emitting any items.
     *
     * If the source emits at least one item, `alternative` is never subscribed to.
     * If the source errors, the error is forwarded and `alternative` is not subscribed to.
     *
     * @param alternative - Publisher to subscribe to if the source is empty.
     * @returns A publisher that uses `alternative` when the source is empty.
     */
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

    /**
     * Substitutes the error signal with items from `replacement`.
     *
     * When the source signals `onError`, `replacement` is subscribed to and its items are
     * forwarded downstream as if they came from the original source.
     * If `replacement` itself errors, that error is forwarded.
     *
     * @param replacement - Publisher whose items replace the error.
     * @returns A publisher that recovers from errors by switching to `replacement`.
     */
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

    /**
     * Handles errors selectively: if `predicate(error)` returns `true`, the error is
     * swallowed and the stream completes normally; otherwise the error is forwarded.
     *
     * @param predicate - Receives the error and returns `true` to suppress it.
     * @returns A publisher that converts matching errors into normal completions.
     */
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

    /**
     * Executes a side-effect `fn` on the **first** item emitted by the source.
     *
     * Subsequent items pass through unchanged. Any exception thrown by `fn` is silently swallowed.
     *
     * @param fn - Side-effect to run on the very first item.
     * @returns A publisher with the side effect attached to the first item.
     */
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

    /**
     * Executes a side-effect `fn` for every item emitted by the source, before
     * forwarding the item downstream.
     *
     * Any exception thrown by `fn` is silently swallowed; the item is still forwarded.
     *
     * @param fn - Side-effect called for each item.
     * @returns A publisher with the side effect attached to each item.
     */
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

    /**
     * Executes a side-effect `fn` when the source signals an error, before
     * forwarding the error downstream.
     *
     * Any exception thrown by `fn` is silently swallowed; the original error is still forwarded.
     *
     * @param fn - Side-effect called with the error.
     * @returns A publisher with the side effect attached to the error signal.
     */
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

    /**
     * Executes a side-effect `fn` when the source calls `onSubscribe`, passing the
     * resulting {@link Subscription} to the callback.
     *
     * Any exception thrown by `fn` is silently swallowed.
     *
     * @param fn - Side-effect called with the subscription object.
     * @returns A publisher with the side effect attached to the subscribe event.
     */
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

    /**
     * Shifts item delivery to the given `scheduler`.
     *
     * Each `onNext`, `onError`, and `onComplete` callback is scheduled on `scheduler`
     * instead of being called inline. The source is still subscribed to on the
     * calling thread; only the *delivery* of signals is redirected.
     *
     * @param scheduler - The scheduler on which downstream signals are delivered.
     * @returns A publisher that delivers signals on the given scheduler.
     */
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

    /**
     * Shifts the *subscription* (and source execution) to the given `scheduler`.
     *
     * The actual call to `this.source.subscribe(...)` is deferred and run on `scheduler`.
     * Downstream `onSubscribe` is delivered immediately on the calling thread with a proxy
     * `Subscription` that buffers `request(n)` calls until the source subscription is ready.
     *
     * Use this for cold sources that do their work synchronously on subscribe — it moves
     * that work to a different thread/context.
     *
     * @param scheduler - The scheduler on which the source is subscribed to and runs.
     * @returns A publisher whose source execution runs on the given scheduler.
     */
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
