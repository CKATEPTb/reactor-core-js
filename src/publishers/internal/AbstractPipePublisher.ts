import {Publisher} from "@/publishers/Publisher";
import {Subscriber} from "@/subscriptions/Subscriber";
import {Subscription} from "@/subscriptions/Subscription";
import {Scheduler} from "@/schedulers/Scheduler";
import {Signal} from "@/publishers/Signal";

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

    // ──────────────────────── Error recovery ────────────────────────────────

    /**
     * Falls back to a publisher produced by `fn` when the source signals an error.
     *
     * Unlike {@link onErrorReturn} (which takes a static replacement), this operator
     * calls `fn` with the actual error, allowing dynamic recovery based on the error type.
     *
     * @param fn - Called with the error; returns the replacement publisher.
     * @returns A publisher that recovers from errors dynamically.
     *
     * @example
     * ```typescript
     * Flux.error(new Error('not found'))
     *   .onErrorResume(e => e.message === 'not found' ? Flux.just('default') : Flux.error(e))
     *   .subscribe(v => console.log(v)); // 'default'
     * ```
     */
    public onErrorResume(fn: (error: Error) => Publisher<T>): Self {
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
                    onSubscribe(s: Subscription) { primarySub = s; subscriber.onSubscribe(operatorSub); },
                    onNext(v: T) { subscriber.onNext(v); },
                    onError(e: Error) {
                        if (cancelled) return;
                        let alt: Publisher<T>;
                        try { alt = fn(e); }
                        catch (fnErr) { subscriber.onError(fnErr instanceof Error ? fnErr : new Error(String(fnErr))); return; }
                        altSub = alt.subscribe({
                            onSubscribe(s: Subscription) { altSub = s; if (demand > 0) s.request(demand); },
                            onNext(v: T) { subscriber.onNext(v); },
                            onError(e2: Error) { subscriber.onError(e2); },
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
     * Transforms the error signal using `fn` before forwarding it downstream.
     *
     * Useful for converting low-level errors into domain-specific error types.
     *
     * @param fn - Receives the original error and returns a replacement error.
     * @returns A publisher that transforms errors before propagating them.
     *
     * @example
     * ```typescript
     * Flux.error(new Error('HTTP 404'))
     *   .onErrorMap(e => new Error(`NotFound: ${e.message}`))
     *   .subscribe(null, e => console.error(e.message)); // NotFound: HTTP 404
     * ```
     */
    public onErrorMap(fn: (error: Error) => Error): Self {
        return this.wrapSource({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v: T) { subscriber.onNext(v); },
                    onError(e: Error) {
                        try { subscriber.onError(fn(e)); }
                        catch (fnErr) { subscriber.onError(fnErr instanceof Error ? fnErr : new Error(String(fnErr))); }
                    },
                    onComplete() { subscriber.onComplete(); }
                });
                subscriber.onSubscribe(sub);
                return sub;
            }
        });
    }

    // ──────────────────────── Timing ────────────────────────────────────────

    /**
     * Emits a `TimeoutError` (or switches to `fallback`) if no item is received
     * within `ms` milliseconds of the previous item (or of subscription).
     *
     * The timer resets on each received item, so it measures **inter-item** idle time.
     *
     * @param ms - Timeout duration in milliseconds.
     * @param fallback - Optional publisher to switch to on timeout instead of erroring.
     * @returns A publisher that errors or falls back when no item arrives in time.
     *
     * @example
     * ```typescript
     * Flux.never<number>()
     *   .timeout(100)
     *   .subscribe(null, e => console.error(e.message)); // TimeoutError after 100ms
     * ```
     */
    public timeout(ms: number, fallback?: Publisher<T>): Self {
        return this.wrapSource({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let sourceSub: Subscription = { request() {}, unsubscribe() {} };
                let altSub: Subscription | null = null;
                let cancelled = false;
                let sourceDone = false;
                let terminated = false;
                let demand = 0;
                let timer: ReturnType<typeof setTimeout> | null = null;

                const clearTimer = () => { if (timer !== null) { clearTimeout(timer); timer = null; } };

                const startTimer = () => {
                    clearTimer();
                    timer = setTimeout(() => {
                        if (cancelled || terminated) return;
                        sourceDone = true;
                        sourceSub.unsubscribe();
                        if (fallback) {
                            altSub = fallback.subscribe({
                                onSubscribe(s: Subscription) { altSub = s; if (demand > 0) s.request(demand); },
                                onNext(v: T) { if (!cancelled && !terminated) subscriber.onNext(v); },
                                onError(e: Error) { if (!cancelled && !terminated) { terminated = true; subscriber.onError(e); } },
                                onComplete() { if (!cancelled && !terminated) { terminated = true; subscriber.onComplete(); } }
                            });
                        } else {
                            terminated = true;
                            subscriber.onError(new Error(`TimeoutError: no item within ${ms}ms`));
                        }
                    }, ms);
                };

                const operatorSub: Subscription = {
                    request(n: number) {
                        if (cancelled || terminated) return;
                        if (n <= 0) { subscriber.onError(new Error(`request must be > 0, but was ${n}`)); return; }
                        demand = Math.min(demand + n, Number.MAX_SAFE_INTEGER);
                        if (altSub) altSub.request(n); else sourceSub.request(n);
                    },
                    unsubscribe() { cancelled = true; clearTimer(); sourceSub.unsubscribe(); altSub?.unsubscribe(); }
                };

                this.source.subscribe({
                    onSubscribe(s: Subscription) { sourceSub = s; subscriber.onSubscribe(operatorSub); startTimer(); },
                    onNext(v: T) {
                        if (cancelled || sourceDone) return;
                        startTimer();
                        subscriber.onNext(v);
                    },
                    onError(e: Error) {
                        clearTimer();
                        if (!cancelled && !terminated) { terminated = true; subscriber.onError(e); }
                    },
                    onComplete() {
                        clearTimer();
                        if (!cancelled && !terminated) { terminated = true; subscriber.onComplete(); }
                    }
                });

                return operatorSub;
            }
        });
    }

    /**
     * Delays the subscription to the upstream source by `ms` milliseconds.
     *
     * `onSubscribe` is delivered immediately with a proxy `Subscription`.
     * Any `request(n)` calls before the delay elapses are accumulated and
     * forwarded once the actual subscription starts.
     *
     * @param ms - Delay in milliseconds before subscribing to the source.
     * @returns A publisher whose upstream subscription is deferred.
     */
    public delaySubscription(ms: number): Self {
        return this.wrapSource({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                let sourceSub: Subscription | null = null;
                let cancelled = false;
                let demand = 0;

                const proxy: Subscription = {
                    request(n: number) {
                        if (cancelled) return;
                        if (n <= 0) { subscriber.onError(new Error(`request must be > 0, but was ${n}`)); return; }
                        demand = Math.min(demand + n, Number.MAX_SAFE_INTEGER);
                        if (sourceSub) sourceSub.request(n);
                    },
                    unsubscribe() { cancelled = true; sourceSub?.unsubscribe(); }
                };

                subscriber.onSubscribe(proxy);

                const timerId = setTimeout(() => {
                    if (cancelled) return;
                    this.source.subscribe({
                        onSubscribe(s: Subscription) {
                            sourceSub = s;
                            if (demand > 0) s.request(demand);
                        },
                        onNext(v: T) { if (!cancelled) subscriber.onNext(v); },
                        onError(e: Error) { if (!cancelled) subscriber.onError(e); },
                        onComplete() { if (!cancelled) subscriber.onComplete(); }
                    });
                }, ms);

                // Override unsubscribe to also clear the timer
                const originalUnsub = proxy.unsubscribe.bind(proxy);
                (proxy as { unsubscribe: () => void }).unsubscribe = () => { clearTimeout(timerId); originalUnsub(); };

                return proxy;
            }
        });
    }

    // ──────────────────────── Observability ─────────────────────────────────

    /**
     * Logs each reactive signal to the console for debugging.
     *
     * Prints `onSubscribe`, `request(n)`, `onNext(value)`, `onError`, `onComplete`,
     * and `cancel` with an optional label prefix.
     *
     * @param label - Optional label prepended to each log line (default: `'reactor'`).
     * @returns A publisher with logging side effects attached.
     *
     * @example
     * ```typescript
     * Flux.just(1, 2).log('source').subscribe(v => console.log(v));
     * // [source] onSubscribe
     * // [source] request(9007199254740991)
     * // [source] onNext(1)
     * // ...
     * ```
     */
    public log(label: string = 'reactor'): Self {
        return this.wrapSource({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const tag = `[${label}]`;
                let wrappedSub!: Subscription;
                this.source.subscribe({
                    onSubscribe(s: Subscription) {
                        console.log(`${tag} onSubscribe`);
                        wrappedSub = {
                            request(n: number) { console.log(`${tag} request(${n})`); s.request(n); },
                            unsubscribe() { console.log(`${tag} cancel`); s.unsubscribe(); }
                        };
                        subscriber.onSubscribe(wrappedSub);
                    },
                    onNext(v: T) {
                        try {
                            console.log(`${tag} onNext(${JSON.stringify(v)})`);
                        } catch (e) {
                            console.log(`${tag} onNext(`, v, ")");
                        }
                        subscriber.onNext(v);
                    },
                    onError(e: Error) { console.error(`${tag} onError: ${e.message}`); subscriber.onError(e); },
                    onComplete() { console.log(`${tag} onComplete`); subscriber.onComplete(); }
                });
                return wrappedSub;
            }
        });
    }

    /**
     * Executes a side-effect `fn` whenever the downstream subscriber issues a `request(n)`.
     *
     * Useful for debugging backpressure — see exactly how much demand is flowing upstream.
     * Any exception thrown by `fn` is silently swallowed.
     *
     * @param fn - Called with the requested count `n`.
     * @returns A publisher with the side effect attached to the request signal.
     */
    public doOnRequest(fn: (n: number) => void): Self {
        return this.wrapSource({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v: T) { subscriber.onNext(v); },
                    onError(e: Error) { subscriber.onError(e); },
                    onComplete() { subscriber.onComplete(); }
                });
                const wrappedSub: Subscription = {
                    request(n: number) { try { fn(n); } catch (_) {} sub.request(n); },
                    unsubscribe() { sub.unsubscribe(); }
                };
                subscriber.onSubscribe(wrappedSub);
                return wrappedSub;
            }
        });
    }

    /**
     * Executes a side-effect `fn` for every reactive signal (next, error, complete).
     *
     * The {@link Signal} discriminated union lets you inspect the kind and payload
     * of each event in a single callback.
     *
     * @param fn - Called for every signal with a {@link Signal} wrapper.
     * @returns A publisher with the side effect attached to all signals.
     *
     * @example
     * ```typescript
     * Flux.just(1, 2)
     *   .doOnEach(s => { if (s.kind === 'next') console.log('item:', s.value); })
     *   .subscribe();
     * ```
     */
    public doOnEach(fn: (signal: Signal<T>) => void): Self {
        return this.wrapSource({
            subscribe: (subscriber: Subscriber<T>): Subscription => {
                const sub = this.source.subscribe({
                    onSubscribe(_s) {},
                    onNext(v: T) { try { fn(Signal.next(v)); } catch (_) {} subscriber.onNext(v); },
                    onError(e: Error) { try { fn(Signal.error<T>(e)); } catch (_) {} subscriber.onError(e); },
                    onComplete() { try { fn(Signal.complete<T>()); } catch (_) {} subscriber.onComplete(); }
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
