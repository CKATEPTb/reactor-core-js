/**
 * @packageDocumentation
 * Reactive Streams subscription and subscriber contracts.
 */
import {addCap, normalizeRequest} from "@/core/index.js";
import type {Context} from "@/context/index.js";
import {toAsyncIterator} from "@/internal/index.js";
import type {Flux} from "@/publishers/flux.js";
import type {Subscriber, Subscription} from "@/subscriptions/index.js";

/** Subscription implementation that drains a Flux only when demand is requested. */
export class IterableSubscription<T> implements Subscription {
    /** Abort controller used to cancel the active source iterator. */
    private readonly controller = new AbortController();
    /** Lazily created iterator over the backing Flux. */
    private iterator: AsyncIterator<T> | undefined;
    /** Tracks whether the source iterator has been assembled. */
    private started = false;
    /** Stores a synchronous source assembly failure until it can be signalled. */
    private startFailure: unknown;
    /** Tracks whether source assembly failed synchronously. */
    private hasStartFailure = false;
    /** Prefetched result used to detect completion with zero demand. */
    private pendingNext: Promise<IteratorResult<T>> | undefined;
    /** Outstanding downstream demand. */
    private requested = 0;
    /** Guards against concurrent drain loops. */
    private draining = false;
    /** Tracks downstream cancellation. */
    private cancelled = false;
    /** Tracks terminal completion or failure delivery. */
    private terminated = false;
    /** Source Flux drained by this subscription. */
    private readonly flux: Flux<T>;
    /** Downstream subscriber receiving signals. */
    private readonly subscriber: Subscriber<T>;
    /** Context used for the source iteration. */
    private readonly context: Context;

    /** Creates a subscription for the provided Flux and subscriber. */
    public constructor(flux: Flux<T>, subscriber: Subscriber<T>, context: Context) {
        this.flux = flux;
        this.subscriber = subscriber;
        this.context = context;
    }

    /** Starts source assembly without pulling any values from it. */
    public start(): void {
        if (this.started || this.cancelled || this.terminated) {
            return;
        }
        this.started = true;
        try {
            this.iterator = toAsyncIterator(this.flux.iterate(this.controller.signal, this.context));
        } catch (error) {
            this.startFailure = error;
            this.hasStartFailure = true;
        }
    }

    /** Delivers a synchronous source assembly failure after `onSubscribe`. */
    public deliverStartFailure(): void {
        if (!this.hasStartFailure || this.cancelled || this.terminated) {
            return;
        }
        this.hasStartFailure = false;
        this.terminated = true;
        this.subscriber.onError(this.startFailure);
    }

    /** Requests more values from the backing iterator. */
    public request(n: number): void {
        if (this.cancelled || this.terminated) {
            return;
        }
        let request: number;
        try {
            request = normalizeRequest(n);
        } catch (error) {
            this.cancel();
            this.subscriber.onError(error);
            return;
        }
        this.requested = addCap(this.requested, request);
        if (this.hasStartFailure) {
            this.deliverStartFailure();
            return;
        }
        void this.drain();
    }

    /** Cancels iteration and closes upstream resources. */
    public cancel(): void {
        if (this.cancelled || this.terminated) {
            return;
        }
        this.cancelled = true;
        this.controller.abort();
        void this.iterator?.return?.();
    }

    /** Drains the source while there is outstanding demand. */
    private async drain(): Promise<void> {
        if (this.draining) {
            return;
        }
        this.draining = true;
        try {
            this.start();
            if (this.hasStartFailure) {
                this.deliverStartFailure();
                return;
            }
            while (!this.cancelled && !this.terminated && this.requested > 0) {
                const result = await this.nextResult();
                if (this.cancelled || this.terminated) {
                    return;
                }
                if (result.done) {
                    this.terminated = true;
                    this.subscriber.onComplete();
                    return;
                }
                if (this.requested !== Number.POSITIVE_INFINITY) {
                    this.requested -= 1;
                }
                try {
                    this.subscriber.onNext(result.value);
                } catch (error) {
                    this.cancel();
                    this.subscriber.onError(error);
                    return;
                }
            }
        } catch (error) {
            if (!this.cancelled && !this.terminated) {
                this.terminated = true;
                this.subscriber.onError(error);
            }
        } finally {
            this.draining = false;
            if (!this.cancelled && !this.terminated && this.requested > 0) {
                queueMicrotask(() => {
                    void this.drain();
                });
            } else if (!this.cancelled && !this.terminated && this.requested === 0) {
                this.lookAheadForTermination();
            }
        }
    }

    /** Returns the pending prefetched result or asks the iterator for the next item. */
    private nextResult(): Promise<IteratorResult<T>> {
        if (this.pendingNext) {
            const pending = this.pendingNext;
            this.pendingNext = undefined;
            return pending;
        }
        return this.iterator?.next() ?? Promise.resolve({done: true, value: undefined as T});
    }

    /** Prefetches one item to discover terminal completion without extra demand. */
    private lookAheadForTermination(): void {
        if (!this.iterator || this.pendingNext) {
            return;
        }
        const pending = this.iterator.next();
        this.pendingNext = pending;
        pending.then(
            result => {
                if (this.pendingNext !== pending || this.cancelled || this.terminated) {
                    return;
                }
                if (result.done) {
                    this.pendingNext = undefined;
                    this.terminated = true;
                    this.subscriber.onComplete();
                }
            },
            error => {
                if (this.pendingNext !== pending || this.cancelled || this.terminated) {
                    return;
                }
                this.pendingNext = undefined;
                this.terminated = true;
                this.subscriber.onError(error);
            }
        );
    }
}
