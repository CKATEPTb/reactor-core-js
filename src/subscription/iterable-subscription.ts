/**
 * @packageDocumentation
 * Reactive Streams subscription and subscriber contracts.
 */
import {addCap, normalizeRequest, UNBOUNDED_DEMAND} from "@/core/demand.js";
import type {Context} from "@/context/context.js";
import {doneResult, isAsyncIterable, resolvedDoneResult} from "@/internal/iterable.js";
import {applyIterationDemandHint, markUnboundedIteration} from "@/internal/iteration-demand.js";
import {scheduleMicrotask} from "@/internal/microtask.js";
import type {Flux} from "@/publisher/flux.js";
import type {Subscriber} from "@/subscription/subscriber.js";
import type {Subscription} from "@/subscription/subscription.js";

/** Subscription implementation that drains a Flux only when demand is requested. */
export class IterableSubscription<T> implements Subscription {
    /** Abort controller used to cancel the active source iterator. */
    private readonly controller = new AbortController();
    /** Lazily created async iterator over the backing Flux. */
    private asyncIterator: AsyncIterator<T> | undefined;
    /** Lazily created synchronous iterator over the backing Flux. */
    private syncIterator: Iterator<T> | undefined;
    /** Tracks whether the source iterator has been assembled. */
    private started = false;
    /** Stores a synchronous source assembly failure until it can be signalled. */
    private startFailure: unknown;
    /** Tracks whether source assembly failed synchronously. */
    private hasStartFailure = false;
    /** Prefetched async result used to detect completion with zero demand. */
    private pendingAsyncNext: Promise<IteratorResult<T>> | undefined;
    /** Prefetched sync result used to detect completion with zero demand. */
    private pendingSyncNext: IteratorResult<T> | undefined;
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
        applyIterationDemandHint(this.controller.signal, subscriber);
    }

    /** Starts source assembly without pulling any values from it. */
    public start(): void {
        if (this.started || this.cancelled || this.terminated) {
            return;
        }
        this.started = true;
        try {
            const source = this.flux.iterate(this.controller.signal, this.context);
            if (isAsyncIterable<T>(source)) {
                this.asyncIterator = source[Symbol.asyncIterator]();
            } else {
                this.syncIterator = source[Symbol.iterator]();
            }
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
        if (request === UNBOUNDED_DEMAND) {
            markUnboundedIteration(this.controller.signal);
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
        this.closeIterators();
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
            if (this.syncIterator) {
                this.drainSync();
                return;
            }
            while (!this.cancelled && !this.terminated && this.requested > 0) {
                const result = await this.nextAsyncResult();
                if (this.cancelled || this.terminated) {
                    return;
                }
                if (this.deliverResult(result)) {
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
                scheduleMicrotask(() => {
                    void this.drain();
                });
            } else if (!this.cancelled && !this.terminated && this.requested === 0) {
                this.lookAheadForTermination();
            }
        }
    }

    /** Drains a synchronous source without per-item promise allocation. */
    private drainSync(): void {
        while (!this.cancelled && !this.terminated && this.requested > 0) {
            const result = this.nextSyncResult();
            if (this.cancelled || this.terminated) {
                return;
            }
            if (this.deliverResult(result)) {
                return;
            }
        }
    }

    /** Returns true after delivering a terminal result. */
    private deliverResult(result: IteratorResult<T>): boolean {
        if (result.done) {
            this.terminated = true;
            this.subscriber.onComplete();
            return true;
        }
        if (this.requested !== UNBOUNDED_DEMAND) {
            this.requested -= 1;
        }
        try {
            this.subscriber.onNext(result.value);
        } catch (error) {
            this.cancel();
            this.subscriber.onError(error);
            return true;
        }
        return false;
    }

    /** Returns the pending prefetched async result or asks the async iterator for the next item. */
    private nextAsyncResult(): Promise<IteratorResult<T>> {
        if (this.pendingAsyncNext) {
            const pending = this.pendingAsyncNext;
            this.pendingAsyncNext = undefined;
            return pending;
        }
        return this.asyncIterator?.next() ?? resolvedDoneResult<T>();
    }

    /** Returns the pending prefetched sync result or asks the sync iterator for the next item. */
    private nextSyncResult(): IteratorResult<T> {
        if (this.pendingSyncNext) {
            const pending = this.pendingSyncNext;
            this.pendingSyncNext = undefined;
            return pending;
        }
        return this.syncIterator?.next() ?? doneResult<T>();
    }

    /** Prefetches one item to discover terminal completion without extra demand. */
    private lookAheadForTermination(): void {
        if (this.syncIterator) {
            this.lookAheadForSyncTermination();
            return;
        }
        if (!this.asyncIterator || this.pendingAsyncNext) {
            return;
        }
        const pending = this.asyncIterator.next();
        this.pendingAsyncNext = pending;
        pending.then(
            result => {
                if (this.pendingAsyncNext !== pending || this.cancelled || this.terminated) {
                    return;
                }
                if (result.done) {
                    this.pendingAsyncNext = undefined;
                    this.terminated = true;
                    this.subscriber.onComplete();
                }
            },
            error => {
                if (this.pendingAsyncNext !== pending || this.cancelled || this.terminated) {
                    return;
                }
                this.pendingAsyncNext = undefined;
                this.terminated = true;
                this.subscriber.onError(error);
            }
        );
    }

    /** Prefetches one synchronous item to discover terminal completion without extra demand. */
    private lookAheadForSyncTermination(): void {
        if (!this.syncIterator || this.pendingSyncNext) {
            return;
        }
        try {
            const result = this.syncIterator.next();
            if (this.cancelled || this.terminated) {
                return;
            }
            if (result.done) {
                this.terminated = true;
                this.subscriber.onComplete();
            } else {
                this.pendingSyncNext = result;
            }
        } catch (error) {
            if (!this.cancelled && !this.terminated) {
                this.terminated = true;
                this.subscriber.onError(error);
            }
        }
    }

    /** Closes active iterators and ignores cleanup failures after cancellation. */
    private closeIterators(): void {
        try {
            void this.asyncIterator?.return?.();
        } catch {
            // cancellation best-effort cleanup
        }
        try {
            this.syncIterator?.return?.();
        } catch {
            // cancellation best-effort cleanup
        }
    }
}
