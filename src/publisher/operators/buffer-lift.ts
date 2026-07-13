/**
 * @packageDocumentation
 * Demand-aware buffering subscriber used by fixed-size Flux buffers.
 */
import {Context} from "@/context/context.js";
import {addCap, multiplyCap, normalizeRequest, UNBOUNDED_DEMAND} from "@/core/demand.js";
import {ITERATION_DEMAND_HINT, iterationDemandHint} from "@/internal/iteration-demand.js";
import {Flux} from "@/publisher/flux.js";
import {liftFlux, subscriberContext} from "@/publisher/operators/lift.js";
import type {SourceFactory} from "@/publisher/types.js";
import type {CoreSubscriber} from "@/subscription/core-subscriber.js";
import type {Subscriber} from "@/subscription/subscriber.js";
import type {Subscription} from "@/subscription/subscription.js";

/** Creates a fixed-size buffer operator that scales downstream demand upstream. */
export function liftBuffer<T>(source: Flux<T>, sourceFactory: SourceFactory<T[]>, size: number): Flux<T[]> {
    return liftFlux(source, sourceFactory, subscriber => new BufferOperatorSubscriber(subscriber, size));
}

/** Subscriber that turns each requested buffer into `size` requested source values. */
class BufferOperatorSubscriber<T> implements CoreSubscriber<T>, Subscription {
    /** Values accumulated for the next downstream buffer. */
    private buffer: T[] = [];
    /** Active upstream subscription. */
    private upstream: Subscription | undefined;
    /** Outstanding downstream buffer demand. */
    private requested = 0;
    /** Guards synchronous upstream requests against reentrant downstream demand. */
    private requesting = false;
    /** Source demand accumulated while an upstream request call is active. */
    private missedRequested = 0;
    /** Prevents duplicate subscriptions from replacing the active upstream. */
    private subscribed = false;
    /** Guards final-buffer delivery against reentrant upstream signals. */
    private completing = false;
    /** Suppresses signals after cancellation or termination. */
    private terminated = false;

    /** Creates a buffering subscriber for one downstream subscriber. */
    public constructor(private readonly actual: Subscriber<T[]>, private readonly size: number) {
    }

    /** Stores upstream and exposes this buffer-aware subscription downstream. */
    public onSubscribe(subscription: Subscription): void {
        if (this.subscribed || this.terminated) {
            cancelSilently(subscription);
            return;
        }
        this.subscribed = true;
        this.upstream = subscription;
        try {
            this.actual.onSubscribe(this);
        } catch (error) {
            this.terminated = true;
            cancelSilently(subscription);
            throw error;
        }
    }

    /** Adds one source value and emits a full buffer when it reaches the configured size. */
    public onNext(value: T): void {
        if (this.terminated || this.completing) {
            return;
        }
        this.buffer.push(value);
        if (this.buffer.length !== this.size) {
            return;
        }
        const next = this.buffer;
        this.buffer = [];
        this.producedOne();
        try {
            this.actual.onNext(next);
        } catch (error) {
            this.fail(error, true);
        }
    }

    /** Discards the partial buffer and forwards the first source failure. */
    public onError(error: unknown): void {
        this.buffer = [];
        this.fail(error, false);
    }

    /** Emits a final partial buffer and then completes downstream. */
    public onComplete(): void {
        if (this.terminated || this.completing) {
            return;
        }
        const finalBuffer = this.buffer;
        this.buffer = [];
        if (finalBuffer.length > 0) {
            this.producedOne();
            this.completing = true;
            try {
                this.actual.onNext(finalBuffer);
            } catch (error) {
                this.completing = false;
                if (this.terminated) {
                    return;
                }
                this.terminated = true;
                this.actual.onError(error);
                return;
            }
            this.completing = false;
            if (this.terminated) {
                return;
            }
        }
        this.terminated = true;
        this.actual.onComplete();
    }

    /** Multiplies valid downstream buffer demand before requesting the source. */
    public request(n: number): void {
        if (this.terminated || this.completing) {
            return;
        }
        let demand: number;
        try {
            demand = normalizeRequest(n);
        } catch (error) {
            this.fail(error, true);
            return;
        }
        this.requested = addCap(this.requested, demand);
        this.requestUpstream(multiplyCap(demand, this.size));
    }

    /** Cancels upstream and releases buffered values. */
    public cancel(): void {
        if (this.terminated) {
            return;
        }
        this.terminated = true;
        this.buffer = [];
        this.upstream?.cancel();
    }

    /** Exposes downstream context unchanged. */
    public currentContext(): Context {
        return subscriberContext(this.actual);
    }

    /** Propagates a known terminal demand batch through the buffer boundary. */
    public [ITERATION_DEMAND_HINT](): number | undefined {
        const downstream = iterationDemandHint(this.actual);
        return downstream === undefined ? undefined : multiplyCap(downstream, this.size);
    }

    /** Decrements finite downstream demand after emitting one buffer. */
    private producedOne(): void {
        if (this.requested !== UNBOUNDED_DEMAND && this.requested > 0) {
            this.requested -= 1;
        }
    }

    /** Requests source demand iteratively to avoid recursive synchronous request calls. */
    private requestUpstream(demand: number): void {
        if (this.requesting) {
            this.missedRequested = addCap(this.missedRequested, demand);
            return;
        }
        this.requesting = true;
        let request = demand;
        try {
            while (!this.terminated) {
                this.upstream?.request(request);
                if (this.terminated || this.missedRequested === 0) {
                    return;
                }
                request = this.missedRequested;
                this.missedRequested = 0;
            }
        } catch (error) {
            if (!this.terminated) {
                this.fail(error, true);
            }
        } finally {
            this.requesting = false;
        }
    }

    /** Cancels when requested and forwards one terminal failure. */
    private fail(error: unknown, cancelUpstream: boolean): void {
        if (this.terminated || this.completing) {
            return;
        }
        this.terminated = true;
        this.buffer = [];
        if (cancelUpstream && this.upstream) {
            cancelSilently(this.upstream);
        }
        this.actual.onError(error);
    }
}

/** Cancels upstream without replacing the active failure or completion signal. */
function cancelSilently(subscription: Subscription): void {
    try {
        subscription.cancel();
    } catch {
        // The selected terminal signal wins over cancellation failures.
    }
}
