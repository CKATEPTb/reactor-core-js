/**
 * @packageDocumentation
 * Internal subscriber-lifting primitives for demand-preserving Flux operators.
 */
import {Context} from "@/context/context.js";
import {addCap, normalizeRequest, UNBOUNDED_DEMAND} from "@/core/demand.js";
import {ITERATION_DEMAND_HINT, iterationDemandHint} from "@/internal/iteration-demand.js";
import type {SourceFactory} from "@/publisher/types.js";
import {Flux} from "@/publisher/flux.js";
import type {CoreSubscriber} from "@/subscription/core-subscriber.js";
import type {Subscriber} from "@/subscription/subscriber.js";
import type {Subscription} from "@/subscription/subscription.js";

/** Creates the subscriber that an upstream Flux sees for a downstream subscriber. */
export type SubscriberLifter<T, R> = (subscriber: Subscriber<R>) => Subscriber<T>;

/** Hooks used by synchronous one-input/one-output operators. */
export interface OneToOneOperatorHooks<T, R> {
    /** Converts one upstream value into exactly one downstream value. */
    onNext(value: T): R;

    /** Runs before the downstream receives its subscription. */
    onSubscribe?(subscription: Subscription): void;

    /** Runs before an upstream error is forwarded. */
    onError?(error: unknown): void;

    /** Runs before downstream demand is forwarded upstream. */
    onRequest?(request: number): void;

    /** Runs before successful completion is forwarded. */
    onComplete?(): void;

    /** Runs when downstream cancellation is propagated upstream. */
    onCancel?(): void;

    /** Runs after the terminal signal or cancellation has been propagated. */
    onFinally?(signal: "complete" | "error" | "cancel"): void;

    /** Context exposed to the upstream subscriber. */
    currentContext?(): Context;
}

/**
 * Lifts a Flux at the Reactive Streams subscriber boundary while keeping an
 * iterable implementation for explicit async-iterator consumption.
 */
export function liftFlux<T, R>(
    source: Flux<T>,
    sourceFactory: SourceFactory<R>,
    lifter: SubscriberLifter<T, R>
): Flux<R> {
    return new LiftedFlux(source, sourceFactory, lifter);
}

/** Creates a one-to-one operator that forwards downstream demand unchanged. */
export function liftOneToOne<T, R>(
    source: Flux<T>,
    sourceFactory: SourceFactory<R>,
    hooksFactory: (subscriber: Subscriber<R>) => OneToOneOperatorHooks<T, R>
): Flux<R> {
    return liftFlux(source, sourceFactory, subscriber =>
        new OneToOneOperatorSubscriber(subscriber, hooksFactory(subscriber))
    );
}

/** Creates a filtering operator that replenishes only values dropped from finite demand. */
export function liftFilter<T>(
    source: Flux<T>,
    sourceFactory: SourceFactory<T>,
    predicate: (value: T) => boolean
): Flux<T> {
    return liftFlux(source, sourceFactory, subscriber => new FilterOperatorSubscriber(subscriber, predicate));
}

/** Creates a limiting operator that never requests more than its remaining item count. */
export function liftTake<T>(source: Flux<T>, sourceFactory: SourceFactory<T>, count: number): Flux<T> {
    return liftFlux(source, sourceFactory, subscriber => new TakeOperatorSubscriber(subscriber, count));
}

/** Returns the context exposed by a subscriber, or the shared empty context. */
export function subscriberContext<T>(subscriber: Subscriber<T>): Context {
    return (subscriber as CoreSubscriber<T>).currentContext?.() ?? Context.empty();
}

/** Flux whose normal subscribe path is assembled through a subscriber lifter. */
class LiftedFlux<T, R> extends Flux<R> {
    /** Creates a lifted Flux around an upstream source. */
    public constructor(
        private readonly source: Flux<T>,
        sourceFactory: SourceFactory<R>,
        private readonly lifter: SubscriberLifter<T, R>
    ) {
        super(sourceFactory);
    }

    /** Subscribes without crossing the async-iterator bridge. */
    protected override subscribeActual(subscriber: Subscriber<R>): void {
        let upstreamSubscriber: Subscriber<T>;
        try {
            upstreamSubscriber = this.lifter(subscriber);
        } catch (error) {
            signalAssemblyError(subscriber, error);
            return;
        }
        this.source.subscribe(upstreamSubscriber);
    }
}

/** Subscription/subscriber pair shared by synchronous one-to-one operators. */
class OneToOneOperatorSubscriber<T, R> implements CoreSubscriber<T>, Subscription {
    /** Upstream subscription received during the Reactive Streams handshake. */
    private upstream: Subscription | undefined;
    /** Prevents duplicate subscriptions from replacing the active upstream. */
    private subscribed = false;
    /** Suppresses signals and demand after cancellation or termination. */
    private terminated = false;

    /** Creates a one-to-one operator subscriber. */
    public constructor(
        private readonly actual: Subscriber<R>,
        private readonly hooks: OneToOneOperatorHooks<T, R>
    ) {
    }

    /** Installs the upstream subscription and exposes this forwarding subscription downstream. */
    public onSubscribe(subscription: Subscription): void {
        if (this.subscribed || this.terminated) {
            subscription.cancel();
            return;
        }
        this.subscribed = true;
        this.upstream = subscription;

        try {
            this.hooks.onSubscribe?.(subscription);
        } catch (error) {
            this.terminated = true;
            cancelSilently(subscription);
            this.actual.onSubscribe(this);
            this.signalError(error);
            return;
        }

        try {
            this.actual.onSubscribe(this);
        } catch (error) {
            this.terminated = true;
            try {
                subscription.cancel();
            } finally {
                this.runFinally("cancel");
            }
            throw error;
        }
    }

    /** Converts and forwards one upstream value without changing demand. */
    public onNext(value: T): void {
        if (this.terminated) {
            return;
        }
        let mapped: R;
        try {
            mapped = this.hooks.onNext(value);
        } catch (error) {
            this.terminateWithError(error, true);
            return;
        }
        if (this.terminated) {
            return;
        }
        this.actual.onNext(mapped);
    }

    /** Forwards an upstream error after running the operator callback. */
    public onError(error: unknown): void {
        this.terminateWithError(error, false);
    }

    /** Forwards successful completion after running the operator callback. */
    public onComplete(): void {
        if (this.terminated) {
            return;
        }
        try {
            this.hooks.onComplete?.();
        } catch (error) {
            this.terminateWithError(error, false);
            return;
        }
        if (this.terminated) {
            return;
        }
        this.terminated = true;
        try {
            this.actual.onComplete();
        } finally {
            this.runFinally("complete");
        }
    }

    /** Forwards exactly the requested amount to upstream. */
    public request(n: number): void {
        if (this.terminated) {
            return;
        }
        try {
            this.hooks.onRequest?.(n);
        } catch (error) {
            this.terminateWithError(error, true);
            return;
        }
        if (this.terminated) {
            return;
        }
        this.upstream?.request(n);
    }

    /** Cancels upstream once and reports the cancellation lifecycle hooks. */
    public cancel(): void {
        if (this.terminated) {
            return;
        }
        this.terminated = true;
        try {
            this.upstream?.cancel();
        } finally {
            try {
                this.hooks.onCancel?.();
            } finally {
                this.runFinally("cancel");
            }
        }
    }

    /** Exposes downstream context to context-aware upstream publishers. */
    public currentContext(): Context {
        return this.hooks.currentContext?.() ?? subscriberContext(this.actual);
    }

    /** Propagates a known terminal demand batch through this one-to-one boundary. */
    public [ITERATION_DEMAND_HINT](): number | undefined {
        return iterationDemandHint(this.actual);
    }

    /** Cancels when needed and emits one downstream error signal. */
    private terminateWithError(error: unknown, cancelUpstream: boolean): void {
        if (this.terminated) {
            return;
        }
        this.terminated = true;
        if (cancelUpstream && this.upstream) {
            cancelSilently(this.upstream);
        }
        let failure = error;
        try {
            this.hooks.onError?.(error);
        } catch (callbackError) {
            failure = callbackError;
        }
        this.signalError(failure);
    }

    /** Delivers an error followed by the final error callback. */
    private signalError(error: unknown): void {
        try {
            this.actual.onError(error);
        } finally {
            this.runFinally("error");
        }
    }

    /** Runs a final callback without letting observer code alter terminal semantics. */
    private runFinally(signal: "complete" | "error" | "cancel"): void {
        try {
            this.hooks.onFinally?.(signal);
        } catch {
            // Final observers run after propagation and cannot replace the terminal signal.
        }
    }
}

/** Filtering subscriber with stack-safe compensation for dropped values. */
class FilterOperatorSubscriber<T> implements CoreSubscriber<T>, Subscription {
    /** Upstream subscription received during the Reactive Streams handshake. */
    private upstream: Subscription | undefined;
    /** Prevents duplicate subscriptions from replacing the active upstream. */
    private subscribed = false;
    /** Suppresses signals and demand after cancellation or termination. */
    private terminated = false;
    /** Avoids compensation requests after downstream switches to unbounded demand. */
    private unbounded = false;
    /** Guards synchronous request calls against reentrant compensation. */
    private requesting = false;
    /** Compensation or downstream demand accumulated during an active request call. */
    private missedRequested = 0;

    /** Creates a filtering subscriber. */
    public constructor(
        private readonly actual: Subscriber<T>,
        private readonly predicate: (value: T) => boolean
    ) {
    }

    /** Installs upstream and exposes the demand-forwarding subscription downstream. */
    public onSubscribe(subscription: Subscription): void {
        if (this.subscribed || this.terminated) {
            subscription.cancel();
            return;
        }
        this.subscribed = true;
        this.upstream = subscription;
        try {
            this.actual.onSubscribe(this);
        } catch (error) {
            this.terminated = true;
            subscription.cancel();
            throw error;
        }
    }

    /** Tests one value, forwarding matches and replenishing finite demand for drops. */
    public onNext(value: T): void {
        if (this.terminated) {
            return;
        }
        let accepted: boolean;
        try {
            accepted = this.predicate(value);
        } catch (error) {
            this.terminated = true;
            if (this.upstream) {
                cancelSilently(this.upstream);
            }
            this.actual.onError(error);
            return;
        }
        if (accepted) {
            this.actual.onNext(value);
        } else if (!this.unbounded) {
            this.requestUpstream(1);
        }
    }

    /** Forwards one terminal error. */
    public onError(error: unknown): void {
        if (this.terminated) {
            return;
        }
        this.terminated = true;
        this.actual.onError(error);
    }

    /** Forwards successful completion once. */
    public onComplete(): void {
        if (this.terminated) {
            return;
        }
        this.terminated = true;
        this.actual.onComplete();
    }

    /** Forwards demand and records when no further compensation is necessary. */
    public request(n: number): void {
        if (this.terminated) {
            return;
        }
        if (n === UNBOUNDED_DEMAND) {
            this.unbounded = true;
        }
        this.requestUpstream(n);
    }

    /** Cancels the upstream once. */
    public cancel(): void {
        if (this.terminated) {
            return;
        }
        this.terminated = true;
        this.upstream?.cancel();
    }

    /** Exposes the downstream context unchanged. */
    public currentContext(): Context {
        return subscriberContext(this.actual);
    }

    /** Propagates a known unbounded terminal batch through this filtering boundary. */
    public [ITERATION_DEMAND_HINT](): number | undefined {
        return iterationDemandHint(this.actual);
    }

    /** Drains demand iteratively so synchronous sources cannot recurse through dropped values. */
    private requestUpstream(n: number): void {
        if (this.requesting) {
            this.missedRequested = addCap(this.missedRequested, n);
            return;
        }
        const upstream = this.upstream;
        if (!upstream) {
            return;
        }
        this.requesting = true;
        let request = n;
        try {
            while (!this.terminated) {
                upstream.request(request);
                if (this.terminated || this.missedRequested === 0) {
                    return;
                }
                request = this.missedRequested;
                this.missedRequested = 0;
            }
        } finally {
            this.requesting = false;
        }
    }
}

/** Subscriber that caps cumulative upstream demand and completes after a fixed number of values. */
class TakeOperatorSubscriber<T> implements CoreSubscriber<T>, Subscription {
    /** Active upstream subscription. */
    private upstream: Subscription | undefined;
    /** Remaining values before this operator completes. */
    private remaining: number;
    /** Requested upstream values that have not arrived yet. */
    private outstanding = 0;
    /** Guards synchronous upstream request calls against recursive downstream demand. */
    private requesting = false;
    /** Downstream demand accumulated while an upstream request is active. */
    private missedRequested = 0;
    /** Prevents duplicate subscriptions from replacing the active upstream. */
    private subscribed = false;
    /** Suppresses signals after cancellation or termination. */
    private terminated = false;

    /** Creates a take subscriber with the provided maximum value count. */
    public constructor(private readonly actual: Subscriber<T>, count: number) {
        this.remaining = count;
    }

    /** Stores upstream and exposes the capped subscription downstream. */
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

    /** Forwards one value and cancels upstream exactly at the configured limit. */
    public onNext(value: T): void {
        if (this.terminated || this.remaining === 0) {
            return;
        }
        if (this.outstanding > 0) {
            this.outstanding -= 1;
        }
        this.remaining -= 1;
        try {
            this.actual.onNext(value);
        } catch (error) {
            if (this.terminated) {
                return;
            }
            this.terminated = true;
            if (this.upstream) {
                cancelSilently(this.upstream);
            }
            this.actual.onError(error);
            return;
        }
        if (this.remaining === 0 && !this.terminated) {
            this.terminated = true;
            if (this.upstream) {
                cancelSilently(this.upstream);
            }
            this.actual.onComplete();
        }
    }

    /** Forwards the first upstream failure. */
    public onError(error: unknown): void {
        if (this.terminated) {
            return;
        }
        this.terminated = true;
        this.actual.onError(error);
    }

    /** Forwards early upstream completion. */
    public onComplete(): void {
        if (this.terminated) {
            return;
        }
        this.terminated = true;
        this.actual.onComplete();
    }

    /** Requests only demand that still fits below the take limit. */
    public request(n: number): void {
        if (this.terminated) {
            return;
        }
        let demand: number;
        try {
            demand = normalizeRequest(n);
        } catch (error) {
            this.terminated = true;
            if (this.upstream) {
                cancelSilently(this.upstream);
            }
            this.actual.onError(error);
            return;
        }
        if (this.requesting) {
            this.missedRequested = addCap(this.missedRequested, demand);
            return;
        }
        this.requesting = true;
        let nextDemand = demand;
        try {
            while (!this.terminated) {
                const available = this.remaining - this.outstanding;
                if (available > 0) {
                    const request = Math.min(nextDemand, available);
                    this.outstanding += request;
                    this.upstream?.request(request);
                }
                if (this.terminated || this.missedRequested === 0) {
                    return;
                }
                nextDemand = this.missedRequested;
                this.missedRequested = 0;
            }
        } catch (error) {
            if (!this.terminated) {
                this.terminated = true;
                if (this.upstream) {
                    cancelSilently(this.upstream);
                }
                this.actual.onError(error);
            }
        } finally {
            this.requesting = false;
        }
    }

    /** Cancels the active upstream once. */
    public cancel(): void {
        if (this.terminated) {
            return;
        }
        this.terminated = true;
        this.upstream?.cancel();
    }

    /** Exposes the downstream context unchanged. */
    public currentContext(): Context {
        return subscriberContext(this.actual);
    }

    /** Propagates downstream demand metadata while `take` still applies its own cap. */
    public [ITERATION_DEMAND_HINT](): number | undefined {
        const downstream = iterationDemandHint(this.actual);
        return downstream === undefined ? undefined : Math.min(downstream, this.remaining);
    }
}

/** Signals a lifter assembly failure through a valid Reactive Streams handshake. */
function signalAssemblyError<T>(subscriber: Subscriber<T>, error: unknown): void {
    subscriber.onSubscribe(EMPTY_SUBSCRIPTION);
    subscriber.onError(error);
}

/** Cancels an upstream while preserving the original operator failure. */
function cancelSilently(subscription: Subscription): void {
    try {
        subscription.cancel();
    } catch {
        // The operator failure remains the terminal signal.
    }
}

/** Shared inert subscription used only when operator assembly fails. */
const EMPTY_SUBSCRIPTION: Subscription = Object.freeze({
    /** Ignores requests because the sequence has already failed. */
    request() {
        // no-op
    },
    /** Ignores cancellation because no upstream was subscribed. */
    cancel() {
        // no-op
    }
});
