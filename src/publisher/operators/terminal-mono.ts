/**
 * @packageDocumentation
 * Direct terminal Mono implementation shared by aggregating operators.
 */
import {Context} from "@/context/context.js";
import {normalizeRequest, UNBOUNDED_DEMAND} from "@/core/demand.js";
import {ITERATION_DEMAND_HINT, iterationDemandHint} from "@/internal/iteration-demand.js";
import {consumePublisher, type TerminalCollector} from "@/internal/publisher-terminal.js";
import {Flux} from "@/publisher/flux.js";
import {Mono} from "@/publisher/mono.js";
import type {CoreSubscriber} from "@/subscription/core-subscriber.js";
import type {Subscriber} from "@/subscription/subscriber.js";
import type {Subscription} from "@/subscription/subscription.js";

/** Sentinel returned by a terminal collector when its Mono must complete empty. */
export const NO_TERMINAL_VALUE = Symbol("no-terminal-value");

/** Result produced by a terminal collector, including empty Mono completion. */
export type TerminalValue<T> = T | typeof NO_TERMINAL_VALUE;

/** Creates fresh terminal reduction state for each subscription. */
export type TerminalCollectorFactory<T, R> = () => TerminalCollector<T, TerminalValue<R>>;

/** Creates a Mono that reduces Publisher signals on the native demand path. */
export function terminalMono<T, R>(
    source: Flux<T>,
    upstreamRequest: number,
    collectorFactory: TerminalCollectorFactory<T, R>
): Mono<R> {
    return new DirectTerminalMono(source, upstreamRequest, collectorFactory);
}

/** Mono whose subscribe and iteration paths both consume the source directly. */
class DirectTerminalMono<T, R> extends Mono<R> {
    /** Creates a direct terminal Mono around one source. */
    public constructor(
        private readonly source: Flux<T>,
        private readonly upstreamRequest: number,
        private readonly collectorFactory: TerminalCollectorFactory<T, R>
    ) {
        super(async function* (signal, context) {
            if (signal.aborted) {
                return;
            }
            const result = await consumePublisher(source, signal, context, upstreamRequest, collectorFactory());
            if (result !== NO_TERMINAL_VALUE) {
                yield result as R;
            }
        });
    }

    /** Installs a subscriber that waits for downstream demand before requesting upstream. */
    protected override subscribeActual(subscriber: Subscriber<R>): void {
        new TerminalMonoSubscriber(subscriber, this.upstreamRequest, this.collectorFactory).start(this.source);
    }
}

/** Subscriber/subscription pair that reduces a Flux to at most one value. */
class TerminalMonoSubscriber<T, R> implements CoreSubscriber<T>, Subscription {
    /** Active upstream subscription. */
    private upstream: Subscription | undefined;
    /** Collector created lazily on the first valid downstream request. */
    private collector: TerminalCollector<T, TerminalValue<R>> | undefined;
    /** Tracks the upstream Reactive Streams handshake. */
    private subscribed = false;
    /** Ensures upstream is requested at most once. */
    private requested = false;
    /** Records completion that arrived before downstream demand. */
    private completedBeforeRequest = false;
    /** Guards the downstream result callback against reentrant upstream signals. */
    private emitting = false;
    /** Ensures the active upstream is cancelled at most once. */
    private upstreamCancelled = false;
    /** Suppresses signals after cancellation or termination. */
    private terminated = false;

    /** Creates a terminal subscriber for one downstream subscriber. */
    public constructor(
        private readonly actual: Subscriber<R>,
        private readonly upstreamRequest: number,
        private readonly collectorFactory: TerminalCollectorFactory<T, R>
    ) {
    }

    /** Subscribes this terminal reducer to its source with handshake error recovery. */
    public start(source: Flux<T>): void {
        try {
            source.subscribe(this);
        } catch (error) {
            if (this.terminated) {
                throw error;
            }
            if (!this.subscribed && !this.terminated) {
                this.subscribed = true;
                this.actual.onSubscribe(this);
            }
            this.onError(error);
        }
    }

    /** Stores the upstream and exposes this demand-gating subscription downstream. */
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
            this.cancelUpstream();
            throw error;
        }
    }

    /** Reduces a value and emits the result immediately when it becomes final. */
    public onNext(value: T): void {
        if (this.terminated || this.emitting || !this.requested || !this.collector) {
            return;
        }
        try {
            if (this.collector.onNext(value)) {
                this.emitResult(true);
            }
        } catch (error) {
            this.fail(error, true);
        }
    }

    /** Forwards the first upstream failure without waiting for demand. */
    public onError(error: unknown): void {
        if (this.emitting) {
            return;
        }
        this.fail(error, false);
    }

    /** Emits the reduced result, or remembers completion until demand arrives. */
    public onComplete(): void {
        if (this.terminated || this.emitting) {
            return;
        }
        if (!this.requested) {
            this.completedBeforeRequest = true;
            return;
        }
        this.emitResult(false);
    }

    /** Starts the single upstream terminal batch on the first valid request. */
    public request(n: number): void {
        if (this.terminated) {
            return;
        }
        try {
            normalizeRequest(n);
        } catch (error) {
            this.fail(error, true);
            return;
        }
        if (this.requested) {
            return;
        }
        this.requested = true;
        try {
            this.collector = this.collectorFactory();
        } catch (error) {
            this.fail(error, true);
            return;
        }
        if (this.completedBeforeRequest) {
            this.emitResult(false);
            return;
        }
        try {
            this.upstream?.request(this.upstreamRequest);
        } catch (error) {
            this.fail(error, true);
        }
    }

    /** Cancels the source and releases the collector state. */
    public cancel(): void {
        if (this.terminated) {
            return;
        }
        this.terminated = true;
        this.collector = undefined;
        this.cancelUpstream();
    }

    /** Exposes the downstream Reactor context to the source. */
    public currentContext(): Context {
        return (this.actual as CoreSubscriber<R>).currentContext?.() ?? Context.empty();
    }

    /** Propagates a guaranteed downstream batch to eagerly assembled iterable sources. */
    public [ITERATION_DEMAND_HINT](): number | undefined {
        return iterationDemandHint(this.actual) === UNBOUNDED_DEMAND
            ? this.upstreamRequest
            : undefined;
    }

    /** Produces and emits the terminal result, cancelling upstream for early results. */
    private emitResult(cancelUpstream: boolean): void {
        if (this.terminated || !this.collector) {
            return;
        }
        let result: TerminalValue<R>;
        try {
            result = this.collector.result();
        } catch (error) {
            this.fail(error, cancelUpstream);
            return;
        }
        this.collector = undefined;
        this.emitting = true;
        if (cancelUpstream) {
            this.cancelUpstream();
        }
        if (result !== NO_TERMINAL_VALUE) {
            try {
                this.actual.onNext(result as R);
            } catch (error) {
                this.emitting = false;
                if (this.terminated) {
                    return;
                }
                this.terminated = true;
                this.cancelUpstream();
                this.actual.onError(error);
                return;
            }
        }
        this.emitting = false;
        if (this.terminated) {
            return;
        }
        this.terminated = true;
        this.actual.onComplete();
    }

    /** Cancels when requested and forwards one terminal failure. */
    private fail(error: unknown, cancelUpstream: boolean): void {
        if (this.terminated) {
            return;
        }
        this.terminated = true;
        this.collector = undefined;
        if (cancelUpstream) {
            this.cancelUpstream();
        }
        this.actual.onError(error);
    }

    /** Cancels the active upstream at most once. */
    private cancelUpstream(): void {
        if (this.upstreamCancelled) {
            return;
        }
        this.upstreamCancelled = true;
        if (this.upstream) {
            cancelSilently(this.upstream);
        }
    }
}

/** Cancels a subscription without replacing the active terminal signal. */
function cancelSilently(subscription: Subscription): void {
    try {
        subscription.cancel();
    } catch {
        // Terminal state has already been selected.
    }
}
