/**
 * @packageDocumentation
 * Direct Publisher terminal-consumption primitives.
 */
import type {Context} from "@/context/context.js";
import {UNBOUNDED_DEMAND} from "@/core/demand.js";
import {CancelledError} from "@/errors/classes.js";
import {ITERATION_DEMAND_HINT, type IterationDemandAware} from "@/internal/iteration-demand.js";
import type {Publisher} from "@/publisher/publisher.js";
import type {CoreSubscriber} from "@/subscription/core-subscriber.js";
import type {Subscription} from "@/subscription/subscription.js";

/** Stateful terminal reduction performed directly from Publisher signals. */
export interface TerminalCollector<T, R> {
    /** Consumes one value and returns true when the terminal result is already known. */
    onNext(value: T): boolean;

    /** Produces the terminal result after completion or an early match. */
    result(): R;
}

/**
 * Subscribes directly to a Publisher, requests one configured batch and reduces
 * signals without crossing the pull-based async-iterator bridge.
 */
export function consumePublisher<T, R>(
    publisher: Publisher<T>,
    signal: AbortSignal,
    context: Context,
    request: number,
    collector: TerminalCollector<T, R>
): Promise<R> {
    return new Promise<R>((resolve, reject) => {
        let subscription: Subscription | undefined;
        let subscribed = false;
        let terminated = false;

        const cleanup = () => signal.removeEventListener("abort", onAbort);
        const terminate = (): boolean => {
            if (terminated) {
                return false;
            }
            terminated = true;
            cleanup();
            return true;
        };
        const cancelUpstream = () => {
            try {
                subscription?.cancel();
            } catch {
                // Terminal cancellation is best-effort and must not replace the result.
            }
        };
        const fail = (error: unknown, cancel: boolean) => {
            if (!terminate()) {
                return;
            }
            if (cancel) {
                cancelUpstream();
            }
            reject(error);
        };
        const complete = (cancel: boolean) => {
            if (!terminate()) {
                return;
            }
            if (cancel) {
                cancelUpstream();
            }
            try {
                resolve(collector.result());
            } catch (error) {
                reject(error);
            }
        };
        function onAbort(): void {
            fail(new CancelledError(), true);
        }

        if (signal.aborted) {
            reject(new CancelledError());
            return;
        }
        signal.addEventListener("abort", onAbort, {once: true});

        const subscriber: CoreSubscriber<T> & IterationDemandAware = {
            /** Propagates the configured terminal batch through nested iterable operators. */
            [ITERATION_DEMAND_HINT]: () => request,
            /** Exposes the downstream context to context-aware Publisher sources. */
            currentContext: () => context,
            /** Stores the subscription and starts the configured terminal batch. */
            onSubscribe(nextSubscription) {
                if (subscribed || terminated) {
                    try {
                        nextSubscription.cancel();
                    } catch {
                        // Duplicate subscriptions are rejected best-effort.
                    }
                    return;
                }
                subscribed = true;
                subscription = nextSubscription;
                try {
                    nextSubscription.request(request);
                } catch (error) {
                    fail(error, true);
                }
            },
            /** Reduces a value and cancels immediately when the result is known. */
            onNext(value) {
                if (terminated) {
                    return;
                }
                try {
                    if (collector.onNext(value)) {
                        complete(true);
                    }
                } catch (error) {
                    fail(error, true);
                }
            },
            /** Forwards the first Publisher failure. */
            onError(error) {
                fail(error, false);
            },
            /** Produces the collected result after normal completion. */
            onComplete() {
                complete(false);
            }
        };

        try {
            publisher.subscribe(subscriber);
        } catch (error) {
            fail(error, true);
        }
    });
}

/** Collects every Publisher value with one unbounded upstream request. */
export function collectPublisher<T>(publisher: Publisher<T>, signal: AbortSignal, context: Context): Promise<T[]> {
    const values: T[] = [];
    return consumePublisher<T, T[]>(publisher, signal, context, UNBOUNDED_DEMAND, {
        /** Appends one source value to the result array. */
        onNext(value) {
            values.push(value);
            return false;
        },
        result: () => values
    });
}

/** Returns the last Publisher value after consuming it with unbounded demand. */
export function lastPublisherValue<T>(
    publisher: Publisher<T>,
    signal: AbortSignal,
    context: Context
): Promise<T | undefined> {
    let last: T | undefined;
    return consumePublisher<T, T | undefined>(publisher, signal, context, UNBOUNDED_DEMAND, {
        /** Replaces the previously observed last value. */
        onNext(value) {
            last = value;
            return false;
        },
        result: () => last
    });
}

/** Consumes and discards every Publisher value with one unbounded request. */
export function drainPublisher(publisher: Publisher<unknown>, signal: AbortSignal, context: Context): Promise<void> {
    return consumePublisher(publisher, signal, context, UNBOUNDED_DEMAND, {
        onNext: () => false,
        result: () => undefined
    });
}
