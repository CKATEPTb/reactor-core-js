import {Sink} from "@/sinks/Sink";

/**
 * Backpressure-aware push interface handed to the generator function of {@link Flux.create}.
 *
 * Extends {@link Sink} with demand tracking and lifecycle hooks so the producer
 * can react to downstream request signals and cancellation. The generator may call
 * `next`, `error`, and `complete` freely from any context (event listener, timer,
 * callback). Items pushed when downstream demand is zero are buffered and delivered
 * as soon as demand becomes available.
 *
 * @typeParam T - The type of items to push.
 */
export interface FluxSink<T> extends Sink<T> {
    /**
     * Push the next value. Buffered if downstream has no outstanding demand.
     * No-op after `complete()` or `error()` have been called.
     */
    next(value: T): void;

    /**
     * Terminate the stream with an error.
     * No-op if the sink has already terminated.
     */
    error(error: Error): void;

    /**
     * Complete the stream normally.
     * No-op if the sink has already terminated.
     */
    complete(): void;

    /**
     * The current downstream demand (number of items the subscriber has requested
     * but not yet received). Useful for push-pull hybrid sources.
     */
    readonly requested: number;

    /**
     * Register a callback that is called each time downstream requests more items.
     * Multiple callbacks may be registered; they are all called in registration order.
     * @param fn - Called with the additional demand count.
     * @returns `this` for chaining.
     */
    onRequest(fn: (n: number) => void): FluxSink<T>;

    /**
     * Register a callback that is called when downstream cancels (unsubscribes).
     * @param fn - Cancellation handler.
     * @returns `this` for chaining.
     */
    onCancel(fn: () => void): FluxSink<T>;

    /**
     * Register a callback that is called when the sink is disposed —
     * either by cancellation or terminal signal.
     * @param fn - Disposal handler.
     * @returns `this` for chaining.
     */
    onDispose(fn: () => void): FluxSink<T>;
}
