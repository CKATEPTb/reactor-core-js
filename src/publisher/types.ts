/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import type {Publisher} from "@/publisher/publisher.js";
import type {Context} from "@/context/context.js";
import type {AnyIterable} from "@/internal/iterable.js";

/** Lazy factory backing Flux and Mono sources. */
export type SourceFactory<T> = (signal: AbortSignal, context: Context) => AnyIterable<T>;
/** Callback that consumes a value. */
export type Consumer<T> = (value: T) => void;
/** Predicate callback used by filtering operators. */
export type Predicate<T> = (value: T) => boolean;
/** Mapping callback used by transformation operators. */
export type Mapper<T, R> = (value: T) => R;
/** Input type accepted by publisher adaptation methods. */
export type PublisherInput<T> = Publisher<T> | Iterable<T> | AsyncIterable<T> | PromiseLike<T>;
/** Callback used by `Flux.create`. */
export type FluxSinkCallback<T> = (sink: FluxSink<T>) => void;
/** Callback used by `Mono.create`. */
export type MonoSinkCallback<T> = (sink: MonoSink<T>) => void;

/** Imperative sink exposed to `Flux.create` callbacks. */
export interface FluxSink<T> {
    /** Pushes one value to the created Flux. */
    next(value: T): void;

    /** Terminates the sequence with an error. */
    error(error: unknown): void;

    /** Completes the created Flux successfully. */
    complete(): void;

    /** Registers a callback invoked on cancellation. */
    onCancel(callback: () => void): FluxSink<T>;

    /** Returns true when the subscriber cancelled the sequence. */
    isCancelled(): boolean;
}

/** Imperative sink exposed to `Mono.create` callbacks. */
export interface MonoSink<T> extends FluxSink<T> {
    /** Completes the Mono with an optional value. */
    success(value?: T): void;

    /** Registers a callback invoked on cancellation. */
    onCancel(callback: () => void): MonoSink<T>;
}

/** Synchronous one-signal sink used by handle operators. */
export interface SynchronousSink<T> {
    /** Emits one value for the currently handled input. */
    next(value: T): void;

    /** Completes without emitting a value for the current input. */
    complete(): void;

    /** Fails the sequence with an error. */
    error(error: unknown): void;
}
