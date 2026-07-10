/**
 * @packageDocumentation
 * Sink specifications and default browser-oriented sink implementations.
 */
import type {Disposable} from "@/core/index.js";
import type {Flux} from "@/publishers/flux.js";
import type {Mono} from "@/publishers/mono.js";
import type {PublisherInput} from "@/publishers/types.js";
import {SignalType} from "@/signal/index.js";

/** Result of a sink emission attempt. */
export enum EmitResult {
    /** Emission succeeded. */
    OK = "OK",
    /** Emission failed because the sink has already terminated. */
    FAIL_TERMINATED = "FAIL_TERMINATED",
    /** Emission failed because buffering capacity was exhausted. */
    FAIL_OVERFLOW = "FAIL_OVERFLOW",
    /** Emission failed because the sink was cancelled. */
    FAIL_CANCELLED = "FAIL_CANCELLED",
    /** Emission failed because concurrent emission was detected. */
    FAIL_NON_SERIALIZED = "FAIL_NON_SERIALIZED",
    /** Emission failed because no subscriber was present. */
    FAIL_ZERO_SUBSCRIBER = "FAIL_ZERO_SUBSCRIBER"
}

/** Error thrown when an emitting API cannot recover from an emission failure. */
export class EmissionException extends Error {
    /** Failed emission result that caused this exception. */
    public readonly reason: EmitResult;

    /** Creates an emission exception for a failed result. */
    public constructor(reason: EmitResult, cause?: unknown) {
        super(cause === undefined ? `Emission failed with ${reason}` : `Emission failed with ${reason}: ${String(cause)}`);
        this.reason = reason;
        this.name = "EmissionException";
    }
}

/** Callback that decides whether a failed emission should be retried. */
export type EmitFailureHandler = (signalType: SignalType, emitResult: EmitResult) => boolean;

/** Built-in emission failure handlers. */
export const EmitFailureHandlers = {
    /** Fails immediately without retrying. */
    FAIL_FAST: () => false,

    /** Retries non-serialized emissions until the busy-loop deadline expires. */
    busyLooping(durationMs: number): EmitFailureHandler {
        return (signalType, emitResult) => {
            const deadline = Date.now() + durationMs;
            return signalType !== SignalType.ERROR && emitResult === EmitResult.FAIL_NON_SERIALIZED && Date.now() < deadline;
        };
    }
};

/** Sink that can only emit a terminal signal. */
export interface EmptySink<T> {
    /** Returns the Mono view of this sink. */
    asMono(): Mono<T>;

    /** Returns current active subscriber count. */
    currentSubscriberCount(): number;

    /** Attempts to complete this sink. */
    tryEmitEmpty(): EmitResult;

    /** Attempts to fail this sink. */
    tryEmitError(error: unknown): EmitResult;

    /** Completes this sink using the provided failure handler. */
    emitEmpty(handler?: EmitFailureHandler): void;

    /** Fails this sink using the provided failure handler. */
    emitError(error: unknown, handler?: EmitFailureHandler): void;

    /** Handles an emission failure and returns true when the caller should retry. */
    onEmitFailure(signalType: SignalType, emitResult: EmitResult): boolean;
}

/** Sink that can emit zero or one value followed by completion. */
export interface OneSink<T> extends EmptySink<T> {
    /** Attempts to emit a value and complete this sink. */
    tryEmitValue(value: T): EmitResult;

    /** Emits a value using the provided failure handler. */
    emitValue(value: T, handler?: EmitFailureHandler): void;
}

/** Sink that can emit multiple values and a terminal signal. */
export interface ManySink<T> {
    /** Returns the Flux view of this sink. */
    asFlux(): Flux<T>;

    /** Attempts to emit a value. */
    tryEmitNext(value: T): EmitResult;

    /** Attempts to complete this sink. */
    tryEmitComplete(): EmitResult;

    /** Attempts to fail this sink. */
    tryEmitError(error: unknown): EmitResult;

    /** Emits a value using the provided failure handler. */
    emitNext(value: T, handler?: EmitFailureHandler): void;

    /** Completes this sink using the provided failure handler. */
    emitComplete(handler?: EmitFailureHandler): void;

    /** Fails this sink using the provided failure handler. */
    emitError(error: unknown, handler?: EmitFailureHandler): void;

    /** Returns current active subscriber count. */
    currentSubscriberCount(): number;

    /** Subscribes this sink to an upstream publisher. */
    subscribeTo(source: PublisherInput<T>): void;

    /** Handles an emission failure and returns true when the caller should retry. */
    onEmitFailure(signalType: SignalType, emitResult: EmitResult): boolean;
}

/** Root builder for many-valued sink variants. */
export interface ManySpec {
    /** Returns unicast sink builders. */
    unicast(): UnicastSpec;

    /** Returns multicast sink builders. */
    multicast(): MulticastSpec;

    /** Returns replay sink builders. */
    replay(): ReplaySpec;
}

/** Builder for unicast many-valued sinks. */
export interface UnicastSpec {
    /** Creates a unicast sink with backpressure buffering. */
    onBackpressureBuffer<T>(): ManySink<T>;

    /** Creates a unicast sink that fails when backpressure cannot be honored. */
    onBackpressureError<T>(): ManySink<T>;
}

/** Builder for sinks that can subscribe to one upstream publisher. */
export interface ManyWithUpstreamSpec {
    /** Creates a multicast sink that buffers and can subscribe to one upstream publisher. */
    multicastOnBackpressureBuffer<T>(): ManySink<T>;
}

/** Builder for multicast many-valued sinks. */
export interface MulticastSpec {
    /** Creates a multicast sink with backpressure buffering. */
    onBackpressureBuffer<T>(): ManySink<T>;

    /** Creates a direct multicast sink that fails when no subscriber is present. */
    directAllOrNothing<T>(): ManySink<T>;

    /** Creates a direct best-effort multicast sink. */
    directBestEffort<T>(): ManySink<T>;
}

/** Builder for replaying many-valued sinks. */
export interface ReplaySpec {
    /** Replays all emitted values to late subscribers. */
    all<T>(): ManySink<T>;

    /** Replays up to `historySize` values to late subscribers. */
    limit<T>(historySize: number): ManySink<T>;

    /** Replays only the latest value to late subscribers. */
    latest<T>(): ManySink<T>;

    /** Replays the latest value or the provided default before the first emission. */
    latestOrDefault<T>(value: T): ManySink<T>;
}

/** Disposable type alias used by sink implementations. */
export type SinkDisposable = Disposable;

/** Repeatedly invokes an emission attempt or throws when failure is not recoverable. */
export function emitOrThrow(
    signalType: SignalType,
    emit: () => EmitResult,
    handler: EmitFailureHandler,
    cause?: unknown
): void {
    for (; ;) {
        const result = emit();
        if (result === EmitResult.OK) {
            return;
        }
        if (!handler(signalType, result)) {
            throw new EmissionException(result, cause);
        }
    }
}

/** Returns true when the emission result is successful. */
export function isEmitSuccess(result: EmitResult): boolean {
    return result === EmitResult.OK;
}

/** Returns true when the emission result is a failure. */
export function isEmitFailure(result: EmitResult): boolean {
    return result !== EmitResult.OK;
}
