/**
 * @packageDocumentation
 * Sink specifications and default browser-oriented sink implementations.
 */
import {AsyncQueue} from "@/internal/index.js";
import {Mono} from "@/publishers/mono.js";
import {SignalType} from "@/signal/index.js";
import {type EmitFailureHandler, EmitFailureHandlers, emitOrThrow, EmitResult, type OneSink} from "@/sinks/types.js";

/** Terminal or pending state held by a one-valued sink. */
type OneState<T> =
    | { type: "pending" }
    | { type: "empty" }
    | { type: "value"; value: T }
    | { type: "error"; error: unknown };

/** Default replaying implementation for `Sinks.one()` and `Sinks.empty()`. */
export class DefaultOneSink<T> implements OneSink<T> {
    /** Current terminal or pending state of the sink. */
    private state: OneState<T> = {type: "pending"};
    /** Active subscriber queues waiting for the terminal result. */
    private readonly subscribers = new Set<AsyncQueue<T>>();

    /** Returns the Mono view of this sink. */
    public asMono(): Mono<T> {
        return new Mono(signal => {
            const queue = new AsyncQueue<T>(signal);
            this.replay(queue);
            if (this.state.type === "pending") {
                this.subscribers.add(queue);
                signal.addEventListener("abort", () => this.subscribers.delete(queue), {once: true});
            }
            return queue;
        });
    }

    /** Attempts to emit a value and complete this sink. */
    public tryEmitValue(value: T): EmitResult {
        if (this.state.type !== "pending") {
            return EmitResult.FAIL_TERMINATED;
        }
        this.state = {type: "value", value};
        for (const subscriber of this.subscribers) {
            subscriber.push(value);
            subscriber.complete();
        }
        this.subscribers.clear();
        return EmitResult.OK;
    }

    /** Attempts to complete this sink without a value. */
    public tryEmitEmpty(): EmitResult {
        if (this.state.type !== "pending") {
            return EmitResult.FAIL_TERMINATED;
        }
        this.state = {type: "empty"};
        for (const subscriber of this.subscribers) {
            subscriber.complete();
        }
        this.subscribers.clear();
        return EmitResult.OK;
    }

    /** Attempts to fail this sink. */
    public tryEmitError(error: unknown): EmitResult {
        if (this.state.type !== "pending") {
            return EmitResult.FAIL_TERMINATED;
        }
        this.state = {type: "error", error};
        for (const subscriber of this.subscribers) {
            subscriber.error(error);
        }
        this.subscribers.clear();
        return EmitResult.OK;
    }

    /** Emits a value or delegates failure handling to the provided handler. */
    public emitValue(value: T, handler: EmitFailureHandler = EmitFailureHandlers.FAIL_FAST): void {
        const result = this.tryEmitValue(value);
        if (result !== EmitResult.FAIL_TERMINATED) {
            emitOrThrow(SignalType.NEXT, () => result, handler);
        }
    }

    /** Completes this sink or delegates failure handling to the provided handler. */
    public emitEmpty(handler: EmitFailureHandler = EmitFailureHandlers.FAIL_FAST): void {
        const result = this.tryEmitEmpty();
        if (result !== EmitResult.FAIL_TERMINATED) {
            emitOrThrow(SignalType.COMPLETE, () => result, handler);
        }
    }

    /** Fails this sink or delegates failure handling to the provided handler. */
    public emitError(error: unknown, handler: EmitFailureHandler = EmitFailureHandlers.FAIL_FAST): void {
        const result = this.tryEmitError(error);
        if (result !== EmitResult.FAIL_TERMINATED) {
            emitOrThrow(SignalType.ERROR, () => result, handler, error);
        }
    }

    /** Returns the current number of active subscribers. */
    public currentSubscriberCount(): number {
        return this.subscribers.size;
    }

    /** Handles an emission failure and returns false to fail fast by default. */
    public onEmitFailure(_signalType: SignalType, _emitResult: EmitResult): boolean {
        return false;
    }

    /** Replays the current state into a newly subscribed queue. */
    private replay(queue: AsyncQueue<T>): void {
        switch (this.state.type) {
            case "value":
                queue.push(this.state.value);
                queue.complete();
                break;
            case "empty":
                queue.complete();
                break;
            case "error":
                queue.error(this.state.error);
                break;
            case "pending":
                break;
        }
    }
}
