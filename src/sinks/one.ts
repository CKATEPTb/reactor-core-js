/**
 * @packageDocumentation
 * Sink specifications and default browser-oriented sink implementations.
 */
import {AsyncQueue} from "@/internal/async-queue.js";
import {Mono} from "@/publisher/mono.js";
import {SignalType} from "@/signal/signal-type.js";
import {
    addSinkSubscriber,
    clearSinkSubscribers,
    removeSinkSubscriber,
    sinkSubscriberCount,
    type SinkSubscribers
} from "@/sinks/subscribers.js";
import {type EmitFailureHandler, EmitFailureHandlers, emitOrThrow, EmitResult, type OneSink} from "@/sinks/types.js";

/** Terminal or pending state held by a one-valued sink. */
type OneState = "pending" | "empty" | "value" | "error";

/** Default replaying implementation for `Sinks.one()` and `Sinks.empty()`. */
export class DefaultOneSink<T> implements OneSink<T> {
    /** Current terminal or pending state of the sink. */
    private state: OneState = "pending";
    /** Stored value replayed to late subscribers after successful termination. */
    private value: T | undefined;
    /** Stored error replayed to late subscribers after failed termination. */
    private errorValue: unknown;
    /** Active subscriber queues waiting for the terminal result, created only when needed. */
    private subscribers: SinkSubscribers<T> | undefined;

    /** Returns the Mono view of this sink. */
    public asMono(): Mono<T> {
        return new Mono(signal => {
            const queue = new AsyncQueue<T>(signal);
            this.replay(queue);
            if (this.state === "pending") {
                this.addSubscriber(queue, signal);
            }
            return queue;
        });
    }

    /** Attempts to emit a value and complete this sink. */
    public tryEmitValue(value: T): EmitResult {
        if (this.state !== "pending") {
            return EmitResult.FAIL_TERMINATED;
        }
        this.state = "value";
        this.value = value;
        const subscribers = this.subscribers;
        if (subscribers instanceof Set) {
            for (const subscriber of subscribers) {
                subscriber.push(value);
                subscriber.complete();
            }
        } else if (subscribers) {
            subscribers.push(value);
            subscribers.complete();
        }
        this.clearSubscribers();
        return EmitResult.OK;
    }

    /** Attempts to complete this sink without a value. */
    public tryEmitEmpty(): EmitResult {
        if (this.state !== "pending") {
            return EmitResult.FAIL_TERMINATED;
        }
        this.state = "empty";
        const subscribers = this.subscribers;
        if (subscribers instanceof Set) {
            for (const subscriber of subscribers) {
                subscriber.complete();
            }
        } else if (subscribers) {
            subscribers.complete();
        }
        this.clearSubscribers();
        return EmitResult.OK;
    }

    /** Attempts to fail this sink. */
    public tryEmitError(error: unknown): EmitResult {
        if (this.state !== "pending") {
            return EmitResult.FAIL_TERMINATED;
        }
        this.state = "error";
        this.errorValue = error;
        const subscribers = this.subscribers;
        if (subscribers instanceof Set) {
            for (const subscriber of subscribers) {
                subscriber.error(error);
            }
        } else if (subscribers) {
            subscribers.error(error);
        }
        this.clearSubscribers();
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
        return sinkSubscriberCount(this.subscribers);
    }

    /** Handles an emission failure and returns false to fail fast by default. */
    public onEmitFailure(_signalType: SignalType, _emitResult: EmitResult): boolean {
        return false;
    }

    /** Adds a subscriber queue and removes it again when its subscription is cancelled. */
    private addSubscriber(queue: AsyncQueue<T>, signal: AbortSignal): void {
        this.subscribers = addSinkSubscriber(this.subscribers, queue);
        signal.addEventListener("abort", () => this.removeSubscriber(queue), {once: true});
    }

    /** Removes a subscriber queue and releases the set when it becomes empty. */
    private removeSubscriber(queue: AsyncQueue<T>): void {
        this.subscribers = removeSinkSubscriber(this.subscribers, queue);
    }

    /** Clears and releases all active subscriber queues. */
    private clearSubscribers(): void {
        clearSinkSubscribers(this.subscribers);
        this.subscribers = undefined;
    }

    /** Replays the current state into a newly subscribed queue. */
    private replay(queue: AsyncQueue<T>): void {
        switch (this.state) {
            case "value":
                queue.push(this.value as T);
                queue.complete();
                break;
            case "empty":
                queue.complete();
                break;
            case "error":
                queue.error(this.errorValue);
                break;
            case "pending":
                break;
        }
    }
}
