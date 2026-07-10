/**
 * @packageDocumentation
 * Sink specifications and default browser-oriented sink implementations.
 */
import {AsyncQueue} from "@/internal/async-queue.js";
import {Flux} from "@/publisher/flux.js";
import type {PublisherInput} from "@/publisher/types.js";
import {SignalType} from "@/signal/signal-type.js";
import {
    addSinkSubscriber,
    clearSinkSubscribers,
    removeSinkSubscriber,
    sinkSubscriberCount,
    type SinkSubscribers
} from "@/sinks/subscribers.js";
import {type EmitFailureHandler, EmitFailureHandlers, emitOrThrow, EmitResult, type ManySink} from "@/sinks/types.js";

/** Delivery mode supported by the default many-valued sink. */
type ManyMode = "unicast" | "unicast-error" | "multicast-buffer" | "multicast-direct" | "replay";

/** Configuration for a many-valued sink implementation. */
export interface ManyOptions<T> {
    /** Delivery mode used by the sink. */
    mode: ManyMode;
    /** Maximum number of replayed values retained in history. */
    replayLimit?: number;
    /** Initial value replayed by latest-or-default sinks. */
    latestDefault?: T;
}

/** Default implementation for unicast, multicast and replay many-valued sinks. */
export class DefaultManySink<T> implements ManySink<T> {
    /** Active subscriber queues currently receiving live values. */
    private subscribers: SinkSubscribers<T> | undefined;
    /** Values buffered before a non-replay sink receives its first subscriber. */
    private pending: T[] | undefined;
    /** Values retained for replay subscribers. */
    private history: T[] | undefined;
    /** Ring-buffer index of the oldest replay value. */
    private historyHead = 0;
    /** Terminal state when the sink has completed or failed. */
    private terminated: "complete" | "error" | undefined;
    /** Error retained for late subscribers after failure. */
    private errorValue: unknown;
    /** Tracks whether a unicast sink already accepted its single subscriber. */
    private unicastAttached = false;
    /** Construction options controlling delivery and replay behavior. */
    private readonly options: ManyOptions<T>;

    /** Creates a many-valued sink with the provided delivery mode. */
    public constructor(options: ManyOptions<T>) {
        this.options = options;
        if (options.latestDefault !== undefined) {
            this.rememberHistory(options.latestDefault);
        }
    }

    /** Returns the Flux view of this sink. */
    public asFlux(): Flux<T> {
        return new Flux(signal => {
            const queue = new AsyncQueue<T>(signal);
            if (isUnicast(this.options.mode) && this.unicastAttached) {
                queue.error(new Error("Unicast sink allows only one subscriber"));
                return queue;
            }
            if (isUnicast(this.options.mode)) {
                this.unicastAttached = true;
            }
            const history = this.history;
            if (history) {
                for (let index = 0; index < history.length; index += 1) {
                    queue.push(historyValue(history, this.historyHead, index));
                }
            }
            if (this.pending && this.pending.length > 0) {
                const pending = this.pending;
                this.pending = undefined;
                for (const value of pending) {
                    queue.push(value);
                }
            }
            if (this.terminated === "complete") {
                queue.complete();
            } else if (this.terminated === "error") {
                queue.error(this.errorValue);
            } else {
                this.addSubscriber(queue, signal);
            }
            return queue;
        });
    }

    /** Attempts to emit a value to current subscribers. */
    public tryEmitNext(value: T): EmitResult {
        if (this.terminated) {
            return EmitResult.FAIL_TERMINATED;
        }
        if (this.options.mode === "replay") {
            this.rememberHistory(value);
        }
        const subscribers = this.subscribers;
        if (!subscribers) {
            if (this.options.mode === "multicast-direct" || this.options.mode === "unicast-error") {
                return EmitResult.FAIL_ZERO_SUBSCRIBER;
            }
            if (this.options.mode !== "replay") {
                (this.pending ??= []).push(value);
            }
            return EmitResult.OK;
        }
        if (subscribers instanceof Set) {
            for (const subscriber of subscribers) {
                subscriber.push(value);
            }
        } else {
            subscribers.push(value);
        }
        return EmitResult.OK;
    }

    /** Attempts to complete this sink. */
    public tryEmitComplete(): EmitResult {
        if (this.terminated) {
            return EmitResult.FAIL_TERMINATED;
        }
        const subscribers = this.subscribers;
        if (this.options.mode === "unicast-error" && !subscribers) {
            return EmitResult.FAIL_ZERO_SUBSCRIBER;
        }
        this.terminated = "complete";
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
        if (this.terminated) {
            return EmitResult.FAIL_TERMINATED;
        }
        const subscribers = this.subscribers;
        if (this.options.mode === "unicast-error" && !subscribers) {
            return EmitResult.FAIL_ZERO_SUBSCRIBER;
        }
        this.terminated = "error";
        this.errorValue = error;
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
    public emitNext(value: T, handler: EmitFailureHandler = EmitFailureHandlers.FAIL_FAST): void {
        emitOrThrow(SignalType.NEXT, () => this.tryEmitNext(value), handler);
    }

    /** Completes this sink or delegates failure handling to the provided handler. */
    public emitComplete(handler: EmitFailureHandler = EmitFailureHandlers.FAIL_FAST): void {
        emitOrThrow(SignalType.COMPLETE, () => this.tryEmitComplete(), handler);
    }

    /** Fails this sink or delegates failure handling to the provided handler. */
    public emitError(error: unknown, handler: EmitFailureHandler = EmitFailureHandlers.FAIL_FAST): void {
        emitOrThrow(SignalType.ERROR, () => this.tryEmitError(error), handler, error);
    }

    /** Returns the current number of active subscribers. */
    public currentSubscriberCount(): number {
        return sinkSubscriberCount(this.subscribers);
    }

    /** Subscribes this sink to an upstream publisher and relays its signals. */
    public subscribeTo(source: PublisherInput<T>): void {
        Flux.from(source).subscribe(
            value => this.emitNext(value),
            error => this.emitError(error),
            () => this.emitComplete()
        );
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

    /** Records one value into replay history, allocating history only for replay sinks. */
    private rememberHistory(value: T): void {
        const limit = this.options.replayLimit ?? Number.POSITIVE_INFINITY;
        if (limit <= 0) {
            return;
        }
        const history = this.history;
        if (!history) {
            this.history = [value];
            this.historyHead = 0;
            return;
        }
        if (history.length < limit) {
            history.push(value);
            return;
        }
        history[this.historyHead] = value;
        this.historyHead = (this.historyHead + 1) % history.length;
    }
}

/** Returns true for sink modes that allow exactly one subscriber. */
function isUnicast(mode: ManyMode): boolean {
    return mode === "unicast" || mode === "unicast-error";
}

/** Returns a replay history value in oldest-to-newest order. */
function historyValue<T>(history: T[], head: number, index: number): T {
    return history[(head + index) % history.length] as T;
}
