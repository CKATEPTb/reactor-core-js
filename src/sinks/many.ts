/**
 * @packageDocumentation
 * Sink specifications and default browser-oriented sink implementations.
 */
import {AsyncQueue} from "@/internal/index.js";
import {Flux} from "@/publishers/flux.js";
import type {PublisherInput} from "@/publishers/types.js";
import {SignalType} from "@/signal/index.js";
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
    private readonly subscribers = new Set<AsyncQueue<T>>();
    /** Values buffered before a non-replay sink receives its first subscriber. */
    private pending: T[] = [];
    /** Values retained for replay subscribers. */
    private readonly history: T[] = [];
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
            this.historyHead = rememberHistory(this.history, this.historyHead, options.replayLimit, options.latestDefault);
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
            for (let index = 0; index < this.history.length; index += 1) {
                queue.push(historyValue(this.history, this.historyHead, index));
            }
            if (this.pending.length > 0) {
                const pending = this.pending;
                this.pending = [];
                for (const value of pending) {
                    queue.push(value);
                }
            }
            if (this.terminated === "complete") {
                queue.complete();
            } else if (this.terminated === "error") {
                queue.error(this.errorValue);
            } else {
                this.subscribers.add(queue);
                signal.addEventListener("abort", () => this.subscribers.delete(queue), {once: true});
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
            this.historyHead = rememberHistory(this.history, this.historyHead, this.options.replayLimit, value);
        }
        if (this.subscribers.size === 0) {
            if (this.options.mode === "multicast-direct" || this.options.mode === "unicast-error") {
                return EmitResult.FAIL_ZERO_SUBSCRIBER;
            }
            if (this.options.mode !== "replay") {
                this.pending.push(value);
            }
            return EmitResult.OK;
        }
        for (const subscriber of this.subscribers) {
            subscriber.push(value);
        }
        return EmitResult.OK;
    }

    /** Attempts to complete this sink. */
    public tryEmitComplete(): EmitResult {
        if (this.terminated) {
            return EmitResult.FAIL_TERMINATED;
        }
        if (this.options.mode === "unicast-error" && this.subscribers.size === 0) {
            return EmitResult.FAIL_ZERO_SUBSCRIBER;
        }
        this.terminated = "complete";
        for (const subscriber of this.subscribers) {
            subscriber.complete();
        }
        this.subscribers.clear();
        return EmitResult.OK;
    }

    /** Attempts to fail this sink. */
    public tryEmitError(error: unknown): EmitResult {
        if (this.terminated) {
            return EmitResult.FAIL_TERMINATED;
        }
        if (this.options.mode === "unicast-error" && this.subscribers.size === 0) {
            return EmitResult.FAIL_ZERO_SUBSCRIBER;
        }
        this.terminated = "error";
        this.errorValue = error;
        for (const subscriber of this.subscribers) {
            subscriber.error(error);
        }
        this.subscribers.clear();
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
        return this.subscribers.size;
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
}

/** Returns true for sink modes that allow exactly one subscriber. */
function isUnicast(mode: ManyMode): boolean {
    return mode === "unicast" || mode === "unicast-error";
}

/** Records a value into replay history and returns the updated ring-buffer head. */
function rememberHistory<T>(history: T[], head: number, replayLimit: number | undefined, value: T): number {
    const limit = replayLimit ?? Number.POSITIVE_INFINITY;
    if (limit <= 0) {
        return head;
    }
    if (history.length < limit) {
        history.push(value);
        return head;
    }
    history[head] = value;
    return (head + 1) % history.length;
}

/** Returns a replay history value in oldest-to-newest order. */
function historyValue<T>(history: T[], head: number, index: number): T {
    return history[(head + index) % history.length] as T;
}
