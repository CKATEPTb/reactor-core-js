import {AbstractMulticastSink} from "@/sinks/internal/AbstractMulticastSink";
import {Subscriber} from "@/subscriptions";

// Rule 3.16: draining flag prevents re-entrant drain (onNext → request → drain)
type Entry<T> = { pending: T; hasPending: boolean; demand: number; cancelled: boolean; draining: boolean };

/**
 * Replays the latest emitted item to new subscribers, or the default value if no item was emitted yet.
 */
export default class ReplayLatestOrDefaultSink<T> extends AbstractMulticastSink<T, Entry<T>> {

    private latestValue: T;

    constructor(defaultValue: T) {
        super();
        this.latestValue = defaultValue;
    }

    // ──── AbstractMulticastSink hooks ─────────────────────────────────────────

    protected createEntry(): Entry<T> {
        return { pending: this.latestValue, hasPending: true, demand: 0, cancelled: false, draining: false };
    }

    /**
     * Only replay an error terminal immediately. If completed, allow subscription
     * so the subscriber can still receive the latest value.
     */
    protected shouldReplayTerminalOnSubscribe(): boolean {
        return this.terminated && this.terminalError !== null;
    }

    /** Drain pending value first, then signal error. */
    protected deliverError(sub: Subscriber<T>, entry: Entry<T>): void {
        this.drainEntry(sub, entry);
        if (!entry.cancelled) sub.onError(this.terminalError!);
    }

    /** Drain pending value — drainEntry signals complete when done. */
    protected deliverComplete(sub: Subscriber<T>, entry: Entry<T>): void {
        if (!entry.cancelled) this.drainEntry(sub, entry);
    }

    protected clearEntriesAfterComplete(): boolean {
        return false;
    }

    protected onDemandGranted(sub: Subscriber<T>, entry: Entry<T>): void {
        this.drainEntry(sub, entry);
    }

    // ──── Sink.next ───────────────────────────────────────────────────────────

    next(value: T): void {
        if (this.terminated) return;
        this.latestValue = value;
        for (const [sub, entry] of this.entries) {
            if (!entry.cancelled) {
                entry.pending = value;
                entry.hasPending = true;
                this.drainEntry(sub, entry);
            }
        }
    }

    // ──── Internal drain ──────────────────────────────────────────────────────

    private drainEntry(sub: Subscriber<T>, entry: Entry<T>): void {
        if (entry.cancelled || entry.draining) return;
        entry.draining = true;
        try {
            if (entry.hasPending && entry.demand > 0) {
                entry.demand--;
                entry.hasPending = false;
                sub.onNext(entry.pending);
                if (entry.cancelled) return;
            }
            if (!entry.hasPending && this.terminated) {
                this.entries.delete(sub);
                this.terminalError ? sub.onError(this.terminalError) : sub.onComplete();
            }
        } finally {
            entry.draining = false;
        }
    }
}
