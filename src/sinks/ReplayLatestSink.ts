import {AbstractMulticastSink} from "@/sinks/internal/AbstractMulticastSink";
import {Subscriber} from "@/subscriptions";

// Rule 3.16: draining flag prevents re-entrant drain (onNext → request → drain)
type Entry<T> = { replay: T[]; demand: number; cancelled: boolean; draining: boolean };

/**
 * Replays the latest `limit` emitted items to each new subscriber.
 * Used for both `Sinks.many().replay().latest(limit)` and `Sinks.many().replay().limit(limit)`.
 */
export default class ReplayLatestSink<T> extends AbstractMulticastSink<T, Entry<T>> {

    private readonly limit: number;
    private readonly history: T[] = [];

    constructor(limit: number) {
        super();
        this.limit = limit;
    }

    // ──── AbstractMulticastSink hooks ─────────────────────────────────────────

    protected createEntry(): Entry<T> {
        return { replay: [...this.history], demand: 0, cancelled: false, draining: false };
    }

    protected shouldReplayTerminalOnSubscribe(): boolean {
        return this.terminated && this.history.length === 0;
    }

    /** Drain buffered replay queue first, then signal error. */
    protected deliverError(sub: Subscriber<T>, entry: Entry<T>): void {
        this.drainEntry(sub, entry);
        if (!entry.cancelled) sub.onError(this.terminalError!);
    }

    /** Drain buffered replay queue — drainEntry signals complete when done. */
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
        this.history.push(value);
        if (this.history.length > this.limit) this.history.shift();
        for (const [sub, entry] of this.entries) {
            if (!entry.cancelled) {
                entry.replay.push(value);
                this.drainEntry(sub, entry);
            }
        }
    }

    // ──── Internal drain ──────────────────────────────────────────────────────

    private drainEntry(sub: Subscriber<T>, entry: Entry<T>): void {
        if (entry.cancelled || entry.draining) return;
        entry.draining = true;
        try {
            while (entry.demand > 0 && entry.replay.length > 0) {
                entry.demand--;
                sub.onNext(entry.replay.shift()!);
                if (entry.cancelled) return;
            }
            if (entry.replay.length === 0 && this.terminated) {
                this.entries.delete(sub);
                this.terminalError ? sub.onError(this.terminalError) : sub.onComplete();
            }
        } finally {
            entry.draining = false;
        }
    }
}
