import {AbstractMulticastSink} from "@/sinks/internal/AbstractMulticastSink";
import {Subscriber} from "@/subscriptions";

// Rule 3.16: draining flag prevents re-entrant drain (onNext → request → drain)
type Entry = { position: number; demand: number; cancelled: boolean; draining: boolean };

export default class ReplayAllSink<T> extends AbstractMulticastSink<T, Entry> {

    private readonly history: T[] = [];

    // ──── AbstractMulticastSink hooks ─────────────────────────────────────────

    protected createEntry(): Entry {
        return { position: 0, demand: 0, cancelled: false, draining: false };
    }

    /** Only replay terminal immediately if there's nothing in history to deliver. */
    protected shouldReplayTerminalOnSubscribe(): boolean {
        return this.terminated && this.history.length === 0;
    }

    /** Drain buffered history first, then signal error. */
    protected deliverError(sub: Subscriber<T>, entry: Entry): void {
        this.drainEntry(sub, entry);
        if (!entry.cancelled) sub.onError(this.terminalError!);
    }

    /** Drain buffered history — drainEntry signals complete when done. */
    protected deliverComplete(sub: Subscriber<T>, entry: Entry): void {
        if (!entry.cancelled) this.drainEntry(sub, entry);
    }

    protected clearEntriesAfterComplete(): boolean {
        return false;
    }

    protected onDemandGranted(sub: Subscriber<T>, entry: Entry): void {
        this.drainEntry(sub, entry);
    }

    // ──── Sink.next ───────────────────────────────────────────────────────────

    next(value: T): void {
        if (this.terminated) return;
        this.history.push(value);
        for (const [sub, entry] of this.entries) {
            if (!entry.cancelled) this.drainEntry(sub, entry);
        }
    }

    // ──── Internal drain ──────────────────────────────────────────────────────

    private drainEntry(sub: Subscriber<T>, entry: Entry): void {
        if (entry.cancelled || entry.draining) return;
        entry.draining = true;
        try {
            while (entry.demand > 0 && entry.position < this.history.length) {
                entry.demand--;
                sub.onNext(this.history[entry.position++]);
                if (entry.cancelled) return;
            }
            if (entry.position >= this.history.length && this.terminated) {
                this.entries.delete(sub);
                this.terminalError ? sub.onError(this.terminalError) : sub.onComplete();
            }
        } finally {
            entry.draining = false;
        }
    }
}
