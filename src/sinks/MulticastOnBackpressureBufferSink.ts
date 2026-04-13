import {AbstractMulticastSink} from "@/sinks/internal/AbstractMulticastSink";
import {Subscriber} from "@/subscriptions";

// Rule 3.16: draining flag per subscriber prevents re-entrant drain (onNext → request → drain)
type Entry<T> = { buffer: T[]; demand: number; cancelled: boolean; draining: boolean };

export default class MulticastOnBackpressureBufferSink<T>
    extends AbstractMulticastSink<T, Entry<T>> {

    private readonly bufferSize: number;
    private readonly autoCancel: boolean;

    constructor(bufferSize: number = 256, autoCancel: boolean = true) {
        super();
        this.bufferSize = bufferSize;
        this.autoCancel = autoCancel;
    }

    // ──── AbstractMulticastSink hooks ─────────────────────────────────────────

    protected createEntry(): Entry<T> {
        return { buffer: [], demand: 0, cancelled: false, draining: false };
    }

    /** Drain buffered items for each subscriber instead of completing immediately. */
    protected deliverComplete(sub: Subscriber<T>, entry: Entry<T>): void {
        this.drainEntry(sub, entry);
    }

    /** drainEntry handles its own cleanup — don't clear the map eagerly. */
    protected clearEntriesAfterComplete(): boolean {
        return false;
    }

    protected onDemandGranted(sub: Subscriber<T>, entry: Entry<T>): void {
        this.drainEntry(sub, entry);
    }

    protected onUnsubscribed(_sub: Subscriber<T>, _entry: Entry<T>): void {
        if (this.autoCancel && this.entries.size === 0) this.terminated = true;
    }

    // ──── Sink.next ───────────────────────────────────────────────────────────

    next(value: T): void {
        if (this.terminated) return;
        for (const [sub, entry] of this.entries) {
            if (entry.cancelled) continue;
            if (entry.demand > 0) {
                entry.demand--;
                sub.onNext(value);
            } else if (entry.buffer.length < this.bufferSize) {
                entry.buffer.push(value);
            } else {
                entry.cancelled = true;
                this.entries.delete(sub);
                sub.onError(new Error('MulticastOnBackpressureBufferSink: buffer overflow'));
                if (this.autoCancel && this.entries.size === 0) this.terminated = true;
            }
        }
    }

    // ──── Internal drain ──────────────────────────────────────────────────────

    private drainEntry(sub: Subscriber<T>, entry: Entry<T>): void {
        if (entry.cancelled || entry.draining) return;
        entry.draining = true;
        try {
            while (entry.demand > 0 && entry.buffer.length > 0) {
                entry.demand--;
                sub.onNext(entry.buffer.shift()!);
                if (entry.cancelled) return;
            }
            if (entry.buffer.length === 0 && this.terminated) {
                this.entries.delete(sub);
                this.terminalError ? sub.onError(this.terminalError) : sub.onComplete();
            }
        } finally {
            entry.draining = false;
        }
    }
}
