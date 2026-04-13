import {Sink} from "@/sinks/Sink";
import {Publisher} from "@/publishers";
import {Subscriber, Subscription} from "@/subscriptions";

// Rule 3.16: draining flag per subscriber prevents re-entrant drain (onNext → request → drain)
type Entry<T> = { buffer: T[]; demand: number; cancelled: boolean; draining: boolean };

export default class MulticastOnBackpressureBufferSink<T> implements Sink<T>, Publisher<T> {
    private readonly entries = new Map<Subscriber<T>, Entry<T>>();
    private readonly bufferSize: number;
    private readonly autoCancel: boolean;
    private terminated: boolean = false;
    private terminalError: Error | null = null;

    constructor(bufferSize: number = 256, autoCancel: boolean = true) {
        this.bufferSize = bufferSize;
        this.autoCancel = autoCancel;
    }

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

    error(error: Error): void {
        if (this.terminated) return;
        this.terminated = true;
        this.terminalError = error;
        for (const [sub, entry] of this.entries) {
            if (!entry.cancelled) sub.onError(error);
        }
        this.entries.clear();
    }

    complete(): void {
        if (this.terminated) return;
        this.terminated = true;
        for (const [sub, entry] of this.entries) {
            if (!entry.cancelled) this.drainEntry(sub, entry);
        }
    }

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

    subscribe(subscriber: Subscriber<T>): Subscription {
        if (this.terminated) {
            const sub = { request() {}, unsubscribe() {} };
            subscriber.onSubscribe(sub);
            this.terminalError
                ? subscriber.onError(this.terminalError)
                : subscriber.onComplete();
            return sub;
        }
        const entry: Entry<T> = { buffer: [], demand: 0, cancelled: false, draining: false };
        this.entries.set(subscriber, entry);
        const sub = {
            request: (n: number) => {
                if (entry.cancelled) return;
                // Rule 3.9: request(n ≤ 0) MUST signal onError
                if (n <= 0) {
                    entry.cancelled = true;
                    this.entries.delete(subscriber);
                    subscriber.onError(new Error(`request must be > 0, but was ${n}`));
                    return;
                }
                entry.demand = Math.min(entry.demand + n, Number.MAX_SAFE_INTEGER);
                this.drainEntry(subscriber, entry);
            },
            unsubscribe: () => {
                entry.cancelled = true;
                this.entries.delete(subscriber);
                if (this.autoCancel && this.entries.size === 0) this.terminated = true;
            }
        };
        subscriber.onSubscribe(sub);
        return sub;
    }
}
