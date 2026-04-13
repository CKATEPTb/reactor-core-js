import {Sink} from "@/sinks/Sink";
import {Publisher} from "@/publishers";
import {Subscriber, Subscription} from "@/subscriptions";

// Rule 3.16: draining flag prevents re-entrant drain (onNext → request → drain)
type Entry<T> = { replay: T[]; demand: number; cancelled: boolean; draining: boolean };

/**
 * Replays the latest `limit` emitted items to each new subscriber.
 * Used for both `Sinks.many().replay().latest(limit)` and `Sinks.many().replay().limit(limit)`.
 */
export default class ReplayLatestSink<T> implements Sink<T>, Publisher<T> {
    private readonly limit: number;
    private readonly history: T[] = [];
    private readonly entries = new Map<Subscriber<T>, Entry<T>>();
    private terminated: boolean = false;
    private terminalError: Error | null = null;

    constructor(limit: number) {
        this.limit = limit;
    }

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

    error(error: Error): void {
        if (this.terminated) return;
        this.terminated = true;
        this.terminalError = error;
        for (const [sub, entry] of this.entries) {
            if (!entry.cancelled) {
                this.drainEntry(sub, entry);
                if (!entry.cancelled) sub.onError(error);
            }
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

    subscribe(subscriber: Subscriber<T>): Subscription {
        if (this.terminated && this.history.length === 0) {
            const sub = { request() {}, unsubscribe() {} };
            subscriber.onSubscribe(sub);
            this.terminalError
                ? subscriber.onError(this.terminalError)
                : subscriber.onComplete();
            return sub;
        }
        const entry: Entry<T> = { replay: [...this.history], demand: 0, cancelled: false, draining: false };
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
            }
        };
        subscriber.onSubscribe(sub);
        return sub;
    }
}
