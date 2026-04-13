import {Sink} from "@/sinks/Sink";
import {Publisher} from "@/publishers";
import {Subscriber, Subscription} from "@/subscriptions";

// Rule 3.16: draining flag prevents re-entrant drain (onNext → request → drain)
type Entry<T> = { pending: T; hasPending: boolean; demand: number; cancelled: boolean; draining: boolean };

/**
 * Replays the latest emitted item to new subscribers, or the default value if no item was emitted yet.
 */
export default class ReplayLatestOrDefaultSink<T> implements Sink<T>, Publisher<T> {
    private latestValue: T;
    private readonly entries = new Map<Subscriber<T>, Entry<T>>();
    private terminated: boolean = false;
    private terminalError: Error | null = null;

    constructor(defaultValue: T) {
        this.latestValue = defaultValue;
    }

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

    subscribe(subscriber: Subscriber<T>): Subscription {
        if (this.terminated && this.terminalError) {
            subscriber.onError(this.terminalError);
            return { request() {}, unsubscribe() {} };
        }
        // New subscriber always starts with latestValue (or default) as pending
        const entry: Entry<T> = {
            pending: this.latestValue,
            hasPending: true,
            demand: 0,
            cancelled: false,
            draining: false
        };
        this.entries.set(subscriber, entry);
        return {
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
    }
}
