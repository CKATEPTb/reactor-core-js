import {Sink} from "@/sinks/Sink";
import {Publisher} from "@/publishers";
import {Subscriber, Subscription} from "@/subscriptions";

type Entry = { demand: number; cancelled: boolean };

export default class MulticastDirectAllOrNothingSink<T> implements Sink<T>, Publisher<T> {
    private readonly entries = new Map<Subscriber<T>, Entry>();
    private terminated: boolean = false;
    private terminalError: Error | null = null;

    next(value: T): void {
        if (this.terminated) return;
        const active = [...this.entries.values()].filter(e => !e.cancelled);
        if (active.length === 0 || !active.every(e => e.demand > 0)) return;
        for (const [sub, entry] of this.entries) {
            if (!entry.cancelled) {
                entry.demand--;
                sub.onNext(value);
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
            if (!entry.cancelled) sub.onComplete();
        }
        this.entries.clear();
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
        const entry: Entry = { demand: 0, cancelled: false };
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
