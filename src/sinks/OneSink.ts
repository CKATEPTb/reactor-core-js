import {Sink} from "@/sinks/Sink";
import {Flux, Publisher} from "@/publishers";
import {Subscriber, Subscription} from "@/subscriptions";

// Rule 1.1: demand tracked per subscriber — onNext only sent when demand >= 1
type Entry = { demand: number; cancelled: boolean; completionQueued: boolean };

export default class OneSink<T> implements Sink<T>, Publisher<T> {
    private pendingValue: { value: T } | null = null;
    private completed: boolean = false;
    private terminalError: Error | null = null;
    private readonly entries = new Map<Subscriber<T>, Entry>();

    private get terminated(): boolean {
        return this.completed || this.terminalError !== null;
    }

    next(value: T): void {
        if (this.terminated || this.pendingValue !== null) return;
        this.pendingValue = { value };
    }

    error(error: Error): void {
        if (this.terminated) return;
        this.terminalError = error;
        for (const [s, e] of this.entries) {
            if (!e.cancelled) s.onError(error);
        }
        this.entries.clear();
    }

    complete(): void {
        if (this.terminated) return;
        this.completed = true;
        for (const [s, entry] of this.entries) {
            if (entry.cancelled) continue;
            if (this.pendingValue === null) {
                s.onComplete();
                this.entries.delete(s);
            } else if (entry.demand >= 1) {
                entry.demand--;
                s.onNext(this.pendingValue.value);
                if (!entry.cancelled) s.onComplete();
                this.entries.delete(s);
            } else {
                // No demand yet — defer delivery until request(n)
                entry.completionQueued = true;
            }
        }
    }

    asFlux(): Flux<T> {
        return Flux.from(this);
    }

    subscribe(subscriber: Subscriber<T>): Subscription {
        if (this.terminalError) {
            const sub = { request() {}, unsubscribe() {} };
            subscriber.onSubscribe(sub);
            subscriber.onError(this.terminalError);
            return sub;
        }

        const entry: Entry = { demand: 0, cancelled: false, completionQueued: this.completed };
        this.entries.set(subscriber, entry);

        // Already completed with no value — signal immediately
        if (this.completed && this.pendingValue === null) {
            this.entries.delete(subscriber);
            const sub = { request() {}, unsubscribe() {} };
            subscriber.onSubscribe(sub);
            subscriber.onComplete();
            return sub;
        }

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
                // Deliver queued value now that demand is available
                if (entry.completionQueued && this.pendingValue !== null && entry.demand >= 1) {
                    entry.demand--;
                    entry.cancelled = true;
                    this.entries.delete(subscriber);
                    subscriber.onNext(this.pendingValue.value);
                    subscriber.onComplete();
                }
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
