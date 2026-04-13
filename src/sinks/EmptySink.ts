import {Sink} from "@/sinks/Sink";
import {Publisher} from "@/publishers";
import {Subscriber, Subscription} from "@/subscriptions";

type Terminal = { kind: 'completed' } | { kind: 'error'; error: Error };

export default class EmptySink<T> implements Sink<T>, Publisher<T> {
    private terminal: Terminal | null = null;
    private readonly subscribers = new Set<Subscriber<T>>();

    next(_value: T): void {}

    error(error: Error): void {
        if (this.terminal) return;
        this.terminal = { kind: 'error', error };
        for (const s of this.subscribers) s.onError(error);
        this.subscribers.clear();
    }

    complete(): void {
        if (this.terminal) return;
        this.terminal = { kind: 'completed' };
        for (const s of this.subscribers) s.onComplete();
        this.subscribers.clear();
    }

    subscribe(subscriber: Subscriber<T>): Subscription {
        if (this.terminal) {
            this.terminal.kind === 'completed'
                ? subscriber.onComplete()
                : subscriber.onError(this.terminal.error);
            return { request() {}, unsubscribe() {} };
        }
        this.subscribers.add(subscriber);
        return {
            // Rule 3.9: request(n ≤ 0) MUST signal onError
            request: (n: number) => {
                if (!this.subscribers.has(subscriber)) return;
                if (n <= 0) {
                    this.subscribers.delete(subscriber);
                    subscriber.onError(new Error(`request must be > 0, but was ${n}`));
                }
            },
            unsubscribe: () => { this.subscribers.delete(subscriber); }
        };
    }
}
