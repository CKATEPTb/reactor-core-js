import {Sink} from "@/sinks/Sink";
import {Publisher} from "@/publishers";
import {Subscriber, Subscription} from "@/subscriptions";

export default class UnicastOnBackpressureBufferSink<T> implements Sink<T>, Publisher<T> {
    private readonly buffer: T[] = [];
    private demand: number = 0;
    private subscriber: Subscriber<T> | null = null;
    private cancelled: boolean = false;
    private terminated: boolean = false;
    private terminalError: Error | null = null;
    // Rule 3.16: guard against re-entrant drain (onNext → request → drain)
    private draining: boolean = false;

    next(value: T): void {
        if (this.terminated || this.cancelled) return;
        this.buffer.push(value);
        this.drain();
    }

    error(error: Error): void {
        if (this.terminated) return;
        this.terminated = true;
        this.terminalError = error;
        this.drain();
    }

    complete(): void {
        if (this.terminated) return;
        this.terminated = true;
        this.drain();
    }

    private drain(): void {
        if (!this.subscriber || this.cancelled || this.draining) return;
        this.draining = true;
        try {
            while (this.demand > 0 && this.buffer.length > 0) {
                this.demand--;
                this.subscriber.onNext(this.buffer.shift()!);
                if (this.cancelled) return;
            }
            if (this.buffer.length === 0 && this.terminated) {
                const s = this.subscriber;
                this.subscriber = null;
                this.terminalError ? s.onError(this.terminalError) : s.onComplete();
            }
        } finally {
            this.draining = false;
        }
    }

    subscribe(subscriber: Subscriber<T>): Subscription {
        if (this.subscriber !== null) {
            subscriber.onError(new Error('UnicastOnBackpressureBufferSink allows only one subscriber'));
            return { request() {}, unsubscribe() {} };
        }
        this.subscriber = subscriber;
        this.drain();
        return {
            request: (n: number) => {
                if (this.cancelled) return;
                // Rule 3.9: request(n ≤ 0) MUST signal onError
                if (n <= 0) {
                    this.cancelled = true;
                    this.buffer.length = 0;
                    const s = this.subscriber;
                    this.subscriber = null;
                    s?.onError(new Error(`request must be > 0, but was ${n}`));
                    return;
                }
                this.demand = Math.min(this.demand + n, Number.MAX_SAFE_INTEGER);
                this.drain();
            },
            unsubscribe: () => {
                this.cancelled = true;
                this.subscriber = null;
                this.buffer.length = 0;
            }
        };
    }
}
