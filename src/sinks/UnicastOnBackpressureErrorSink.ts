import {Sink} from "@/sinks/Sink";
import {Publisher} from "@/publishers";
import {Subscriber, Subscription} from "@/subscriptions";

export default class UnicastOnBackpressureErrorSink<T> implements Sink<T>, Publisher<T> {
    private demand: number = 0;
    private subscriber: Subscriber<T> | null = null;
    private cancelled: boolean = false;
    private terminated: boolean = false;
    private terminalError: Error | null = null;

    next(value: T): void {
        if (this.terminated || this.cancelled || !this.subscriber) return;
        if (this.demand <= 0) {
            this.error(new Error('UnicastOnBackpressureErrorSink: subscriber cannot keep up with demand'));
            return;
        }
        this.demand--;
        this.subscriber.onNext(value);
    }

    error(error: Error): void {
        if (this.terminated) return;
        this.terminated = true;
        this.terminalError = error;
        if (this.subscriber && !this.cancelled) {
            this.subscriber.onError(error);
        }
    }

    complete(): void {
        if (this.terminated) return;
        this.terminated = true;
        if (this.subscriber && !this.cancelled) {
            this.subscriber.onComplete();
        }
    }

    subscribe(subscriber: Subscriber<T>): Subscription {
        if (this.subscriber !== null) {
            subscriber.onError(new Error('UnicastOnBackpressureErrorSink allows only one subscriber'));
            return { request() {}, unsubscribe() {} };
        }
        if (this.terminated) {
            this.terminalError
                ? subscriber.onError(this.terminalError)
                : subscriber.onComplete();
            return { request() {}, unsubscribe() {} };
        }
        this.subscriber = subscriber;
        return {
            request: (n: number) => {
                if (this.cancelled) return;
                // Rule 3.9: request(n ≤ 0) MUST signal onError
                if (n <= 0) {
                    this.cancelled = true;
                    const s = this.subscriber;
                    this.subscriber = null;
                    s?.onError(new Error(`request must be > 0, but was ${n}`));
                    return;
                }
                this.demand = Math.min(this.demand + n, Number.MAX_SAFE_INTEGER);
            },
            unsubscribe: () => {
                this.cancelled = true;
                this.subscriber = null;
            }
        };
    }
}
