import {AbstractUnicastSink} from "@/sinks/internal/AbstractUnicastSink";

export default class UnicastOnBackpressureErrorSink<T> extends AbstractUnicastSink<T> {

    next(value: T): void {
        if (this.terminated || this.cancelled || !this.subscriber) return;
        if (this.demand <= 0) {
            this.error(new Error('UnicastOnBackpressureErrorSink: subscriber cannot keep up with demand'));
            return;
        }
        this.demand--;
        this.subscriber.onNext(value);
    }
}
