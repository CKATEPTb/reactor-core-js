import {BackpressureSink} from "@/sinks/BackpressureSink";
import {Subscriber} from "@/subscriptions/Subscriber";
import {Subscription} from "@/subscriptions/Subscription";

export default class OneSink<T> extends BackpressureSink<T> {
    public override subscribe(subscriber: Subscriber<T>): Subscription {
        if (this.subscribers.size > 0) {
            throw new Error("Only one subscriber is allowed for OneSink.")
        }
        return super.subscribe(subscriber)
    }

    public override next(value: T): void {
        super.next(value)
        this.complete()
    }

    public error(error: Error): void {
        super.error(error)
        this.complete()
    }
}
