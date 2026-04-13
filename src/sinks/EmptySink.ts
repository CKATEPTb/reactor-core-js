import {Sink} from "@/sinks/Sink";
import {Publisher} from "@/publishers";
import {Subscriber, Subscription} from "@/subscriptions";

export default class EmptySink<T> implements Sink<T>, Publisher<T> {
    complete(): void {
        // todo
    }

    error(error: Error): void {
        // todo
    }

    next(value: T): void {
        // todo
    }

    subscribe(subscriber: Subscriber<T>): Subscription {
        return {
            request(count: number) {
                // todo
            },
            unsubscribe() {
                // todo
            }
        };
    }

}