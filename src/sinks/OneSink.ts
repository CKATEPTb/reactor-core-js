import {BackpressureSink} from "@/sinks/BackpressureSink";
import {Subscriber} from "@/subscriptions/Subscriber";
import {Subscription} from "@/subscriptions/Subscription";

/**
 * A sink that only allows a single subscriber and emits one value before completing.
 * Extends `BackpressureSink` and enforces a unicast pattern.
 * Suitable for scenarios where only one data emission is expected.
 *
 * @template T - The type of data being emitted.
 */
export default class OneSink<T> extends BackpressureSink<T> {
    /**
     * Subscribes a single subscriber to the sink.
     * Throws an error if a second subscriber attempts to subscribe.
     *
     * @param {Subscriber<T>} subscriber - The subscriber to add.
     * @returns {Subscription} The subscription object for managing the subscriber's lifecycle.
     * @throws {Error} If more than one subscriber attempts to subscribe.
     */
    public override subscribe(subscriber: Subscriber<T>): Subscription {
        if (this.subscribers.size > 0) {
            throw new Error("Only one subscriber is allowed for OneSink.")
        }
        return super.subscribe(subscriber)
    }

    /**
     * Emits a single value and immediately completes the stream.
     * Suitable for cases where only one value is expected.
     *
     * @param {T} value - The value to emit.
     */
    public override next(value: T): void {
        super.next(value)
        this.complete()
    }

    /**
     * Emits an error and immediately completes the stream.
     * Ensures that the stream is closed after an error is emitted.
     *
     * @param {Error} error - The error to emit.
     */
    public error(error: Error): void {
        super.error(error)
        this.complete()
    }
}
