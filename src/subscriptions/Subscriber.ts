import {Subscription} from "@/subscriptions/Subscription";

/**
 * Represents a subscriber that listens to events emitted by a Publisher.
 *
 * Reactive Streams spec: the Publisher MUST call onSubscribe before any other signal.
 * The Subscriber SHOULD call Subscription.request(n) from within onSubscribe.
 *
 * @template T - The type of data that the subscriber will receive.
 */
export interface Subscriber<T> {
    /**
     * Called once by the Publisher immediately after subscribe().
     * MUST be called before onNext, onError, or onComplete.
     * @param {Subscription} subscription - The subscription controlling demand and cancellation.
     */
    onSubscribe(subscription: Subscription): void

    /**
     * Called when the next item is emitted.
     * @param {T} value - The next value emitted by the Publisher.
     */
    onNext(value: T): void

    /**
     * Called when an error occurs during data emission.
     * @param {Error} error - The error encountered.
     */
    onError(error: Error): void

    /**
     * Called when the Publisher has successfully completed emitting all items.
     */
    onComplete(): void
}
