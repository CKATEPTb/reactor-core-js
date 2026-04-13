import {Subscriber} from "@/subscriptions/Subscriber";
import {Subscription} from "@/subscriptions/Subscription";

/**
 * Represents a generic publisher that can subscribe to a data stream.
 * @template T - The type of data being published.
 */
export interface Publisher<T> {
    /**
     * Subscribes a subscriber to the publisher.
     * @param {Subscriber<T>} subscriber - The subscriber to receive published data.
     * @returns {Subscription} A subscription object to manage the subscriber's lifecycle.
     */
    subscribe(subscriber: Subscriber<T>): Subscription
}