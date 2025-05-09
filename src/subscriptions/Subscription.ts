/**
 * Represents a subscription to a Publisher.
 * Manages the flow of data and provides the ability to cancel the subscription.
 */
export interface Subscription {
    /**
     * Requests a specified number of items from the Publisher.
     * Allows controlling backpressure by specifying how many items to emit.
     * @param {number} count - The number of items to request from the Publisher.
     */
    request(count: number): void

    /**
     * Cancels the subscription, stopping any further emissions.
     * Unsubscribing releases resources associated with the subscription.
     */
    unsubscribe(): void
}