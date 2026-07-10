/**
 * @packageDocumentation
 * Reactive Streams subscription contract.
 */

/** Reactive Streams subscription used to signal demand and cancellation. */
export interface Subscription {
    /** Requests a strictly positive amount of items from the upstream publisher. */
    request(n: number): void;

    /** Cancels the subscription and releases upstream resources. */
    cancel(): void;
}
