/**
 * Represents a subscriber that listens to events emitted by a Publisher.
 *
 * @template T - The type of data that the subscriber will receive.
 */
export interface Subscriber<T> {
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