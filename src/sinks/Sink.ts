/**
 * Represents a sink that consumes emitted values, errors, and completion signals.
 * Typically used as the endpoint in a reactive stream to receive and handle data.
 *
 * @template T - The type of data being consumed.
 */
export interface Sink<T> {
    /**
     * Consumes the next emitted value.
     * Called when a new value is pushed to the sink.
     * @param {T} value - The value to be processed.
     */
    next(value: T): void

    /**
     * Handles an error emitted during the data stream.
     * Called when an error occurs within the publisher.
     * @param {Error} error - The error encountered.
     */
    error(error: Error): void

    /**
     * Signals the completion of the data stream.
     * Called when the publisher has finished emitting all values.
     */
    complete(): void
}

