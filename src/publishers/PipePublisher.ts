import {Publisher} from "@/publishers/Publisher";
import {Subscription} from "@/subscriptions/Subscription";
import {Scheduler} from "@/schedulers/Scheduler";

/**
 * An interface representing a publisher that supports data transformation and manipulation through pipes.
 * @template T - The type of data being published.
 */
export interface PipePublisher<T> extends Publisher<T> {
    /**
     * Pipes the data through custom transformations.
     * @template R - The result type after processing.
     * @param {Function} producer - The function to produce new values.
     * @param {Function} onSubscribe - Callback on subscription.
     * @param {Function} onRequest - Callback on request.
     * @param {Function} onUnsubscribe - Callback on unsubscribe.
     * @returns {PipePublisher<R>} A new pipe publisher with transformed data.
     */
    pipe<R>(producer: (onNext: (value: R) => void, onError: (error: Error) => void, onComplete: () => void) => void, onRequest: (request: number) => void, onUnsubscribe: () => void): PipePublisher<R>

    /**
     * Transforms each emitted value using the given function.
     * @template R - The transformed data type.
     * @param {Function} fn - The mapping function.
     * @returns {PipePublisher<R>} A new pipe publisher with mapped data.
     */
    map<R>(fn: (value: T) => R): PipePublisher<R>

    /**
     * Transforms each emitted value and filter transform result.
     * @template R - The transformed data type.
     * @param {Function} fn - The mapping function that can return null or undefined.
     * @returns {PipePublisher<R>} A new pipe publisher with mapped non-null data.
     */
    mapNotNull<R>(fn: (value: T) => R | null | undefined): PipePublisher<R>

    /**
     * Transforms the value using a function that returns a new publisher.
     * @template R - The resulting data type.
     * @param {Function} fn - The function to transform values into new publishers.
     * @returns {PipePublisher<R>} A pipe publisher that flattens the result.
     */
    flatMap<R>(fn: (value: T) => Publisher<R>): PipePublisher<R>

    /**
     * Filters emitted values using a given predicate.
     * @param {Function} predicate - The function to determine whether to emit a value.
     * @returns {PipePublisher<T>} A new pipe publisher with filtered data.
     */
    filter(predicate: (value: T) => boolean): PipePublisher<T>

    /**
     * Filters emitted values based on a publisher that returns a boolean.
     * @param {Function} predicate - A function returning a boolean publisher.
     * @returns {PipePublisher<T>} A new pipe publisher with conditional data.
     */
    filterWhen(predicate: (value: T) => Publisher<boolean>): PipePublisher<T>

    /**
     * Casts the current publisher to another type.
     * @template R - The target type.
     * @returns {PipePublisher<R>} The casted pipe publisher.
     */
    cast<R>(): PipePublisher<R>

    /**
     * Switches to an alternative publisher if the current one is empty.
     * @param {Publisher<T>} alternative - The alternative publisher.
     * @returns {PipePublisher<T>} A new pipe publisher.
     */
    switchIfEmpty(alternative: Publisher<T>): PipePublisher<T>

    /**
     * Continues with a replacement publisher if an error occurs.
     * @param {Publisher<T>} replacement - The publisher to switch to on error.
     * @returns {PipePublisher<T>} A new pipe publisher.
     */
    onErrorReturn(replacement: Publisher<T>): PipePublisher<T>

    /**
     * Continues processing even if an error occurs based on a predicate.
     * @param {Function} predicate - Function to determine whether to continue on error.
     * @returns {PipePublisher<T>} A new pipe publisher.
     */
    onErrorContinue(predicate: (error: Error) => boolean): PipePublisher<T>

    /**
     * Executes a function when the first value is emitted.
     * @param {Function} fn - The function to execute.
     * @returns {PipePublisher<T>} A new pipe publisher.
     */
    doFirst(fn: () => void): PipePublisher<T>

    /**
     * Executes a function when each value is emitted.
     * @param {Function} fn - The function to execute on each value.
     * @returns {PipePublisher<T>} A new pipe publisher.
     */
    doOnNext(fn: (value: T) => void): PipePublisher<T>

    /**
     * Executes a function when each error is emitted.
     * @param {Function} fn - The function to execute on each error.
     * @returns {PipePublisher<T>} A new pipe publisher.
     */
    doOnError(fn: (value: Error) => void): PipePublisher<T>

    /**
     * Executes a function when the stream completes.
     * @param {Function} fn - The function to execute on completion.
     * @returns {PipePublisher<T>} A new pipe publisher.
     */
    doFinally(fn: () => void): PipePublisher<T>

    /**
     * Executes a function when a subscription occurs.
     * @param {Function} fn - The function to execute on subscription.
     * @returns {PipePublisher<T>} A new pipe publisher.
     */
    doOnSubscribe(fn: (subscription: Subscription) => void): PipePublisher<T>

    /**
     * Publishes values on a specified scheduler.
     * @param {Scheduler} scheduler - The scheduler to use.
     * @returns {PipePublisher<T>} A new pipe publisher.
     */
    publishOn(scheduler: Scheduler): PipePublisher<T>

    /**
     * Subscribes to the stream on a specified scheduler.
     * @param {Scheduler} scheduler - The scheduler to use.
     * @returns {PipePublisher<T>} A new pipe publisher.
     */
    subscribeOn(scheduler: Scheduler): PipePublisher<T>
}