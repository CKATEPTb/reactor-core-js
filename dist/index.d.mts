/**
 * Represents a subscriber that listens to events emitted by a Publisher.
 *
 * @template T - The type of data that the subscriber will receive.
 */
interface Subscriber<T> {
    /**
     * Called when the next item is emitted.
     * @param {T} value - The next value emitted by the Publisher.
     */
    onNext(value: T): void;
    /**
     * Called when an error occurs during data emission.
     * @param {Error} error - The error encountered.
     */
    onError(error: Error): void;
    /**
     * Called when the Publisher has successfully completed emitting all items.
     */
    onComplete(): void;
}

/**
 * Represents a subscription to a Publisher.
 * Manages the flow of data and provides the ability to cancel the subscription.
 */
interface Subscription {
    /**
     * Requests a specified number of items from the Publisher.
     * Allows controlling backpressure by specifying how many items to emit.
     * @param {number} count - The number of items to request from the Publisher.
     */
    request(count: number): void;
    /**
     * Cancels the subscription, stopping any further emissions.
     * Unsubscribing releases resources associated with the subscription.
     */
    unsubscribe(): void;
}

/**
 * Represents a generic publisher that can subscribe to a data stream.
 * @template T - The type of data being published.
 */
interface Publisher<T> {
    /**
     * Subscribes a subscriber to the publisher.
     * @param {Subscriber<T>} subscriber - The subscriber to receive published data.
     * @returns {Subscription} A subscription object to manage the subscriber's lifecycle.
     */
    subscribe(subscriber: Subscriber<T>): Subscription;
}

/**
 * Represents a basic scheduling interface for executing tasks.
 */
interface Scheduler {
    /**
     * Schedules a task to be executed.
     * @param {Function} task - The task function to be executed.
     */
    schedule(task: () => void): void;
}
/**
 * Represents a cancellable scheduling interface, extending the basic `Scheduler` interface.
 * Provides the ability to cancel a scheduled task.
 */
interface CancellableScheduler extends Scheduler {
    /**
     * Schedules a task to be executed with the ability to cancel it.
     * @param {Function} task - The task function to be executed.
     * @returns {Object} An object containing a `cancel` method to stop the execution.
     */
    schedule(task: () => void): {
        cancel: () => void;
    };
}

/**
 * Represents a sink that consumes emitted values, errors, and completion signals.
 * Typically used as the endpoint in a reactive stream to receive and handle data.
 *
 * @template T - The type of data being consumed.
 */
interface Sink<T> {
    /**
     * Consumes the next emitted value.
     * Called when a new value is pushed to the sink.
     * @param {T} value - The value to be processed.
     */
    next(value: T): void;
    /**
     * Handles an error emitted during the data stream.
     * Called when an error occurs within the publisher.
     * @param {Error} error - The error encountered.
     */
    error(error: Error): void;
    /**
     * Signals the completion of the data stream.
     * Called when the publisher has finished emitting all values.
     */
    complete(): void;
}

/**
 * Represents a backpressure control structure.
 * Stores the subscriber, buffered data, and the number of requested items.
 *
 * @template T - The type of data being processed.
 */
type Backpressure<T> = {
    subscriber: Subscriber<T>;
    data: EmitAction<T>[];
    requested: number;
};
/**
 * Represents an action emitted by the sink.
 * Contains the type of emission (next, error, complete) and the associated data.
 *
 * @template T - The type of data being emitted.
 */
type EmitAction<T> = {
    emit: 'next' | 'error' | 'complete';
    data?: T | Error;
};
/**
 * An abstract sink that supports backpressure handling.
 * Manages the flow of data to multiple subscribers while respecting backpressure demands.
 *
 * @template T - The type of data being emitted.
 */
declare abstract class BackpressureSink<T> implements Sink<T>, Publisher<T> {
    protected readonly subscribers: Set<Backpressure<T>>;
    protected completed: boolean;
    /**
     * Subscribes a subscriber to the sink with backpressure handling.
     *
     * @param {Subscriber<T>} subscriber - The subscriber to add.
     * @returns {Subscription} The subscription object for managing the subscriber's lifecycle.
     */
    subscribe(subscriber: Subscriber<T>): Subscription;
    /**
     * Emits the next value to all subscribers.
     * Validates emission and triggers flushing of buffered data.
     *
     * @param {T} value - The value to emit.
     * @throws {Error} If the sink has already completed.
     */
    next(value: T): void;
    /**
     * Emits an error to all subscribers and marks the sink as completed.
     *
     * @param {Error} error - The error to emit.
     * @throws {Error} If the sink has already completed.
     */
    error(error: Error): void;
    /**
     * Completes the sink, notifying all subscribers.
     * Prevents further emissions and clears the subscriber set.
     */
    complete(): void;
    /**
     * Emits an action to a specific subscriber.
     * Handles 'next', 'error', and 'complete' emissions.
     *
     * @protected
     * @param {EmitAction<T>} action - The action to be emitted.
     * @param {Subscriber<T>} subscriber - The subscriber to receive the action.
     */
    protected emit(action: EmitAction<T>, subscriber: Subscriber<T>): void;
    /**
     * Validates whether the sink can emit new data.
     * Throws an error if the sink has already completed.
     *
     * @protected
     * @throws {Error} If the sink has already completed.
     */
    protected validateEmit(): void;
    /**
     * Flushes buffered data to the subscriber based on the requested count.
     * Ensures that only the requested number of items are sent.
     *
     * @private
     * @param {Backpressure<T>} backpressure - The backpressure object for the subscriber.
     */
    private flush;
}

/**
 * A sink that only allows a single subscriber and emits one value before completing.
 * Extends `BackpressureSink` and enforces a unicast pattern.
 * Suitable for scenarios where only one data emission is expected.
 *
 * @template T - The type of data being emitted.
 */
declare class OneSink<T> extends BackpressureSink<T> {
    /**
     * Subscribes a single subscriber to the sink.
     * Throws an error if a second subscriber attempts to subscribe.
     *
     * @param {Subscriber<T>} subscriber - The subscriber to add.
     * @returns {Subscription} The subscription object for managing the subscriber's lifecycle.
     * @throws {Error} If more than one subscriber attempts to subscribe.
     */
    subscribe(subscriber: Subscriber<T>): Subscription;
    /**
     * Emits a single value and immediately completes the stream.
     * Suitable for cases where only one value is expected.
     *
     * @param {T} value - The value to emit.
     */
    next(value: T): void;
    /**
     * Emits an error and immediately completes the stream.
     * Ensures that the stream is closed after an error is emitted.
     *
     * @param {Error} error - The error to emit.
     */
    error(error: Error): void;
}

/**
 * A sink that supports multiple subscribers and handles backpressure.
 * Extends `BackpressureSink` to allow broadcasting emitted values to multiple subscribers.
 *
 * @template T - The type of data being emitted.
 */
declare class ManySink<T> extends BackpressureSink<T> {
}

/**
 * A sink that stores emitted values and replays them to new subscribers.
 * Extends the `ManySink` to support replaying previously emitted events.
 * Useful in scenarios where late subscribers need to receive historical data.
 *
 * @template T - The type of data being emitted.
 */
declare abstract class ReplaySink<T> extends ManySink<T> {
    readonly buffer: EmitAction<T>[];
    /**
     * Emits a value to all current subscribers and stores it in the buffer for future replay.
     * @param {T} value - The value to emit.
     */
    next(value: T): void;
    /**
     * Emits an error to all current subscribers and stores it in the buffer for future replay.
     * @param {Error} error - The error to emit.
     */
    error(error: Error): void;
    /**
     * Signals the completion of the data stream to all current subscribers.
     * Stores the completion event in the buffer for future replay.
     */
    complete(): void;
    /**
     * Subscribes a subscriber to the sink.
     * Replays all buffered emits to the new subscriber upon subscription.
     * @param {Subscriber<T>} subscriber - The subscriber to add.
     * @returns {Subscription} The subscription object for managing the subscriber's lifecycle.
     */
    subscribe(subscriber: Subscriber<T>): Subscription;
    /**
     * Stores an emitted action (next, error, complete) in the buffer.
     * @protected
     * @param {'next' | 'error' | 'complete'} emit - The type of emission.
     * @param {T | Error} [data] - The data associated with the emission, if any.
     */
    protected store(emit: 'next' | 'error' | 'complete', data?: T | Error): void;
}

/**
 * A replay sink that retains all emitted events indefinitely.
 * Extends `ReplaySink` to store every event without any limitation.
 *
 * @template T - The type of data being emitted.
 */
declare class ReplayAllSink<T> extends ReplaySink<T> {
}

/**
 * A replay sink that only retains the latest emitted events up to a specified limit.
 * Extends `ReplaySink` to store a fixed number of the most recent events.
 *
 * @template T - The type of data being emitted.
 */
declare class ReplayLatestSink<T> extends ReplaySink<T> {
    private readonly limit;
    /**
     * Creates a new `ReplayLatestSink` with a specified limit on the number of stored events.
     * Ensures that only the most recent events are kept, discarding older ones.
     *
     * @param {number} limit - The maximum number of recent events to retain.
     * @throws {Error} If the limit is less than 1.
     */
    constructor(limit: number);
    /**
     * Stores an emitted action (next, error, complete) in the buffer.
     * Keeps only the most recent events, removing older ones when the limit is exceeded.
     *
     * @protected
     * @param {'next' | 'error' | 'complete'} emit - The type of emission.
     * @param {T | Error} [data] - The data associated with the emission, if any.
     */
    protected store(emit: "next" | "error" | "complete", data?: Error | T): void;
}

/**
 * A replay sink that limits the number of stored events.
 * Extends `ReplaySink` to restrict the buffer size to a specified limit.
 *
 * @template T - The type of data being emitted.
 */
declare class ReplayLimitSink<T> extends ReplaySink<T> {
    private readonly limit;
    /**
     * Creates a new `ReplayLimitSink` with a specified limit on the number of stored events.
     * Throws an error if the limit is less than 1.
     *
     * @param {number} limit - The maximum number of events to retain in the buffer.
     * @throws {Error} If the limit is less than 1.
     */
    constructor(limit: number);
    /**
     * Stores an emitted action (next, error, complete) in the buffer.
     * Ensures that the buffer does not exceed the specified limit.
     * @protected
     * @param {'next' | 'error' | 'complete'} emit - The type of emission.
     * @param {T | Error} [data] - The data associated with the emission, if any.
     */
    protected store(emit: "next" | "error" | "complete", data?: Error | T): void;
}

/**
 * A collection of commonly used sinks for data emission and subscription management.
 * Provides factory functions for creating instances of various sink types.
 */
declare const Sinks: {
    /**
     * Creates a new `OneSink` instance.
     * Suitable for single-value emissions with unicast behavior.
     *
     * @template T - The type of data being emitted.
     * @returns {OneSink<T>} An instance of `OneSink`.
     */
    one: <T>() => OneSink<T>;
    many: () => {
        /**
         * Creates a new `ManySink` instance for multicast data emission.
         * Allows broadcasting data to multiple subscribers.
         *
         * @template T - The type of data being emitted.
         * @returns {ManySink<T>} An instance of `ManySink`.
         */
        multicast: <T>() => ManySink<T>;
        replay: () => {
            /**
             * Creates a `ReplayAllSink` instance that stores all emitted events.
             * Allows replaying the entire event history to new subscribers.
             *
             * @template T - The type of data being emitted.
             * @returns {ReplayAllSink<T>} An instance of `ReplayAllSink`.
             */
            all: <T>() => ReplayAllSink<T>;
            /**
             * Creates a `ReplayLatestSink` instance that stores the most recent N events.
             * Allows replaying the latest events to new subscribers.
             *
             * @template T - The type of data being emitted.
             * @param {number} limit - The maximum number of recent events to retain.
             * @returns {ReplayLatestSink<T>} An instance of `ReplayLatestSink`.
             * @throws {Error} If the limit is less than 1.
             */
            latest: <T>(limit: number) => ReplayLatestSink<T>;
            /**
             * Creates a `ReplayLimitSink` instance that stores up to a specified number of events.
             * Useful when keeping the entire event history is unnecessary.
             *
             * @template T - The type of data being emitted.
             * @param {number} limit - The maximum number of events to retain.
             * @returns {ReplayLimitSink<T>} An instance of `ReplayLimitSink`.
             * @throws {Error} If the limit is less than 1.
             */
            limit: <T>(limit: number) => ReplayLimitSink<T>;
        };
    };
};

/**
 * An interface representing a publisher that supports data transformation and manipulation through pipes.
 * @template T - The type of data being published.
 */
interface PipePublisher<T> extends Publisher<T> {
    /**
     * Pipes the data through custom transformations.
     * @template R - The result type after processing.
     * @param {Function} producer - The function to produce new values.
     * @param {Function} onSubscribe - Callback on subscription.
     * @param {Function} onRequest - Callback on request.
     * @param {Function} onUnsubscribe - Callback on unsubscribe.
     * @returns {PipePublisher<R>} A new pipe publisher with transformed data.
     */
    pipe<R>(producer: (onNext: (value: R) => void, onError: (error: Error) => void, onComplete: () => void) => void, onRequest: (request: number) => void, onUnsubscribe: () => void): PipePublisher<R>;
    /**
     * Transforms each emitted value using the given function.
     * @template R - The transformed data type.
     * @param {Function} fn - The mapping function.
     * @returns {PipePublisher<R>} A new pipe publisher with mapped data.
     */
    map<R>(fn: (value: T) => R): PipePublisher<R>;
    /**
     * Transforms each emitted value and filter transform result.
     * @template R - The transformed data type.
     * @param {Function} fn - The mapping function that can return null or undefined.
     * @returns {PipePublisher<R>} A new pipe publisher with mapped non-null data.
     */
    mapNotNull<R>(fn: (value: T) => R | null | undefined): PipePublisher<R>;
    /**
     * Transforms the value using a function that returns a new publisher.
     * @template R - The resulting data type.
     * @param {Function} fn - The function to transform values into new publishers.
     * @returns {PipePublisher<R>} A pipe publisher that flattens the result.
     */
    flatMap<R>(fn: (value: T) => Publisher<R>): PipePublisher<R>;
    /**
     * Filters emitted values using a given predicate.
     * @param {Function} predicate - The function to determine whether to emit a value.
     * @returns {PipePublisher<T>} A new pipe publisher with filtered data.
     */
    filter(predicate: (value: T) => boolean): PipePublisher<T>;
    /**
     * Filters emitted values based on a publisher that returns a boolean.
     * @param {Function} predicate - A function returning a boolean publisher.
     * @returns {PipePublisher<T>} A new pipe publisher with conditional data.
     */
    filterWhen(predicate: (value: T) => Publisher<boolean>): PipePublisher<T>;
    /**
     * Casts the current publisher to another type.
     * @template R - The target type.
     * @returns {PipePublisher<R>} The casted pipe publisher.
     */
    cast<R>(): PipePublisher<R>;
    /**
     * Switches to an alternative publisher if the current one is empty.
     * @param {Publisher<T>} alternative - The alternative publisher.
     * @returns {PipePublisher<T>} A new pipe publisher.
     */
    switchIfEmpty(alternative: Publisher<T>): PipePublisher<T>;
    /**
     * Continues with a replacement publisher if an error occurs.
     * @param {Publisher<T>} replacement - The publisher to switch to on error.
     * @returns {PipePublisher<T>} A new pipe publisher.
     */
    onErrorReturn(replacement: Publisher<T>): PipePublisher<T>;
    /**
     * Continues processing even if an error occurs based on a predicate.
     * @param {Function} predicate - Function to determine whether to continue on error.
     * @returns {PipePublisher<T>} A new pipe publisher.
     */
    onErrorContinue(predicate: (error: Error) => boolean): PipePublisher<T>;
    /**
     * Executes a function when the first value is emitted.
     * @param {Function} fn - The function to execute.
     * @returns {PipePublisher<T>} A new pipe publisher.
     */
    doFirst(fn: () => void): PipePublisher<T>;
    /**
     * Executes a function when each value is emitted.
     * @param {Function} fn - The function to execute on each value.
     * @returns {PipePublisher<T>} A new pipe publisher.
     */
    doOnNext(fn: (value: T) => void): PipePublisher<T>;
    /**
     * Executes a function when each error is emitted.
     * @param {Function} fn - The function to execute on each error.
     * @returns {PipePublisher<T>} A new pipe publisher.
     */
    doOnError(fn: (value: Error) => void): PipePublisher<T>;
    /**
     * Executes a function when the stream completes.
     * @param {Function} fn - The function to execute on completion.
     * @returns {PipePublisher<T>} A new pipe publisher.
     */
    doFinally(fn: () => void): PipePublisher<T>;
    /**
     * Executes a function when a subscription occurs.
     * @param {Function} fn - The function to execute on subscription.
     * @returns {PipePublisher<T>} A new pipe publisher.
     */
    doOnSubscribe(fn: (subscription: Subscription) => void): PipePublisher<T>;
    /**
     * Publishes values on a specified scheduler.
     * @param {Scheduler} scheduler - The scheduler to use.
     * @returns {PipePublisher<T>} A new pipe publisher.
     */
    publishOn(scheduler: Scheduler): PipePublisher<T>;
    /**
     * Subscribes to the stream on a specified scheduler.
     * @param {Scheduler} scheduler - The scheduler to use.
     * @returns {PipePublisher<T>} A new pipe publisher.
     */
    subscribeOn(scheduler: Scheduler): PipePublisher<T>;
}
/**
 * Abstract base class for pipe publishers.
 * Implements common functionalities while allowing customization.
 * @template T - The type of data being published.
 */
declare abstract class AbstractPipePublisher<T> implements PipePublisher<T> {
    protected readonly publisher: Publisher<T>;
    private unsubscribeOnComplete;
    private onSubscribe?;
    protected constructor(publisher: Publisher<T>);
    subscribe({ onNext, onError, onComplete }?: {
        onNext?: ((value: T) => void) | undefined;
        onError?: ((error: Error) => void) | undefined;
        onComplete?: (() => void) | undefined;
    }): Subscription;
    private canEmitMany;
    pipe<R>(producer: (onNext: (value: R) => void, onError: (error: Error) => void, onComplete: () => void) => void, onRequest?: (request: number) => void, onUnsubscribe?: () => void, constructor?: new (publisher: Publisher<R>) => AbstractPipePublisher<R>): PipePublisher<R>;
    map<R>(fn: (value: T) => R): PipePublisher<R>;
    mapNotNull<R>(fn: (value: T) => R | null | undefined): PipePublisher<R>;
    flatMap<R>(fn: (value: T) => Publisher<R>): PipePublisher<R>;
    filter(predicate: (value: T) => boolean): PipePublisher<T>;
    filterWhen(predicate: (value: T) => Publisher<boolean>): PipePublisher<T>;
    cast<R>(): PipePublisher<R>;
    switchIfEmpty(alternative: Publisher<T>): PipePublisher<T>;
    onErrorReturn(replacement: Publisher<T>): PipePublisher<T>;
    onErrorContinue(predicate: (error: Error) => boolean): PipePublisher<T>;
    doFirst(fn: () => void): PipePublisher<T>;
    doOnNext(fn: (value: T) => void): PipePublisher<T>;
    doOnError(fn: (value: Error) => void): PipePublisher<T>;
    doFinally(fn: () => void): PipePublisher<T>;
    doOnSubscribe(fn: (subscription: Subscription) => void): PipePublisher<T>;
    publishOn(scheduler: Scheduler): PipePublisher<T>;
    subscribeOn(scheduler: Scheduler): PipePublisher<T>;
    protected abstract createSink(): Sink<T> & Publisher<T>;
    private wrap;
}

/**
 * Represents a Flux publisher that can emit multiple values over time.
 * Provides rich reactive programming capabilities such as transformation, filtering, and combination.
 * @template T - The type of data being published.
 */
declare class Flux<T> extends AbstractPipePublisher<T> {
    protected constructor(publisher: Publisher<T>);
    /**
     * Generates a Flux instance using a generator function.
     * @template T - The type of data.
     * @param {Function} generator - The function to generate values.
     * @returns {Flux<T>} A new Flux instance.
     */
    static generate<T>(generator: ((sink: Sink<T>) => void)): Flux<T>;
    /**
     * Creates a Flux instance from another publisher.
     * @template T - The type of data.
     * @param {Publisher<T>} publisher - The source publisher.
     * @returns {Flux<T>} A new Flux instance.
     */
    static from<T>(publisher: Publisher<T>): Flux<T>;
    /**
     * Creates a Flux from an iterable collection.
     * @template T - The type of data.
     * @param {Iterable<T>} iterable - An iterable to create the Flux from.
     * @returns {Flux<T>} A new Flux instance.
     */
    static fromIterable<T>(iterable: Iterable<T>): Flux<T>;
    /**
     * Creates a Flux that emits a range of numbers.
     * @param {number} start - The starting number.
     * @param {number} count - The number of elements to emit.
     * @returns {Flux<number>} A new Flux emitting the range of numbers.
     */
    static range(start: number, count: number): Flux<number>;
    /**
     * Creates an empty Flux that immediately completes.
     * @template T - The type of data.
     * @returns {Flux<T>} A new empty Flux instance.
     */
    static empty<T = never>(): Flux<T>;
    /**
     * Defers the creation of a Flux until it is subscribed to.
     * @template T - The type of data.
     * @param {Function} factory - A function that returns a Flux.
     * @returns {Flux<T>} A new deferred Flux instance.
     */
    static defer<T>(factory: () => Flux<T>): Flux<T>;
    /**
     * Returns a Mono emitting the first element of the Flux.
     * @returns {Mono<T>} A Mono containing the first element.
     */
    first(): Mono<T>;
    /**
     * Returns a Mono emitting the last element of the Flux.
     * @returns {Mono<T>} A Mono containing the last element.
     */
    last(): Mono<T>;
    /**
     * Returns a Mono that emits the count of elements in the Flux.
     * @returns {Mono<number>} A Mono containing the number of elements.
     */
    count(): Mono<number>;
    /**
     * Checks whether the Flux has any elements.
     * @returns {Mono<boolean>} A Mono emitting true if there are elements, false otherwise.
     */
    hasElements(): Mono<boolean>;
    /**
     * Collects all emitted items into an array.
     * @param {boolean} [force=false] - Forces immediate collection.
     * @returns {Mono<T[]>} A Mono containing an array of collected items.
     */
    collect(force?: boolean): Mono<T[]>;
    /**
     * Attaches an index to each emitted value.
     * @returns {Flux<[number, T]>} A new Flux containing tuples of (index, value).
     */
    indexed(): Flux<[number, T]>;
    /**
     * Skips the first `n` emitted elements.
     * @param {number} n - The number of elements to skip.
     * @returns {Flux<T>} A new Flux without the skipped elements.
     */
    skip(n: number): Flux<T>;
    /**
     * Skips elements while the given predicate returns true.
     * @param {Function} predicate - A function that takes a value and returns a boolean.
     * @returns {Flux<T>} A new Flux without the skipped elements.
     */
    skipWhile(predicate: (value: T) => boolean): Flux<T>;
    /**
     * Skips elements until another Publisher emits an item.
     * @param {Publisher<any>} other - The publisher to wait for.
     * @returns {Flux<T>} A new Flux that skips elements until the other publisher emits.
     */
    skipUntil(other: Publisher<any>): Flux<T>;
    /**
     * Emits only distinct elements, discarding duplicates.
     * @returns {Flux<T>} A new Flux containing only distinct elements.
     */
    distinct(): Flux<T>;
    /**
     * Emits items only when they differ from the previous item.
     * @param {Function} comparator - A function to compare previous and current items.
     * @returns {Flux<T>} A new Flux with distinct consecutive items.
     */
    distinctUntilChanged(comparator?: (previous: T, current: T) => boolean): Flux<T>;
    /**
     * Delays each emitted element by a specified duration.
     * @param {number} ms - The delay duration in milliseconds.
     * @returns {Flux<T>} A new Flux with delayed elements.
     */
    delayElements(ms: number): Flux<T>;
    /**
     * Concatenates the current Flux with another Publisher.
     * The current Flux is emitted first, and once it completes, the second Publisher starts emitting.
     *
     * @param {Publisher<T>} other - The Publisher to concatenate after the current one completes.
     * @returns {Flux<T>} A new Flux that first emits the values from the current Flux and then from the other Publisher.
     */
    concatWith(other: Publisher<T>): Flux<T>;
    /**
     * Merges the current Flux with another Publisher.
     * Both Publishers emit values concurrently as they become available.
     *
     * @param {Publisher<T>} other - The other Publisher to merge with.
     * @returns {Flux<T>} A new Flux that emits values from both Publishers as they arrive.
     */
    mergeWith(other: Publisher<T>): Flux<T>;
    /**
     * Reduces the items emitted by this Flux using a given accumulator function.
     * Aggregates the items into a single result.
     *
     * @param {Function} reducer - A function that combines the accumulated value and the next item.
     * @returns {Mono<T>} A Mono that emits the final accumulated value.
     */
    reduce(reducer: (acc: T, next: T) => T): Mono<T>;
    /**
     * Reduces the items emitted by this Flux using a given accumulator function and a seed value.
     * Allows providing an initial seed for the accumulation.
     *
     * @template A - The type of the accumulated value.
     * @param {Function} seedFactory - A function that provides the initial accumulated value.
     * @param {Function} reducer - A function that combines the accumulated value and the next item.
     * @returns {Mono<A>} A Mono that emits the final accumulated value.
     */
    reduceWith<A>(seedFactory: () => A, reducer: (acc: A, next: T) => A): Mono<A>;
    /**
     * Returns a Mono that completes when the current Flux completes.
     * Does not emit any value, just completes.
     *
     * @returns {Mono<void>} A Mono that completes when the Flux completes.
     */
    then(): Mono<void>;
    /**
     * Returns a Mono that completes when the current Flux completes, and then triggers the completion of another Publisher.
     *
     * @param {Publisher<any>} other - Another Publisher to complete after the current Flux.
     * @returns {Mono<void>} A Mono that completes after both Flux and the given Publisher complete.
     */
    thenEmpty(other: Publisher<any>): Mono<void>;
    /**
     * Pipes the data through custom transformations.
     * @template R - The result type after processing.
     * @param {Function} producer - The function to produce new values.
     * @param {Function} onRequest - Callback on request.
     * @param {Function} onUnsubscribe - Callback on unsubscribe.
     * @param constructor - Constructor for generating new Publisher
     * @returns {Flux<R>} A new Flux with transformed data.
     */
    pipe<R>(producer: (onNext: (value: R) => void, onError: (error: Error) => void, onComplete: () => void) => void, onRequest?: (request: number) => void, onUnsubscribe?: () => void, constructor?: new (publisher: Publisher<R>) => AbstractPipePublisher<R>): Flux<R>;
    /**
     * Transforms each emitted value using the given function.
     * @template R - The transformed data type.
     * @param {Function} fn - The mapping function.
     * @returns {Flux<R>} A new Flux with mapped data.
     */
    map<R>(fn: (value: T) => R): Flux<R>;
    /**
     * Transforms each emitted value and filter transform result.
     * @template R - The transformed data type.
     * @param {Function} fn - The mapping function that can return null or undefined.
     * @returns {Flux<R>} A new Flux with mapped non-null data.
     */
    mapNotNull<R>(fn: (value: T) => (R | null | undefined)): Flux<R>;
    /**
     * Transforms the value using a function that returns a new publisher.
     * @template R - The resulting data type.
     * @param {Function} fn - The function to transform values into new publishers.
     * @returns {Flux<R>} A Flux that flattens the result.
     */
    flatMap<R>(fn: (value: T) => Publisher<R>): Flux<R>;
    /**
     * Filters emitted values using a given predicate.
     * @param {Function} predicate - The function to determine whether to emit a value.
     * @returns {Flux<T>} A new Flux with filtered data.
     */
    filter(predicate: (value: T) => boolean): Flux<T>;
    /**
     * Filters emitted values based on a publisher that returns a boolean.
     * @param {Function} predicate - A function returning a boolean publisher.
     * @returns {Flux<T>} A new Flux with conditional data.
     */
    filterWhen(predicate: (value: T) => Publisher<boolean>): Flux<T>;
    /**
     * Casts the current publisher to another type.
     * @template R - The target type.
     * @returns {Flux<R>} The casted Flux.
     */
    cast<R>(): Flux<R>;
    /**
     * Switches to an alternative publisher if the current one is empty.
     * @param {Publisher<T>} alternative - The alternative publisher.
     * @returns {Flux<T>} A new Flux.
     */
    switchIfEmpty(alternative: Publisher<T>): Flux<T>;
    /**
     * Continues with a replacement publisher if an error occurs.
     * @param {Publisher<T>} replacement - The publisher to switch to on error.
     * @returns {Flux<T>} A new Flux.
     */
    onErrorReturn(replacement: Publisher<T>): Flux<T>;
    /**
     * Continues processing even if an error occurs based on a predicate.
     * @param {Function} predicate - Function to determine whether to continue on error.
     * @returns {Flux<T>} A new Flux.
     */
    onErrorContinue(predicate: (error: Error) => boolean): Flux<T>;
    /**
     * Executes a function when the first value is emitted.
     * @param {Function} fn - The function to execute.
     * @returns {Flux<T>} A new Flux.
     */
    doFirst(fn: () => void): Flux<T>;
    /**
     * Executes a function when each value is emitted.
     * @param {Function} fn - The function to execute on each value.
     * @returns {Flux<T>} A new Flux.
     */
    doOnNext(fn: (value: T) => void): Flux<T>;
    /**
     * Executes a function when each error is emitted.
     * @param {Function} fn - The function to execute on each error.
     * @returns {Flux<T>} A new Flux.
     */
    doOnError(fn: (value: Error) => void): Flux<T>;
    /**
     * Executes a function when the stream completes.
     * @param {Function} fn - The function to execute on completion.
     * @returns {Flux<T>} A new Flux.
     */
    doFinally(fn: () => void): Flux<T>;
    /**
     * Executes a function when a subscription occurs.
     * @param {Function} fn - The function to execute on subscription.
     * @returns {Flux<T>} A new Flux.
     */
    doOnSubscribe(fn: (subscription: Subscription) => void): Flux<T>;
    /**
     * Publishes values on a specified scheduler.
     * @param {Scheduler} scheduler - The scheduler to use.
     * @returns {Flux<T>} A new Flux.
     */
    publishOn(scheduler: Scheduler): Flux<T>;
    /**
     * Subscribes to the stream on a specified scheduler.
     * @param {Scheduler} scheduler - The scheduler to use.
     * @returns {Flux<T>} A new Flux.
     */
    subscribeOn(scheduler: Scheduler): Flux<T>;
    protected createSink(): Sink<T> & Publisher<T>;
}

/**
 * Represents a Mono publisher that emits at most one item.
 * Suitable for scenarios where a single value or an empty result is expected.
 * @template T - The type of data being published.
 */
declare class Mono<T> extends AbstractPipePublisher<T> {
    protected constructor(publisher: Publisher<T>);
    /**
     * Creates a Mono instance from a value generator function.
     * @template T - The type of data being generated.
     * @param {Function} generator - A function to generate the data.
     * @returns {Mono<T>} A Mono instance.
     */
    static generate<T>(generator: ((sink: Sink<T>) => void)): Mono<T>;
    /**
     * Creates a Mono instance from an existing publisher.
     * @template T - The type of data being published.
     * @param {Publisher<T>} publisher - The source publisher.
     * @returns {Mono<T>} A Mono instance.
     */
    static from<T>(publisher: Publisher<T>): Mono<T>;
    /**
     * Creates a Mono instance that emits a single value.
     * @template T - The type of data.
     * @param {T} value - The value to emit.
     * @returns {Mono<T>} A Mono instance.
     */
    static just<T>(value: T): Mono<T>;
    /**
     * Creates a Mono instance that emits the given value or completes if null/undefined.
     * @template T - The type of data.
     * @param {T | null | undefined} value - The value to emit.
     * @returns {Mono<T>} A Mono instance.
     */
    static justOrEmpty<T>(value: T | null | undefined): Mono<T>;
    /**
     * Creates an empty Mono instance.
     * @template T - The type of data.
     * @returns {Mono<T>} A Mono instance that completes without emitting.
     */
    static empty<T = never>(): Mono<T>;
    /**
     * Creates a Mono instance that emits an error.
     * @template T - The type of data.
     * @param {any} error - The error to emit.
     * @returns {Mono<T>} A Mono instance that emits an error.
     */
    static error<T = never>(error: any): Mono<T>;
    /**
     * Creates a Mono instance from a Promise.
     * @template T - The type of data.
     * @param {Promise<T>} promise - The promise to wrap.
     * @returns {Mono<T>} A Mono instance.
     */
    static fromPromise<T>(promise: Promise<T>): Mono<T>;
    /**
     * Subscribes to the Mono and triggers the provided callbacks on events.
     * @param {() => Mono<T>} factory - The event handlers.
     * @returns {Subscription} The subscription object.
     */
    static defer<T>(factory: () => Mono<T>): Mono<T>;
    /**
     * Transforms the value emitted by the Mono into a Flux using the provided mapper function.
     * The resulting Flux can emit multiple items from the transformation of a single Mono item.
     *
     * @template R - The result type after mapping.
     * @param {Function} mapper - A function that takes a value of type `T` and returns a `Publisher<R>`.
     * @returns {Flux<R>} A new Flux containing the transformed values.
     */
    flatMapMany<R>(mapper: (value: T) => Publisher<R>): Flux<R>;
    /**
     * Combines the values emitted by this Mono and another Mono into a pair.
     *
     * @template R - The type of the other Mono.
     * @param {Mono<R>} other - The other Mono to zip with.
     * @returns {Mono<[T, R]>} A new Mono emitting a tuple of values from both Monos.
     */
    zipWith<R>(other: Mono<R>): Mono<[T, R]>;
    /**
     * Combines the value emitted by this Mono with a value produced by a function.
     * The function returns another Mono, and the resulting Mono contains a pair of values.
     *
     * @template R - The result type produced by the function.
     * @param {Function} fn - A function that takes a value of type `T` and returns a `Mono<R>`.
     * @returns {Mono<[T, R]>} A new Mono containing the combined pair.
     */
    zipWhen<R>(fn: (value: T) => Mono<R>): Mono<[T, R]>;
    /**
     * Checks if the Mono contains an element.
     * @returns {Mono<boolean>} A Mono that emits true if an element exists, false otherwise.
     */
    hasElement(): Mono<boolean>;
    /**
     * Converts the Mono to a Promise.
     * @returns {Promise<T | null>} A promise that resolves when the Mono completes.
     */
    toPromise(): Promise<T | null>;
    /**
     * Pipes the data through custom transformations.
     * @template R - The result type after processing.
     * @param {Function} producer - The function to produce new values.
     * @param {Function} onRequest - Callback on request.
     * @param {Function} onUnsubscribe - Callback on unsubscribe.
     * @param constructor - Constructor for generating new Publisher
     * @returns {Mono<R>} A new Mono with transformed data.
     */
    pipe<R>(producer: (onNext: (value: R) => void, onError: (error: Error) => void, onComplete: () => void) => void, onRequest?: (request: number) => void, onUnsubscribe?: () => void, constructor?: new (publisher: Publisher<R>) => AbstractPipePublisher<R>): Mono<R>;
    /**
     * Transforms each emitted value using the given function.
     * @template R - The transformed data type.
     * @param {Function} fn - The mapping function.
     * @returns {Mono<R>} A new Mono with mapped data.
     */
    map<R>(fn: (value: T) => R): Mono<R>;
    /**
     * Transforms each emitted value and filter transform result.
     * @template R - The transformed data type.
     * @param {Function} fn - The mapping function that can return null or undefined.
     * @returns {Mono<R>} A new Mono with mapped non-null data.
     */
    mapNotNull<R>(fn: (value: T) => (R | null | undefined)): Mono<R>;
    /**
     * Transforms the value using a function that returns a new publisher.
     * @template R - The resulting data type.
     * @param {Function} fn - The function to transform values into new publishers.
     * @returns {Mono<R>} A Mono that flattens the result.
     */
    flatMap<R>(fn: (value: T) => Publisher<R>): Mono<R>;
    /**
     * Filters emitted values using a given predicate.
     * @param {Function} predicate - The function to determine whether to emit a value.
     * @returns {Mono<T>} A new Mono with filtered data.
     */
    filter(predicate: (value: T) => boolean): Mono<T>;
    /**
     * Filters emitted values based on a publisher that returns a boolean.
     * @param {Function} predicate - A function returning a boolean publisher.
     * @returns {Mono<T>} A new Mono with conditional data.
     */
    filterWhen(predicate: (value: T) => Publisher<boolean>): Mono<T>;
    /**
     * Casts the current publisher to another type.
     * @template R - The target type.
     * @returns {Mono<R>} The casted Mono.
     */
    cast<R>(): Mono<R>;
    /**
     * Switches to an alternative publisher if the current one is empty.
     * @param {Publisher<T>} alternative - The alternative publisher.
     * @returns {Mono<T>} A new Mono.
     */
    switchIfEmpty(alternative: Publisher<T>): Mono<T>;
    /**
     * Continues with a replacement publisher if an error occurs.
     * @param {Publisher<T>} replacement - The publisher to switch to on error.
     * @returns {Mono<T>} A new Mono.
     */
    onErrorReturn(replacement: Publisher<T>): Mono<T>;
    /**
     * Continues processing even if an error occurs based on a predicate.
     * @param {Function} predicate - Function to determine whether to continue on error.
     * @returns {Mono<T>} A new Mono.
     */
    onErrorContinue(predicate: (error: Error) => boolean): Mono<T>;
    /**
     * Executes a function when the first value is emitted.
     * @param {Function} fn - The function to execute.
     * @returns {Mono<T>} A new Mono.
     */
    doFirst(fn: () => void): Mono<T>;
    /**
     * Executes a function when each value is emitted.
     * @param {Function} fn - The function to execute on each value.
     * @returns {Mono<T>} A new Mono.
     */
    doOnNext(fn: (value: T) => void): Mono<T>;
    /**
     * Executes a function when each error is emitted.
     * @param {Function} fn - The function to execute on each error.
     * @returns {Mono<T>} A new Mono.
     */
    doOnError(fn: (value: Error) => void): Mono<T>;
    /**
     * Executes a function when the stream completes.
     * @param {Function} fn - The function to execute on completion.
     * @returns {Mono<T>} A new Mono.
     */
    doFinally(fn: () => void): Mono<T>;
    /**
     * Executes a function when a subscription occurs.
     * @param {Function} fn - The function to execute on subscription.
     * @returns {Mono<T>} A new Mono.
     */
    doOnSubscribe(fn: (subscription: Subscription) => void): Mono<T>;
    /**
     * Publishes values on a specified scheduler.
     * @param {Scheduler} scheduler - The scheduler to use.
     * @returns {Mono<T>} A new Mono.
     */
    publishOn(scheduler: Scheduler): Mono<T>;
    /**
     * Subscribes to the stream on a specified scheduler.
     * @param {Scheduler} scheduler - The scheduler to use.
     * @returns {Mono<T>} A new Mono.
     */
    subscribeOn(scheduler: Scheduler): Mono<T>;
    protected createSink(): Sink<T> & Publisher<T>;
}

/**
 * A scheduler that immediately executes tasks.
 * Implements the `Scheduler` interface to execute tasks without any delay.
 */
declare class ImmediateScheduler implements Scheduler {
    /**
     * Schedules a task to be executed immediately.
     * @param {Function} task - The task function to be executed.
     */
    schedule(task: () => void): void;
}

/**
 * A scheduler that executes tasks asynchronously using the microtask queue.
 * Implements the `Scheduler` interface and schedules tasks using `Promise.resolve().then(...)`.
 */
declare class MicroScheduler implements Scheduler {
    /**
     * Schedules a task to be executed asynchronously as a microtask.
     * Uses `Promise.resolve().then(task)` to place the task in the microtask queue.
     * @param {Function} task - The task function to be executed.
     */
    schedule(task: () => void): void;
}

/**
 * A scheduler that executes tasks asynchronously using the macro task queue.
 * Implements the `Scheduler` interface and schedules tasks using `setTimeout` with a delay of `0`.
 */
declare class MacroScheduler implements Scheduler {
    /**
     * Schedules a task to be executed asynchronously.
     * Uses `setTimeout` with a delay of `0` to place the task in the macro task queue.
     * @param {Function} task - The task function to be executed.
     */
    schedule(task: () => void): void;
}

/**
 * A scheduler that executes tasks after a specified delay.
 * Implements the `CancellableScheduler` interface, allowing scheduled tasks to be canceled.
 */
declare class DelayScheduler implements CancellableScheduler {
    private readonly delay;
    /**
     * Creates a new DelayScheduler.
     * @param {number} delay - The delay duration in milliseconds.
     */
    constructor(delay: number);
    /**
     * Schedules a task to be executed after the specified delay.
     * Returns an object with a cancel method to clear the timeout.
     * @param {Function} task - The task function to be executed.
     * @returns {Object} An object with a `cancel` method to stop the execution.
     */
    schedule(task: () => void): {
        cancel: () => void;
    };
}

/**
 * A collection of commonly used schedulers for task execution.
 * Provides methods to create instances of various scheduler types.
 */
declare const Schedulers: {
    /**
     * Creates an instance of `ImmediateScheduler`.
     * Executes tasks immediately without any delay.
     * @returns {ImmediateScheduler} An instance of ImmediateScheduler.
     */
    immediate: () => ImmediateScheduler;
    /**
     * Creates an instance of `MicroScheduler`.
     * Executes tasks asynchronously using the microtask queue.
     * @returns {MicroScheduler} An instance of MicroScheduler.
     */
    micro: () => MicroScheduler;
    /**
     * Creates an instance of `MacroScheduler`.
     * Executes tasks asynchronously using the macro task queue (via `setTimeout`).
     * @returns {MacroScheduler} An instance of MacroScheduler.
     */
    macro: () => MacroScheduler;
    /**
     * Creates an instance of `DelayScheduler` with a specified delay.
     * Executes tasks after a given delay using `setTimeout`.
     * @param {number} ms - The delay in milliseconds before executing the task.
     * @returns {DelayScheduler} An instance of DelayScheduler.
     */
    delay: (ms: number) => DelayScheduler;
};

/**
 * Creates a reactive subject that holds a single mutable value and supports subscriptions.
 * The subject allows updating the value and notifying subscribers of changes.
 *
 * @template T - The type of the value held by the subject.
 * @param {T} value - The initial value of the subject.
 * @returns {Object} An object with methods to interact with the subject.
 * @property {function(T): void} next - Updates the current value and notifies subscribers.
 * @property {function((current: T) => T): void} update - Updates the current value using a function and notifies subscribers.
 * @property {function(): T} get - Returns the current value of the subject.
 * @property {function(Subscriber<T>): Subscription} subscribe - Subscribes to changes and returns a subscription.
 *
 * @example
 * const count = subject(0);
 * count.next(1);  // Update the value to 1
 * count.update(prev => prev + 1);  // Increment the value
 * console.log(count.get());  // Output: 2
 * const subscription = count.subscribe({
 *   onNext: value => console.log('New value:', value),
 *   onComplete: () => console.log('Completed')
 * });
 * subscription.request(1);  // Request the next value
 * subscription.unsubscribe();  // Stop receiving updates
 */
declare function subject<T>(value: T): {
    next(value: T): void;
    update(fn: (current: T) => T): void;
    get(): T;
    subscribe({ onNext, onError, onComplete }?: {
        onNext?: ((value: T) => void) | undefined;
        onError?: ((error: Error) => void) | undefined;
        onComplete?: (() => void) | undefined;
    }): Subscription;
};

export { type CancellableScheduler, Flux, ManySink, Mono, OneSink, type Publisher, ReplayAllSink, ReplayLatestSink, ReplayLimitSink, type Scheduler, Schedulers, type Sink, Sinks, type Subscriber, type Subscription, subject };
