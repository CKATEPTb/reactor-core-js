import {AbstractPipePublisher} from "@/publishers/PipePublisher";
import {Publisher} from "@/publishers/Publisher";
import OneSink from "@/sinks/OneSink";
import {Sink} from "@/sinks/Sink";
import {Scheduler} from "@/schedulers/Scheduler";
import {combine} from "@/utils";
import {Flux} from "@/publishers/Flux";
import {Subscription} from "@/subscriptions/Subscription";

/**
 * Represents a Mono publisher that emits at most one item.
 * Suitable for scenarios where a single value or an empty result is expected.
 * @template T - The type of data being published.
 */
export class Mono<T> extends AbstractPipePublisher<T> {

    protected constructor(publisher: Publisher<T>) {
        super(publisher)
    }

    /**
     * Creates a Mono instance from a value generator function.
     * @template T - The type of data being generated.
     * @param {Function} generator - A function to generate the data.
     * @returns {Mono<T>} A Mono instance.
     */
    public static generate<T>(generator: ((sink: Sink<T>) => void)): Mono<T> {
        return new Mono(combine(new OneSink<T>(), generator))
    }

    /**
     * Creates a Mono instance from an existing publisher.
     * @template T - The type of data being published.
     * @param {Publisher<T>} publisher - The source publisher.
     * @returns {Mono<T>} A Mono instance.
     */
    public static from<T>(publisher: Publisher<T>): Mono<T> {
        return Mono.generate(sink => {
                const subscription = publisher.subscribe({
                    onNext(value: T) {
                        sink.next(value)
                        subscription.unsubscribe()
                    },
                    onError(error: Error) {
                        sink.error(error)
                        subscription.unsubscribe()
                    },
                    onComplete() {
                        sink.complete()
                        subscription.unsubscribe()
                    }
                })
                return subscription
            }
        )
    }

    /**
     * Creates a Mono instance that emits a single value.
     * @template T - The type of data.
     * @param {T} value - The value to emit.
     * @returns {Mono<T>} A Mono instance.
     */
    public static just<T>(value: T): Mono<T> {
        return Mono.generate(sink => sink.next(value))
    }

    /**
     * Creates a Mono instance that emits the given value or completes if null/undefined.
     * @template T - The type of data.
     * @param {T | null | undefined} value - The value to emit.
     * @returns {Mono<T>} A Mono instance.
     */
    public static justOrEmpty<T>(value: T | null | undefined): Mono<T> {
        return value == null ? Mono.empty() : Mono.just(value)
    }

    /**
     * Creates an empty Mono instance.
     * @template T - The type of data.
     * @returns {Mono<T>} A Mono instance that completes without emitting.
     */
    public static empty<T = never>(): Mono<T> {
        return Mono.generate(sink => sink.complete())
    }

    /**
     * Creates a Mono instance that emits an error.
     * @template T - The type of data.
     * @param {any} error - The error to emit.
     * @returns {Mono<T>} A Mono instance that emits an error.
     */
    public static error<T = never>(error: any): Mono<T> {
        return Mono.generate(sink => sink.error(error))
    }

    /**
     * Creates a Mono instance from a Promise.
     * @template T - The type of data.
     * @param {Promise<T>} promise - The promise to wrap.
     * @returns {Mono<T>} A Mono instance.
     */
    public static fromPromise<T>(promise: Promise<T>): Mono<T> {
        return Mono.generate(sink => promise
            .then((value) => sink.next(value))
            .catch((err) => sink.error(err)))
    }

    /**
     * Subscribes to the Mono and triggers the provided callbacks on events.
     * @param {() => Mono<T>} factory - The event handlers.
     * @returns {Subscription} The subscription object.
     */
    public static defer<T>(factory: () => Mono<T>): Mono<T> {
        return Mono.generate(sink => factory().subscribe({
            onNext(value: T) {
                sink.next(value)
            },
            onError(error: Error) {
                sink.error(error)
            },
            onComplete() {
                sink.complete()
            }
        }))
    }

    /**
     * Transforms the value emitted by the Mono into a Flux using the provided mapper function.
     * The resulting Flux can emit multiple items from the transformation of a single Mono item.
     *
     * @template R - The result type after mapping.
     * @param {Function} mapper - A function that takes a value of type `T` and returns a `Publisher<R>`.
     * @returns {Flux<R>} A new Flux containing the transformed values.
     */
    public flatMapMany<R>(mapper: (value: T) => Publisher<R>): Flux<R> {
        return Flux.generate(sink => {
            let subscription: Subscription | undefined
            this.subscribe({
                onNext: (val) => {
                    subscription = mapper(val).subscribe({
                        onNext(value: R) {
                            sink.next(value)
                        },
                        onError(error: Error) {
                            sink.error(error)
                        },
                        onComplete() {
                            sink.complete()
                        }
                    })
                },
                onError: (error) => {
                    sink.error(error)
                },
                onComplete: () => {
                    if (subscription == null) sink.complete()
                }
            })
                .request(1)
            return {
                request(count: number) {
                    subscription?.request(count)
                },
                unsubscribe() {
                    subscription?.unsubscribe()
                }
            } as Subscription
        })
    }

    /**
     * Combines the values emitted by this Mono and another Mono into a pair.
     *
     * @template R - The type of the other Mono.
     * @param {Mono<R>} other - The other Mono to zip with.
     * @returns {Mono<[T, R]>} A new Mono emitting a tuple of values from both Monos.
     */
    public zipWith<R>(other: Mono<R>): Mono<[T, R]> {
        return this.flatMap(left => other.map(right => [left, right]))
    }

    /**
     * Combines the value emitted by this Mono with a value produced by a function.
     * The function returns another Mono, and the resulting Mono contains a pair of values.
     *
     * @template R - The result type produced by the function.
     * @param {Function} fn - A function that takes a value of type `T` and returns a `Mono<R>`.
     * @returns {Mono<[T, R]>} A new Mono containing the combined pair.
     */
    public zipWhen<R>(fn: (value: T) => Mono<R>): Mono<[T, R]> {
        return this.flatMap((left) => fn(left).map((right) => [left, right]))
    }

    /**
     * Checks if the Mono contains an element.
     * @returns {Mono<boolean>} A Mono that emits true if an element exists, false otherwise.
     */
    public hasElement(): Mono<boolean> {
        return this.onErrorContinue(_error => true)
            .map(_value => true)
            .switchIfEmpty(Mono.defer(() => Mono.just(false)))
    }

    /**
     * Converts the Mono to a Promise.
     * @returns {Promise<T | null>} A promise that resolves when the Mono completes.
     */
    public toPromise(): Promise<T | null> {
        return new Promise((resolve, reject) => {
            let value: T | null = null
            this.subscribe({
                onNext: (v) => value = v,
                onError: reject,
                onComplete: () => resolve(value)
            }).request(1)
        })
    }

    /**
     * Pipes the data through custom transformations.
     * @template R - The result type after processing.
     * @param {Function} producer - The function to produce new values.
     * @param {Function} onSubscribe - Callback on subscription.
     * @param {Function} onRequest - Callback on request.
     * @param {Function} onUnsubscribe - Callback on unsubscribe.
     * @returns {Mono<R>} A new Mono with transformed data.
     */
    public override pipe<R>(producer: (onNext: (value: R) => void, onError: (error: Error) => void, onComplete: () => void) => void, onRequest?: (request: number) => void, onUnsubscribe?: () => void): Mono<R> {
        return super.pipe(producer, onRequest, onUnsubscribe) as Mono<R>;
    }

    /**
     * Transforms each emitted value using the given function.
     * @template R - The transformed data type.
     * @param {Function} fn - The mapping function.
     * @returns {Mono<R>} A new Mono with mapped data.
     */
    public override map<R>(fn: (value: T) => R): Mono<R> {
        return super.map(fn) as Mono<R>;
    }

    /**
     * Transforms each emitted value and filter transform result.
     * @template R - The transformed data type.
     * @param {Function} fn - The mapping function that can return null or undefined.
     * @returns {Mono<R>} A new Mono with mapped non-null data.
     */
    public override mapNotNull<R>(fn: (value: T) => (R | null | undefined)): Mono<R> {
        return super.mapNotNull(fn) as Mono<R>;
    }

    /**
     * Transforms the value using a function that returns a new publisher.
     * @template R - The resulting data type.
     * @param {Function} fn - The function to transform values into new publishers.
     * @returns {Mono<R>} A Mono that flattens the result.
     */
    public override flatMap<R>(fn: (value: T) => Publisher<R>): Mono<R> {
        return super.flatMap(fn) as Mono<R>;
    }

    /**
     * Filters emitted values using a given predicate.
     * @param {Function} predicate - The function to determine whether to emit a value.
     * @returns {Mono<T>} A new Mono with filtered data.
     */
    public override filter(predicate: (value: T) => boolean): Mono<T> {
        return super.filter(predicate) as Mono<T>;
    }

    /**
     * Filters emitted values based on a publisher that returns a boolean.
     * @param {Function} predicate - A function returning a boolean publisher.
     * @returns {Mono<T>} A new Mono with conditional data.
     */
    public override filterWhen(predicate: (value: T) => Publisher<boolean>): Mono<T> {
        return super.filterWhen(predicate) as Mono<T>;
    }

    /**
     * Casts the current publisher to another type.
     * @template R - The target type.
     * @returns {Mono<R>} The casted Mono.
     */
    public override cast<R>(): Mono<R> {
        return super.cast() as Mono<R>;
    }

    /**
     * Switches to an alternative publisher if the current one is empty.
     * @param {Publisher<T>} alternative - The alternative publisher.
     * @returns {Mono<T>} A new Mono.
     */
    public override switchIfEmpty(alternative: Publisher<T>): Mono<T> {
        return super.switchIfEmpty(alternative) as Mono<T>;
    }

    /**
     * Continues with a replacement publisher if an error occurs.
     * @param {Publisher<T>} replacement - The publisher to switch to on error.
     * @returns {Mono<T>} A new Mono.
     */
    public override onErrorReturn(replacement: Publisher<T>): Mono<T> {
        return super.onErrorReturn(replacement) as Mono<T>;
    }

    /**
     * Continues processing even if an error occurs based on a predicate.
     * @param {Function} predicate - Function to determine whether to continue on error.
     * @returns {Mono<T>} A new Mono.
     */
    public override onErrorContinue(predicate: (error: Error) => boolean): Mono<T> {
        return super.onErrorContinue(predicate) as Mono<T>;
    }

    /**
     * Executes a function when the first value is emitted.
     * @param {Function} fn - The function to execute.
     * @returns {Mono<T>} A new Mono.
     */
    public override doFirst(fn: () => void): Mono<T> {
        return super.doFirst(fn) as Mono<T>;
    }

    /**
     * Executes a function when each value is emitted.
     * @param {Function} fn - The function to execute on each value.
     * @returns {Mono<T>} A new Mono.
     */
    public override doOnNext(fn: (value: T) => void): Mono<T> {
        return super.doOnNext(fn) as Mono<T>;
    }

    /**
     * Executes a function when the stream completes.
     * @param {Function} fn - The function to execute on completion.
     * @returns {Mono<T>} A new Mono.
     */
    public override doFinally(fn: () => void): Mono<T> {
        return super.doFinally(fn) as Mono<T>;
    }

    /**
     * Executes a function when a subscription occurs.
     * @param {Function} fn - The function to execute on subscription.
     * @returns {Mono<T>} A new Mono.
     */
    public override doOnSubscribe(fn: (subscription: Subscription) => void): Mono<T> {
        return super.doOnSubscribe(fn) as Mono<T>;
    }

    /**
     * Publishes values on a specified scheduler.
     * @param {Scheduler} scheduler - The scheduler to use.
     * @returns {Mono<T>} A new Mono.
     */
    public override publishOn(scheduler: Scheduler): Mono<T> {
        return super.publishOn(scheduler) as Mono<T>;
    }

    /**
     * Subscribes to the stream on a specified scheduler.
     * @param {Scheduler} scheduler - The scheduler to use.
     * @returns {Mono<T>} A new Mono.
     */
    public override subscribeOn(scheduler: Scheduler): Mono<T> {
        return super.subscribeOn(scheduler) as Mono<T>;
    }

    protected sinkType(): 'one' | 'many' {
        return 'one';
    }
}
