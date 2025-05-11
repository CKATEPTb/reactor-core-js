import {BackpressurePublisher, Publisher} from "@/publishers/Publisher";
import {Subscriber} from "@/subscriptions/Subscriber";
import {Subscription} from "@/subscriptions/Subscription";
import {Scheduler} from "@/schedulers/Scheduler";
import OneSink from "@/sinks/OneSink";
import ManySink from "@/sinks/ManySink";

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
    pipe<R>(producer: (onNext: (value: R) => void, onError: (error: Error) => void, onComplete: () => void) => void, onSubscribe: (subscriber: Subscriber<R>) => void, onRequest: (request: number) => void, onUnsubscribe: () => void): PipePublisher<R>

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
    doOnSubscribe(fn: (subscriber: Subscriber<T>) => void): PipePublisher<T>

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

/**
 * Abstract base class for pipe publishers.
 * Implements common functionalities while allowing customization.
 * @template T - The type of data being published.
 */
export abstract class AbstractPipePublisher<T> implements PipePublisher<T> {
    private unsubscribeOnComplete = true

    protected constructor(protected readonly publisher: Publisher<T>) {
    }

    public subscribe({
                         onNext = (value: T) => {
                         },
                         onError = (error: Error) => {
                         },
                         onComplete = () => {
                         }
                     } = {}): Subscription {
        const subscription = this.publisher.subscribe({
            onNext, onError, onComplete: () => {
                onComplete()
                if (this.unsubscribeOnComplete) subscription.unsubscribe()
            }
        })
        return subscription
    }

    public pipe<R>(producer: (onNext: (value: R) => void, onError: (error: Error) => void, onComplete: () => void) => void, onSubscribe?: (subscriber: Subscriber<R>) => void, onRequest?: (request: number) => void, onUnsubscribe?: () => void): PipePublisher<R> {
        const many = this.sinkType() == 'many';
        const sink = !many ? new OneSink<R>() : new ManySink<R>();
        const unicast = new class _ extends BackpressurePublisher<R> {
            public override subscribe(subscriber: Subscriber<R>): Subscription {
                onSubscribe?.(subscriber)
                try {
                    producer(value => {
                            if (many && value == null) onRequest?.(1)
                            else sink.next(value)
                        },
                        error => {
                            sink.error(error)
                            onRequest?.(1)
                        },
                        () => sink.complete())
                } catch (error) {
                    sink.error(error as Error)
                }
                const sub = super.subscribe(subscriber)
                return {
                    request(count: number) {
                        sub.request(count)
                        onRequest?.(count)
                    },
                    unsubscribe() {
                        sub.unsubscribe()
                        onUnsubscribe?.()
                    }
                };
            }
        }(sink);
        return this.wrap(unicast)
    }

    public map<R>(fn: (value: T) => R): PipePublisher<R> {
        let sub: Subscription;
        return this.pipe((onNext, onError, onComplete) =>
            sub = this.subscribe({
                onNext: value => onNext(fn(value)),
                onError,
                onComplete
            }), undefined, request => sub?.request(request), () => sub?.unsubscribe())
    }

    public mapNotNull<R>(fn: (value: T) => R | null | undefined): PipePublisher<R> {
        return this.map(fn).filter((v): v is R => v != null) as PipePublisher<R>
    }

    public flatMap<R>(fn: (value: T) => Publisher<R>): PipePublisher<R> {
        let sub: Subscription;
        let req = 0
        return this.pipe((onNext, onError, onComplete) =>
                sub = this.subscribe({
                    onNext: (value) => {
                        fn(value).subscribe({
                            onNext, onError, onComplete: () => (this.sinkType() == 'many') ? () => {
                            } : onComplete
                        }).request(req)
                    }, onError, onComplete
                })
            , undefined, request => sub?.request(req = request), () => sub?.unsubscribe())
    }

    public filter(predicate: (value: T) => boolean): PipePublisher<T> {
        let sub: Subscription
        return this.pipe((onNext, onError, onComplete) =>
            sub = this.subscribe({
                onNext: (value) => predicate(value) ? onNext(value) : (this.sinkType() == 'many') ? onNext(null as T) : onComplete(),
                onError,
                onComplete
            }), undefined, request => sub?.request(request), () => sub?.unsubscribe())
    }

    public filterWhen(predicate: (value: T) => Publisher<boolean>): PipePublisher<T> {
        let sub: Subscription
        let req = 0
        return this.pipe((onNext, onError, onComplete) =>
            sub = this.subscribe({
                onNext: (value) => predicate(value).subscribe({
                    onNext: bool => bool ? onNext(value) : (this.sinkType() == 'many') ? onNext(null as T) : onComplete(),
                    onError,
                    onComplete: () => (this.sinkType() == 'many') ? () => {
                    } : onComplete
                }).request(req), onError, onComplete
            }), undefined, request => sub?.request(req = request), () => sub?.unsubscribe())
    }

    public cast<R>(): PipePublisher<R> {
        return this as unknown as PipePublisher<R>
    }

    public switchIfEmpty(alternative: Publisher<T>): PipePublisher<T> {
        let sub: Subscription
        let req = 0
        return this.pipe((onNext, onError, onComplete) => {
            let emitted = false;
            sub = this.subscribe({
                onNext(value: T) {
                    emitted = true
                    onNext(value)
                },
                onError(error: Error) {
                    emitted = true
                    onError(error)
                },
                onComplete: () => {
                    return emitted ? onComplete() : alternative.subscribe({onNext, onError, onComplete}).request(req)
                }
            })
        }, undefined, request => sub?.request(req = request), () => sub?.unsubscribe())
    }

    public onErrorReturn(replacement: Publisher<T>): PipePublisher<T> {
        let sub: Subscription
        let req = 0
        return this.pipe((onNext, onError, onComplete) =>
            sub = this.subscribe({
                onNext,
                onError: () => replacement.subscribe({onNext, onError, onComplete}).request(req),
                onComplete
            }), undefined, request => sub?.request(req = request), () => sub?.unsubscribe())
    }

    public onErrorContinue(predicate: (error: Error) => boolean): PipePublisher<T> {
        let sub: Subscription
        return this.pipe((onNext, onError, onComplete) =>
            sub = this.subscribe({
                onNext,
                onError: error => predicate(error) ? (this.sinkType() == 'many') ? onNext(null as T) : onComplete() : onError(error),
                onComplete
            }), undefined, request => sub?.request(request), () => sub?.unsubscribe())
    }

    public doFirst(fn: () => void): PipePublisher<T> {
        let sub: Subscription
        return this.pipe((onNext, onError, onComplete) => {
                fn()
                sub = this.subscribe({onNext, onError, onComplete})
            }, undefined, request => sub?.request(request), () => sub?.unsubscribe()
        )
    }

    public doOnNext(fn: (value: T) => void): PipePublisher<T> {
        let sub: Subscription
        return this.pipe((onNext, onError, onComplete) =>
            sub = this.subscribe({
                onNext: value => {
                    fn(value)
                    onNext(value)
                }, onError, onComplete
            }), undefined, request => sub?.request(request), () => sub?.unsubscribe())
    }

    public doFinally(fn: () => void): PipePublisher<T> {
        let sub: Subscription
        let called = false;
        return this.pipe((onNext, onError, onComplete) =>
            sub = this.subscribe({
                onNext, onError, onComplete
            }), undefined, request => sub?.request(request), () => {
            sub?.unsubscribe()
            if (!called) {
                called = true
                fn()
            }
        })
    }

    public doOnSubscribe(fn: (subscriber: Subscriber<T>) => void): PipePublisher<T> {
        // todo срабатывает на текущем пайпе, вместо финального
        let sub: Subscription
        return this.pipe((onNext, onError, onComplete) =>
            sub = this.subscribe({
                onNext,
                onError,
                onComplete
            }), subscriber => fn(subscriber), request => sub?.request(request), () => sub?.unsubscribe())
    }

    public publishOn(scheduler: Scheduler): PipePublisher<T> {
        let sub: Subscription
        return this.pipe((onNext, onError, onComplete) =>
            sub = this.subscribe({
                onNext: value => scheduler.schedule(() => onNext(value)),
                onError: error => scheduler.schedule(() => onError(error)),
                onComplete: () => scheduler.schedule(() => onComplete())
            }), undefined, request => sub?.request(request), () => sub?.unsubscribe())
    }

    public subscribeOn(scheduler: Scheduler): PipePublisher<T> {
        let sub: Promise<Subscription>
        return this.pipe((onNext, onError, onComplete) =>
                sub = new Promise(resolve => scheduler.schedule(() => resolve(this.subscribe({
                    onNext,
                    onError,
                    onComplete
                })))),
            undefined, request => sub?.then(value => value.request(request)), () => sub?.then(value => value.unsubscribe())
        )
    }

    protected abstract sinkType(): 'one' | 'many'

    private wrap<R>(publisher: Publisher<R>) {
        this.unsubscribeOnComplete = false
        const wrapped: AbstractPipePublisher<R> = Reflect.construct((Reflect.getPrototypeOf(this) as PipePublisher<any>).constructor, [publisher]);
        wrapped.unsubscribeOnComplete = true
        return wrapped
    }
}
