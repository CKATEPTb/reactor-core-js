import {BackpressurePublisher, Publisher} from "@/publishers/Publisher";
import {Subscriber} from "@/subscriptions/Subscriber";
import {Subscription} from "@/subscriptions/Subscription";
import {Scheduler} from "@/schedulers/Scheduler";
import {OneSink, Sink} from "@/sinks";

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

/**
 * Abstract base class for pipe publishers.
 * Implements common functionalities while allowing customization.
 * @template T - The type of data being published.
 */
export abstract class AbstractPipePublisher<T> implements PipePublisher<T> {
    private unsubscribeOnComplete = true
    private onSubscribe?: (subscription: Subscription) => void

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
        this.onSubscribe?.(subscription)
        return subscription
    }

    private canEmitMany() {
        return !(Reflect.construct(Reflect.getPrototypeOf(this)!.constructor, [null]).createSink() instanceof OneSink)
    }

    public pipe<R>(producer: (onNext: (value: R) => void, onError: (error: Error) => void, onComplete: () => void) => void, onRequest?: (request: number) => void, onUnsubscribe?: () => void, constructor?: new (publisher: Publisher<R>) => AbstractPipePublisher<R>): PipePublisher<R> {
        // todo сделать так, чтобы конечный пайп мог быть inline, для большего удобства
        const sink = Reflect.construct(constructor || Reflect.getPrototypeOf(this)!.constructor, [null]).createSink();
        const many = !(sink instanceof OneSink)
        const unicast = new class _ extends BackpressurePublisher<R> {
            public override subscribe(subscriber: Subscriber<R>): Subscription {
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
        return this.wrap(unicast, constructor)
    }

    public map<R>(fn: (value: T) => R): PipePublisher<R> {
        let sub: Subscription;
        return this.pipe((onNext, onError, onComplete) =>
            sub = this.subscribe({
                onNext: value => onNext(fn(value)),
                onError,
                onComplete
            }), request => sub?.request(request), () => sub?.unsubscribe())
    }

    public mapNotNull<R>(fn: (value: T) => R | null | undefined): PipePublisher<R> {
        return this.map(fn).filter((v): v is R => v != null) as PipePublisher<R>
    }

    public flatMap<R>(fn: (value: T) => Publisher<R>): PipePublisher<R> {
        // TODO избавится от Promise и сделать синхронно
        let subscriptions: Subscription[] = [];
        let promises: Promise<void>[] = []
        return this.pipe((onNext, onError, onComplete) => {
            const sub = this.subscribe({
                onNext: (value) => {
                    promises.push(new Promise(resolve => {
                            let s
                            subscriptions.push(s = fn(value).subscribe({
                                onNext: value => {
                                    onNext(value)
                                }, onError, onComplete: () => {
                                    if (this.canEmitMany()) {
                                        sub.request(1)
                                    }
                                    resolve()
                                }
                            }))
                            s.request(Number.MAX_SAFE_INTEGER)
                        }
                    ))
                }, onError, onComplete: () => {
                    Promise.all(promises).then(onComplete)
                }
            })
            subscriptions.push(sub)
            return sub
        }, request => subscriptions[0]?.request(request), () => subscriptions.forEach(sub => sub.unsubscribe()))
    }

    public filter(predicate: (value: T) => boolean): PipePublisher<T> {
        let sub: Subscription
        return this.pipe((onNext, onError, onComplete) =>
            sub = this.subscribe({
                onNext: (value) => predicate(value) ? onNext(value) : this.canEmitMany() ? onNext(null as T) : onComplete(),
                onError,
                onComplete
            }), request => sub?.request(request + 1), () => sub?.unsubscribe())
    }

    public filterWhen(predicate: (value: T) => Publisher<boolean>): PipePublisher<T> {
        let sub: Subscription
        let req = 0
        return this.pipe((onNext, onError, onComplete) =>
            sub = this.subscribe({
                onNext: (value) => predicate(value).subscribe({
                    onNext: bool => bool ? onNext(value) : this.canEmitMany() ? onNext(null as T) : onComplete(),
                    onError,
                    onComplete: () => this.canEmitMany() ? () => {
                    } : onComplete
                }).request(req), onError, onComplete
            }), request => sub?.request(req = request + 1), () => sub?.unsubscribe())
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
        }, request => sub?.request(req = request), () => sub?.unsubscribe())
    }

    public onErrorReturn(replacement: Publisher<T>): PipePublisher<T> {
        let sub: Subscription
        let req = 0
        return this.pipe((onNext, onError, onComplete) =>
            sub = this.subscribe({
                onNext,
                onError: () => replacement.subscribe({onNext, onError, onComplete}).request(req),
                onComplete
            }), request => sub?.request(req = request), () => sub?.unsubscribe())
    }

    public onErrorContinue(predicate: (error: Error) => boolean): PipePublisher<T> {
        let sub: Subscription
        return this.pipe((onNext, onError, onComplete) =>
            sub = this.subscribe({
                onNext,
                onError: error => predicate(error) ? this.canEmitMany() ? onNext(null as T) : onComplete() : onError(error),
                onComplete
            }), request => sub?.request(request + 1), () => sub?.unsubscribe())
    }

    public doFirst(fn: () => void): PipePublisher<T> {
        let sub: Subscription
        return this.pipe((onNext, onError, onComplete) => {
                fn()
                sub = this.subscribe({onNext, onError, onComplete})
            }, request => sub?.request(request), () => sub?.unsubscribe()
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
            }), request => sub?.request(request), () => sub?.unsubscribe())
    }

    public doOnError(fn: (value: Error) => void): PipePublisher<T> {
        let sub: Subscription
        return this.pipe((onNext, onError, onComplete) =>
            sub = this.subscribe({
                onNext, onError: error => {
                    fn(error)
                    onError(error)
                }, onComplete
            }), request => sub?.request(request), () => sub?.unsubscribe())
    }

    public doFinally(fn: () => void): PipePublisher<T> {
        let sub: Subscription
        let called = false;
        return this.pipe((onNext, onError, onComplete) =>
            sub = this.subscribe({
                onNext, onError, onComplete
            }), request => sub?.request(request), () => {
            sub?.unsubscribe()
            if (!called) {
                called = true
                fn()
            }
        })
    }

    public doOnSubscribe(fn: (subscription: Subscription) => void): PipePublisher<T> {
        let sub: Subscription
        const prev = this.onSubscribe
        this.onSubscribe = (subscription) => {
            prev?.(subscription)
            fn(subscription)
        }
        return this.pipe((onNext, onError, onComplete) =>
            sub = this.subscribe({
                onNext,
                onError,
                onComplete
            }), request => sub?.request(request), () => sub?.unsubscribe())
    }

    public publishOn(scheduler: Scheduler): PipePublisher<T> {
        let sub: Subscription
        return this.pipe((onNext, onError, onComplete) =>
            sub = this.subscribe({
                onNext: value => scheduler.schedule(() => onNext(value)),
                onError: error => scheduler.schedule(() => onError(error)),
                onComplete: () => scheduler.schedule(() => onComplete())
            }), request => sub?.request(request), () => sub?.unsubscribe())
    }

    public subscribeOn(scheduler: Scheduler): PipePublisher<T> {
        let sub: Promise<Subscription>
        return this.pipe((onNext, onError, onComplete) =>
            sub = new Promise(resolve => scheduler.schedule(() => resolve(this.subscribe({
                onNext,
                onError,
                onComplete
            })))), request => sub?.then(value => value.request(request)), () => sub?.then(value => value.unsubscribe())
        )
    }

    protected abstract createSink(): Sink<T> & Publisher<T>

    private wrap<R>(publisher: Publisher<R>, constructor?: new (publisher: Publisher<R>) => AbstractPipePublisher<R>) {
        this.unsubscribeOnComplete = false
        if (constructor == null) constructor = (Reflect.getPrototypeOf(this) as AbstractPipePublisher<R>).constructor as
            new (publisher: Publisher<R>) => AbstractPipePublisher<R>
        const wrapped: AbstractPipePublisher<R> = Reflect.construct(constructor, [publisher]);
        wrapped.unsubscribeOnComplete = true
        if (this.onSubscribe != undefined) {
            wrapped.onSubscribe = this.onSubscribe
            this.onSubscribe = undefined
        }
        return wrapped
    }
}
