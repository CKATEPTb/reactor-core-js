import {AbstractPipePublisher} from "@/publishers/PipePublisher";
import {Publisher} from "@/publishers/Publisher";
import {Sink} from "@/sinks/Sink";
import ManySink from "@/sinks/ManySink";
import {Mono} from "@/publishers/Mono";
import {Scheduler} from "@/schedulers/Scheduler";
import {combine} from "@/utils";
import {MicroScheduler} from "@/schedulers/MicroScheduler";
import {DelayScheduler} from "@/schedulers/DelayScheduler";
import {Subscription} from "@/subscriptions/Subscription";
import {Subscriber} from "@/subscriptions/Subscriber";

/**
 * Represents a Flux publisher that can emit multiple values over time.
 * Provides rich reactive programming capabilities such as transformation, filtering, and combination.
 * @template T - The type of data being published.
 */
export class Flux<T> extends AbstractPipePublisher<T> {
    protected constructor(publisher: Publisher<T>) {
        super(publisher)
    }

    /**
     * Generates a Flux instance using a generator function.
     * @template T - The type of data.
     * @param {Function} generator - The function to generate values.
     * @returns {Flux<T>} A new Flux instance.
     */
    public static generate<T>(generator: ((sink: Sink<T>) => void)): Flux<T> {
        return new Flux(combine(new ManySink<T>(), generator))
    }

    /**
     * Creates a Flux instance from another publisher.
     * @template T - The type of data.
     * @param {Publisher<T>} publisher - The source publisher.
     * @returns {Flux<T>} A new Flux instance.
     */
    public static from<T>(publisher: Publisher<T>): Flux<T> {
        return Flux.generate(sink => {
            return publisher.subscribe({
                onNext(value: T) {
                    sink.next(value)
                },
                onError(error: Error) {
                    sink.error(error)
                },
                onComplete() {
                    sink.complete()
                }
            })
        })
    }

    /**
     * Creates a Flux from an iterable collection.
     * @template T - The type of data.
     * @param {Iterable<T>} iterable - An iterable to create the Flux from.
     * @returns {Flux<T>} A new Flux instance.
     */
    public static fromIterable<T>(iterable: Iterable<T>): Flux<T> {
        return Flux.generate(sink => {
            for (const value of iterable) {
                sink.next(value);
            }
            sink.complete();
        })
    }

    /**
     * Creates a Flux that emits a range of numbers.
     * @param {number} start - The starting number.
     * @param {number} count - The number of elements to emit.
     * @returns {Flux<number>} A new Flux emitting the range of numbers.
     */
    public static range(start: number, count: number): Flux<number> {
        return Flux.generate(sink => {
            let current = start;
            for (let i = 0; i < count; i++) {
                sink.next(current++);
            }
            sink.complete();
        })
    }

    /**
     * Creates an empty Flux that immediately completes.
     * @template T - The type of data.
     * @returns {Flux<T>} A new empty Flux instance.
     */
    public static empty<T = never>(): Flux<T> {
        return Flux.generate(sink => {
            sink.complete()
        })
    }

    /**
     * Defers the creation of a Flux until it is subscribed to.
     * @template T - The type of data.
     * @param {Function} factory - A function that returns a Flux.
     * @returns {Flux<T>} A new deferred Flux instance.
     */
    public static defer<T>(factory: () => Flux<T>): Flux<T> {
        return Flux.generate(sink => factory().subscribe({
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
     * Returns a Mono emitting the first element of the Flux.
     * @returns {Mono<T>} A Mono containing the first element.
     */
    public first(): Mono<T> {
        return Mono.generate(sink => {
            let sub: Subscription;
            (sub = this.subscribe({
                onNext(value: T) {
                    sink.next(value)
                    sub?.unsubscribe()
                },
                onError(error: Error) {
                    sink.error(error)
                },
                onComplete() {
                    sink.complete()
                }
            })).request(Number.MAX_SAFE_INTEGER)
        })
    }

    /**
     * Returns a Mono emitting the last element of the Flux.
     * @returns {Mono<T>} A Mono containing the last element.
     */
    public last(): Mono<T> {
        return Mono.generate(sink => {
            let lastValue: T | undefined
            this.subscribe({
                onNext(value: T): void {
                    lastValue = value
                },
                onError: sink.error,
                onComplete(): void {
                    if (lastValue !== undefined) sink.next(lastValue)
                    else sink.complete()
                }
            }).request(Number.MAX_SAFE_INTEGER)
        })
    }

    /**
     * Returns a Mono that emits the count of elements in the Flux.
     * @returns {Mono<number>} A Mono containing the number of elements.
     */
    public count(): Mono<number> {
        return this.collect().map(value => value.length)
    }

    /**
     * Checks whether the Flux has any elements.
     * @returns {Mono<boolean>} A Mono emitting true if there are elements, false otherwise.
     */
    public hasElements(): Mono<boolean> {
        return this.count().map(value => value > 0)
    }

    /**
     * Collects all emitted items into an array.
     * @param {boolean} [force=false] - Forces immediate collection.
     * @returns {Mono<T[]>} A Mono containing an array of collected items.
     */
    public collect(force = false): Mono<T[]> {
        return Mono.generate(sink => {
            const buffer: T[] = []
            const subscription = this.subscribe({
                onNext(value: T): void {
                    buffer.push(value)
                },
                onError(error: Error): void {
                    sink.error(error)
                },
                onComplete(): void {
                    sink.next(buffer)
                }
            })
            subscription.request(Number.MAX_SAFE_INTEGER)
            if (force) new MicroScheduler().schedule(() => {
                try {
                    sink.next(buffer)
                } catch (e) {
                }
                subscription.unsubscribe()
            })
        })
    }

    /**
     * Attaches an index to each emitted value.
     * @returns {Flux<[number, T]>} A new Flux containing tuples of (index, value).
     */
    public indexed(): Flux<[number, T]> {
        let pipeSub: Subscription
        return this.pipe((onNext, onError, onComplete) => {
            let index = 0
            pipeSub = this.subscribe({
                onNext(value: T): void {
                    onNext([index++, value])
                },
                onError,
                onComplete
            })
        }, undefined, request => pipeSub?.request(request), () => pipeSub?.unsubscribe())
    }

    /**
     * Skips the first `n` emitted elements.
     * @param {number} n - The number of elements to skip.
     * @returns {Flux<T>} A new Flux without the skipped elements.
     */
    public skip(n: number): Flux<T> {
        return this.indexed()
            .filter(value => value[0] >= n)
            .map(value => value[1])
    }

    /**
     * Skips elements while the given predicate returns true.
     * @param {Function} predicate - A function that takes a value and returns a boolean.
     * @returns {Flux<T>} A new Flux without the skipped elements.
     */
    public skipWhile(predicate: (value: T) => boolean): Flux<T> {
        let pipeSub: Subscription
        return this.pipe((onNext, onError, onComplete) => {
            let skipping = true
            pipeSub = this.subscribe({
                onNext(value: T): void {
                    if (!skipping || !predicate(value)) {
                        skipping = false
                        onNext(value)
                    } else onNext(null as T)
                },
                onError,
                onComplete
            })
        }, undefined, request => pipeSub?.request(request), () => pipeSub?.unsubscribe())
    }

    /**
     * Skips elements until another Publisher emits an item.
     * @param {Publisher<any>} other - The publisher to wait for.
     * @returns {Flux<T>} A new Flux that skips elements until the other publisher emits.
     */
    public skipUntil(other: Publisher<any>): Flux<T> {
        let pipeSub: Subscription
        return this.pipe((onNext, onError, onComplete) => {
            let open = false;
            const sub = this.subscribe({
                onNext: (value) => {
                    if (open) onNext(value)
                    else onNext(null as T)
                },
                onError,
                onComplete
            })
            const sub2 = other.subscribe({
                onNext(value: any): void {
                    open = true
                },
                onError,
                onComplete(): void {
                }
            })
            pipeSub = {
                request(count: number) {
                    sub.request(count)
                    sub2.request(count)
                },
                unsubscribe() {
                    sub.unsubscribe()
                    sub2.unsubscribe()
                }
            } as Subscription
        }, undefined, request => pipeSub?.request(request), () => pipeSub?.unsubscribe())
    }

    /**
     * Emits only distinct elements, discarding duplicates.
     * @returns {Flux<T>} A new Flux containing only distinct elements.
     */
    public distinct(): Flux<T> {
        let sub: Subscription
        return this.pipe((onNext, onError, onComplete) => {
            const seen = new Set<T>()
            sub = this.subscribe({
                onNext: (value) => {
                    if (!seen.has(value)) {
                        seen.add(value)
                        onNext(value)
                    } else onNext(null as T)
                },
                onError,
                onComplete
            })
        }, undefined, request => sub?.request(request), () => sub?.unsubscribe())
    }

    /**
     * Emits items only when they differ from the previous item.
     * @param {Function} comparator - A function to compare previous and current items.
     * @returns {Flux<T>} A new Flux with distinct consecutive items.
     */
    public distinctUntilChanged(comparator: (previous: T, current: T) => boolean = (previous, current) => previous == current): Flux<T> {
        let sub: Subscription
        return this.pipe((onNext, onError, onComplete) => {
            let previous: T | null = null
            sub = this.subscribe({
                onNext(value: T) {
                    if (previous == null || comparator(previous, value)) {
                        onNext(previous = value);
                    } else onNext(null as T)
                },
                onError(error: Error) {
                    onError(error)
                },
                onComplete() {
                    onComplete()
                }
            })
        }, undefined, request => sub?.request(request), () => sub?.unsubscribe())
    }

    /**
     * Delays each emitted element by a specified duration.
     * @param {number} ms - The delay duration in milliseconds.
     * @returns {Flux<T>} A new Flux with delayed elements.
     */
    public delayElements(ms: number): Flux<T> {
        let sub: Subscription
        return this.pipe((onNext, onError, onComplete) => {
            let promise = Promise.resolve()
            const emit = (fn: () => void) => {
                promise = promise.then(() => new Promise<void>(resolve => {
                    new DelayScheduler(ms).schedule(() => {
                        fn()
                        resolve()
                    })
                }))
            }
            sub = this.subscribe({
                onNext(value: T) {
                    emit(() => onNext(value))
                },
                onError(error: Error) {
                    emit(() => onError(error))
                },
                onComplete() {
                    emit(() => onComplete())
                }
            })
        }, undefined, request => sub?.request(request), () => sub?.unsubscribe())
    }

    /**
     * Concatenates the current Flux with another Publisher.
     * The current Flux is emitted first, and once it completes, the second Publisher starts emitting.
     *
     * @param {Publisher<T>} other - The Publisher to concatenate after the current one completes.
     * @returns {Flux<T>} A new Flux that first emits the values from the current Flux and then from the other Publisher.
     */
    public concatWith(other: Publisher<T>): Flux<T> {
        let pipeSub: Subscription
        return this.pipe((onNext, onError, onComplete) => {
            let second: Subscription | undefined
            const first = this.subscribe({
                onNext(value: T) {
                    onNext(value)
                },
                onError(error: Error) {
                    onError(error)
                },
                onComplete() {
                    second = other.subscribe({
                        onNext,
                        onError,
                        onComplete
                    })
                }
            })
            pipeSub = {
                request(count: number) {
                    first.request(count)
                    second?.request(count)
                },
                unsubscribe() {
                    first.unsubscribe()
                    second?.unsubscribe()
                }
            } as Subscription
        }, undefined, request => pipeSub?.request(request), () => pipeSub?.unsubscribe())
    }

    /**
     * Merges the current Flux with another Publisher.
     * Both Publishers emit values concurrently as they become available.
     *
     * @param {Publisher<T>} other - The other Publisher to merge with.
     * @returns {Flux<T>} A new Flux that emits values from both Publishers as they arrive.
     */
    public mergeWith(other: Publisher<T>): Flux<T> {
        let pipeSub: Subscription
        return this.pipe((onNext, onError, onComplete) => {
            let left = 2
            const subscriber = {
                onNext,
                onError,
                onComplete() {
                    if (--left <= 0) {
                        onComplete()
                    }
                }
            } as Subscriber<T>
            const first = this.subscribe(subscriber)
            const second = other.subscribe(subscriber)
            pipeSub = {
                request(count: number) {
                    first.request(count)
                    second.request(count)
                },
                unsubscribe() {
                    first.unsubscribe()
                    second.unsubscribe()
                }
            } as Subscription
        }, undefined, request => pipeSub?.request(request), () => pipeSub?.unsubscribe())
    }

    /**
     * Reduces the items emitted by this Flux using a given accumulator function.
     * Aggregates the items into a single result.
     *
     * @param {Function} reducer - A function that combines the accumulated value and the next item.
     * @returns {Mono<T>} A Mono that emits the final accumulated value.
     */
    public reduce(reducer: (acc: T, next: T) => T): Mono<T> {
        return this.collect().map(value => value.reduce(reducer))
    }

    /**
     * Reduces the items emitted by this Flux using a given accumulator function and a seed value.
     * Allows providing an initial seed for the accumulation.
     *
     * @template A - The type of the accumulated value.
     * @param {Function} seedFactory - A function that provides the initial accumulated value.
     * @param {Function} reducer - A function that combines the accumulated value and the next item.
     * @returns {Mono<A>} A Mono that emits the final accumulated value.
     */
    public reduceWith<A>(seedFactory: () => A, reducer: (acc: A, next: T) => A): Mono<A> {
        return Mono.generate(sink => {
            let acc = seedFactory()
            this.subscribe({
                onNext(value: T) {
                    acc = reducer(acc, value)
                },
                onError(error: Error) {
                    sink.error(error)
                },
                onComplete() {
                    sink.next(acc)
                }
            }).request(Number.MAX_SAFE_INTEGER)
        })
    }

    /**
     * Returns a Mono that completes when the current Flux completes.
     * Does not emit any value, just completes.
     *
     * @returns {Mono<void>} A Mono that completes when the Flux completes.
     */
    public then(): Mono<void> {
        return Mono.generate(sink => {
            this.subscribe({
                onNext(value: T) {
                },
                onError(error: Error) {
                    sink.error(error)
                },
                onComplete() {
                    sink.complete()
                }
            }).request(Number.MAX_SAFE_INTEGER)
        })
    }

    /**
     * Returns a Mono that completes when the current Flux completes, and then triggers the completion of another Publisher.
     *
     * @param {Publisher<any>} other - Another Publisher to complete after the current Flux.
     * @returns {Mono<void>} A Mono that completes after both Flux and the given Publisher complete.
     */
    public thenEmpty(other: Publisher<any>): Mono<void> {
        return Mono.generate(sink => {
            this.subscribe({
                onNext(value: T) {
                },
                onError(error: Error) {
                    sink.error(error)
                },
                onComplete() {
                    other.subscribe({
                        onNext(value: any) {
                        },
                        onError(error: Error) {
                            sink.error(error)
                        },
                        onComplete() {
                            sink.complete()
                        }
                    }).request(Number.MAX_SAFE_INTEGER)
                }
            }).request(Number.MAX_SAFE_INTEGER)
        })
    }

    /**
     * Pipes the data through custom transformations.
     * @template R - The result type after processing.
     * @param {Function} producer - The function to produce new values.
     * @param {Function} onSubscribe - Callback on subscription.
     * @param {Function} onRequest - Callback on request.
     * @param {Function} onUnsubscribe - Callback on unsubscribe.
     * @returns {Flux<R>} A new Flux with transformed data.
     */
    public override pipe<R>(producer: (onNext: (value: R) => void, onError: (error: Error) => void, onComplete: () => void) => void, onSubscribe?: (subscriber: Subscriber<R>) => void, onRequest?: (request: number) => void, onUnsubscribe?: () => void): Flux<R> {
        return super.pipe(producer, onSubscribe, onRequest, onUnsubscribe) as Flux<R>;
    }

    /**
     * Transforms each emitted value using the given function.
     * @template R - The transformed data type.
     * @param {Function} fn - The mapping function.
     * @returns {Flux<R>} A new Flux with mapped data.
     */
    public override map<R>(fn: (value: T) => R): Flux<R> {
        return super.map(fn) as Flux<R>;
    }

    /**
     * Transforms each emitted value and filter transform result.
     * @template R - The transformed data type.
     * @param {Function} fn - The mapping function that can return null or undefined.
     * @returns {Flux<R>} A new Flux with mapped non-null data.
     */
    public override mapNotNull<R>(fn: (value: T) => (R | null | undefined)): Flux<R> {
        return super.mapNotNull(fn) as Flux<R>;
    }

    /**
     * Transforms the value using a function that returns a new publisher.
     * @template R - The resulting data type.
     * @param {Function} fn - The function to transform values into new publishers.
     * @returns {Flux<R>} A Flux that flattens the result.
     */
    public override flatMap<R>(fn: (value: T) => Publisher<R>): Flux<R> {
        return super.flatMap(fn) as Flux<R>;
    }

    /**
     * Filters emitted values using a given predicate.
     * @param {Function} predicate - The function to determine whether to emit a value.
     * @returns {Flux<T>} A new Flux with filtered data.
     */
    public override filter(predicate: (value: T) => boolean): Flux<T> {
        return super.filter(predicate) as Flux<T>;
    }

    /**
     * Filters emitted values based on a publisher that returns a boolean.
     * @param {Function} predicate - A function returning a boolean publisher.
     * @returns {Flux<T>} A new Flux with conditional data.
     */
    public override filterWhen(predicate: (value: T) => Publisher<boolean>): Flux<T> {
        return super.filterWhen(predicate) as Flux<T>;
    }

    /**
     * Casts the current publisher to another type.
     * @template R - The target type.
     * @returns {Flux<R>} The casted Flux.
     */
    public override cast<R>(): Flux<R> {
        return super.cast() as Flux<R>;
    }

    /**
     * Switches to an alternative publisher if the current one is empty.
     * @param {Publisher<T>} alternative - The alternative publisher.
     * @returns {Flux<T>} A new Flux.
     */
    public override switchIfEmpty(alternative: Publisher<T>): Flux<T> {
        return super.switchIfEmpty(alternative) as Flux<T>;
    }

    /**
     * Continues with a replacement publisher if an error occurs.
     * @param {Publisher<T>} replacement - The publisher to switch to on error.
     * @returns {Flux<T>} A new Flux.
     */
    public override onErrorReturn(replacement: Publisher<T>): Flux<T> {
        return super.onErrorReturn(replacement) as Flux<T>;
    }

    /**
     * Continues processing even if an error occurs based on a predicate.
     * @param {Function} predicate - Function to determine whether to continue on error.
     * @returns {Flux<T>} A new Flux.
     */
    public override onErrorContinue(predicate: (error: Error) => boolean): Flux<T> {
        return super.onErrorContinue(predicate) as Flux<T>;
    }

    /**
     * Executes a function when the first value is emitted.
     * @param {Function} fn - The function to execute.
     * @returns {Flux<T>} A new Flux.
     */
    public override doFirst(fn: () => void): Flux<T> {
        return super.doFirst(fn) as Flux<T>;
    }

    /**
     * Executes a function when each value is emitted.
     * @param {Function} fn - The function to execute on each value.
     * @returns {Flux<T>} A new Flux.
     */
    public override doOnNext(fn: (value: T) => void): Flux<T> {
        return super.doOnNext(fn) as Flux<T>;
    }

    /**
     * Executes a function when the stream completes.
     * @param {Function} fn - The function to execute on completion.
     * @returns {Flux<T>} A new Flux.
     */
    public override doFinally(fn: () => void): Flux<T> {
        return super.doFinally(fn) as Flux<T>;
    }

    /**
     * Executes a function when a subscription occurs.
     * @param {Function} fn - The function to execute on subscription.
     * @returns {Flux<T>} A new Flux.
     */
    public override doOnSubscribe(fn: (subscriber: Subscriber<T>) => void): Flux<T> {
        return super.doOnSubscribe(fn) as Flux<T>;
    }

    /**
     * Publishes values on a specified scheduler.
     * @param {Scheduler} scheduler - The scheduler to use.
     * @returns {Flux<T>} A new Flux.
     */
    public override publishOn(scheduler: Scheduler): Flux<T> {
        return super.publishOn(scheduler) as Flux<T>;
    }

    /**
     * Subscribes to the stream on a specified scheduler.
     * @param {Scheduler} scheduler - The scheduler to use.
     * @returns {Flux<T>} A new Flux.
     */
    public override subscribeOn(scheduler: Scheduler): Flux<T> {
        return super.subscribeOn(scheduler) as Flux<T>;
    }

    protected sinkType(): 'one' | 'many' {
        return 'many';
    }
}