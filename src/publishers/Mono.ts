import {AbstractPipePublisher} from "@/publishers/PipePublisher";
import {Publisher} from "@/publishers/Publisher";
import OneSink from "@/sinks/OneSink";
import {Sink} from "@/sinks/Sink";
import {Scheduler} from "@/schedulers/Scheduler";
import {combine} from "@/utils";
import {Flux} from "@/publishers/Flux";
import { Subscription } from "@/subscriptions/Subscription";
import { Subscriber } from "@/subscriptions/Subscriber";

export class Mono<T> extends AbstractPipePublisher<T> {

    public static generate<T>(generator: ((sink: Sink<T>) => void)): Mono<T> {
        return new Mono(combine(new OneSink<T>(), generator))
    }

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
            }
        )
    }

    public static just<T>(value: T): Mono<T> {
        return Mono.generate(sink => sink.next(value))
    }

    public static justOrEmpty<T>(value: T | null | undefined): Mono<T> {
        return value == null ? Mono.empty() : Mono.just(value)
    }

    public static empty<T = never>(): Mono<T> {
        return Mono.generate(sink => sink.complete())
    }

    public static error<T = never>(error: any): Mono<T> {
        return Mono.generate(sink => sink.error(error))
    }

    public static fromPromise<T>(promise: Promise<T>): Mono<T> {
        return Mono.generate(sink => promise
            .then((value) => sink.next(value))
            .catch((err) => sink.error(err)))
    }

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

    protected constructor(publisher: Publisher<T>) {
        super(publisher)
    }

    public subscribe({
                         onNext = (value: T) => {
                         },
                         onError = (error: Error) => {
                         },
                         onComplete = () => {
                         }
                     } = {}): Subscription {
        const subscription = this.publisher.subscribe({onNext, onError, onComplete})
        subscription.request(1)
        return subscription
    }

    protected sinkType(): 'one' | 'many' {
        return 'one';
    }

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

    public zipWith<R>(other: Mono<R>): Mono<[T, R]> {
        return this.flatMap(left => other.map(right => [left, right]))
    }

    public zipWhen<R>(fn: (value: T) => Mono<R>): Mono<[T, R]> {
        return this.flatMap((left) => fn(left).map((right) => [left, right]))
    }

    public hasElement(): Mono<boolean> {
        return this.onErrorContinue(_error => true)
            .map(_value => true)
            .switchIfEmpty(Mono.defer(() => Mono.just(false)))
    }

    public toPromise(): Promise<T | null> {
        return new Promise((resolve, reject) => {
            let value: T | null = null
            this.subscribe({
                onNext: (v) => value = v,
                onError: reject,
                onComplete: () => resolve(value)
            })
        })
    }


    public override pipe<R>(producer: (onNext: (value: R) => void, onError: (error: Error) => void, onComplete: () => void) => void, onSubscribe?: (subscriber: Subscriber<R>) => void, onRequest?: (request: number) => void, onUnsubscribe?: () => void): Mono<R> {
        return super.pipe(producer, onSubscribe, onRequest, onUnsubscribe) as Mono<R>;
    }

    public override map<R>(fn: (value: T) => R): Mono<R> {
        return super.map(fn) as Mono<R>;
    }

    public override mapNotNull<R>(fn: (value: T) => (R | null | undefined)): Mono<R> {
        return super.mapNotNull(fn) as Mono<R>;
    }

    public override flatMap<R>(fn: (value: T) => Publisher<R>): Mono<R> {
        return super.flatMap(fn) as Mono<R>;
    }

    public override filter(predicate: (value: T) => boolean): Mono<T> {
        return super.filter(predicate) as Mono<T>;
    }

    public override filterWhen(predicate: (value: T) => Publisher<boolean>): Mono<T> {
        return super.filterWhen(predicate) as Mono<T>;
    }

    public override cast<R>(): Mono<R> {
        return super.cast() as Mono<R>;
    }

    public override switchIfEmpty(alternative: Publisher<T>): Mono<T> {
        return super.switchIfEmpty(alternative) as Mono<T>;
    }

    public override onErrorReturn(replacement: Publisher<T>): Mono<T> {
        return super.onErrorReturn(replacement) as Mono<T>;
    }

    public override onErrorContinue(predicate: (error: Error) => boolean): Mono<T> {
        return super.onErrorContinue(predicate) as Mono<T>;
    }

    public override doFirst(fn: () => void): Mono<T> {
        return super.doFirst(fn) as Mono<T>;
    }

    public override doOnNext(fn: (value: T) => void): Mono<T> {
        return super.doOnNext(fn) as Mono<T>;
    }

    public override doFinally(fn: () => void): Mono<T> {
        return super.doFinally(fn) as Mono<T>;
    }

    public override doOnSubscribe(fn: (subscriber: Subscriber<T>) => void): Mono<T> {
        return super.doOnSubscribe(fn) as Mono<T>;
    }

    public override publishOn(scheduler: Scheduler): Mono<T> {
        return super.publishOn(scheduler) as Mono<T>;
    }

    public override subscribeOn(scheduler: Scheduler): Mono<T> {
        return super.subscribeOn(scheduler) as Mono<T>;
    }
}
