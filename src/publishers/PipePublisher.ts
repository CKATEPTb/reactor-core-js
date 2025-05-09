import {BackpressurePublisher, Publisher} from "@/publishers/Publisher";
import {Subscriber} from "@/subscriptions/Subscriber";
import {Subscription} from "@/subscriptions/Subscription";
import {Scheduler} from "@/schedulers/Scheduler";
import OneSink from "@/sinks/OneSink";
import ManySink from "@/sinks/ManySink";

export interface PipePublisher<T> extends Publisher<T> {
    pipe<R>(producer: (onNext: (value: R) => void, onError: (error: Error) => void, onComplete: () => void) => void, onSubscribe: (subscriber: Subscriber<R>) => void, onRequest: (request: number) => void, onUnsubscribe: () => void): PipePublisher<R>

    map<R>(fn: (value: T) => R): PipePublisher<R>

    mapNotNull<R>(fn: (value: T) => R | null | undefined): PipePublisher<R>

    flatMap<R>(fn: (value: T) => Publisher<R>): PipePublisher<R>

    filter(predicate: (value: T) => boolean): PipePublisher<T>

    filterWhen(predicate: (value: T) => Publisher<boolean>): PipePublisher<T>

    cast<R>(): PipePublisher<R>

    switchIfEmpty(alternative: Publisher<T>): PipePublisher<T>

    onErrorReturn(replacement: Publisher<T>): PipePublisher<T>

    onErrorContinue(predicate: (error: Error) => boolean): PipePublisher<T>

    doFirst(fn: () => void): PipePublisher<T>

    doOnNext(fn: (value: T) => void): PipePublisher<T>

    doFinally(fn: () => void): PipePublisher<T>

    doOnSubscribe(fn: (subscriber: Subscriber<T>) => void): PipePublisher<T>

    publishOn(scheduler: Scheduler): PipePublisher<T>

    subscribeOn(scheduler: Scheduler): PipePublisher<T>
}

export abstract class AbstractPipePublisher<T> implements PipePublisher<T> {
    protected constructor(protected readonly publisher: Publisher<T>) {
    }

    public abstract subscribe(subscriber: Subscriber<T>): Subscription

    protected abstract sinkType(): 'one' | 'many'

    public pipe<R>(producer: (onNext: (value: R) => void, onError: (error: Error) => void, onComplete: () => void) => void, onSubscribe?: (subscriber: Subscriber<R>) => void, onRequest?: (request: number) => void, onUnsubscribe?: () => void): PipePublisher<R> {
        const many = this.sinkType() == 'many';
        const sink = !many ? new OneSink<R>() : new ManySink<R>();
        const unicast = new class A extends BackpressurePublisher<R> {
            public override subscribe(subscriber: Subscriber<R>): Subscription {
                onSubscribe?.(subscriber)
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
        return this.pipe((onNext, onError, onComplete) =>
            sub = this.subscribe({
                onNext, onError, onComplete: () => {
                    try {
                        onComplete()
                    } finally {
                        fn()
                    }
                }
            }), undefined, request => sub?.request(request), () => sub?.unsubscribe())
    }

    public doOnSubscribe(fn: (subscriber: Subscriber<T>) => void): PipePublisher<T> {
        // todo срабатывает без subscribe
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

    private wrap<R>(publisher: Publisher<R>) {
        return Reflect.construct((Reflect.getPrototypeOf(this) as PipePublisher<any>).constructor, [publisher])
    }
}
