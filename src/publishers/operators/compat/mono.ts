/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import {type DurationInput, type Scheduler, Schedulers} from "@/scheduler/index.js";
import {Signal} from "@/signal/index.js";
import type {Subscriber} from "@/core/index.js";
import {Flux} from "@/publishers/flux.js";
import {Mono} from "@/publishers/mono.js";
import type {PublisherInput, SynchronousSink} from "@/publishers/types.js";

declare module "@/publishers/mono.js" {
    /** Reactor compatibility operators added to Mono. */
    interface Mono<T> {
        /** Reactor and operator. */
        and(other: PublisherInput<unknown>): Mono<void>;

        /** Reactor as operator. */
        as<R>(transformer: (source: Mono<T>) => R): R;

        /** Reactor cast operator. */
        cast<R>(): Mono<R>;

        /** Reactor concatWith operator. */
        concatWith(other: PublisherInput<T>): Flux<T>;

        /** Reactor doFinally operator. */
        doFinally(callback: (signal: "complete" | "error" | "cancel") => void): Mono<T>;

        /** Reactor doOnError operator. */
        doOnError(callback: (error: unknown) => void): Mono<T>;

        /** Reactor doOnNext operator. */
        doOnNext(callback: (value: T) => void): Mono<T>;

        /** Reactor doOnSuccess operator. */
        doOnSuccess(callback: (value: T | undefined) => void): Mono<T>;

        /** Reactor doOnTerminate operator. */
        doOnTerminate(callback: () => void): Mono<T>;

        /** Reactor elapsed operator. */
        elapsed(scheduler?: Scheduler): Mono<readonly [number, T]>;

        /** Reactor flux operator. */
        flux(): Flux<T>;

        /** Reactor handle operator. */
        handle<R>(handler: (value: T, sink: SynchronousSink<R>) => void): Mono<R>;

        /** Reactor hasElement operator. */
        hasElement(): Mono<boolean>;

        /** Reactor ignoreElement operator. */
        ignoreElement(): Mono<void>;

        /** Reactor materialize operator. */
        materialize(): Mono<Signal<T>>;

        /** Reactor mergeWith operator. */
        mergeWith(other: PublisherInput<T>): Flux<T>;

        /** Reactor onErrorMap operator. */
        onErrorMap(mapper: (error: unknown) => unknown): Mono<T>;

        /** Reactor onErrorResume operator. */
        onErrorResume<R = T>(fallback: (error: unknown) => PublisherInput<R>): Mono<T | R>;

        /** Reactor onErrorReturn operator. */
        onErrorReturn(fallbackValue: T): Mono<T>;

        /** Reactor publishOn operator. */
        publishOn(scheduler: Scheduler): Mono<T>;

        /** Reactor repeat operator. */
        repeat(repeatCount?: number): Flux<T>;

        /** Reactor retry operator. */
        retry(maxRetries?: number): Mono<T>;

        /** Reactor single operator. */
        single(): Mono<T>;

        /** Reactor subscribeOn operator. */
        subscribeOn(scheduler: Scheduler): Mono<T>;

        /** Reactor subscribeWith operator. */
        subscribeWith<S extends Subscriber<T>>(subscriber: S): S;

        /** Reactor take operator. */
        take(): Mono<T>;

        /** Reactor thenEmpty operator. */
        thenEmpty(other: PublisherInput<unknown>): Mono<void>;

        /** Reactor thenMany operator. */
        thenMany<R>(other: PublisherInput<R>): Flux<R>;

        /** Reactor timeout operator. */
        timeout(timeout: DurationInput, fallback?: PublisherInput<T>, scheduler?: Scheduler): Mono<T>;

        /** Reactor timestamp operator. */
        timestamp(scheduler?: Scheduler): Mono<readonly [number, T]>;

        /** Reactor transform operator. */
        transform<R>(transformer: (source: Mono<T>) => PublisherInput<R>): Mono<R>;

        /** Reactor transformDeferred operator. */
        transformDeferred<R>(transformer: (source: Mono<T>) => PublisherInput<R>): Mono<R>;

        /** Reactor transformDeferredContextual operator. */
        transformDeferredContextual<R>(
            transformer: (source: Mono<T>, context: import("@/context/index.js").ContextView) => PublisherInput<R>
        ): Mono<R>;
    }
}


Mono.prototype.and = function and<T>(this: Mono<T>, other: PublisherInput<unknown>): Mono<void> {
    return this.thenEmpty(other);
};

Mono.prototype.as = function as<T, R>(this: Mono<T>, transformer: (source: Mono<T>) => R): R {
    return transformer(this);
};

Mono.prototype.cast = function cast<T, R>(this: Mono<T>): Mono<R> {
    return this as unknown as Mono<R>;
};

Mono.prototype.concatWith = function concatWith<T>(this: Mono<T>, other: PublisherInput<T>): Flux<T> {
    return this.flux().concatWith(other);
};

Mono.prototype.doFinally = function doFinally<T>(
    this: Mono<T>,
    callback: (signal: "complete" | "error" | "cancel") => void
): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.doFinally.call(this, callback).iterate(signal, context));
};

Mono.prototype.doOnError = function doOnError<T>(this: Mono<T>, callback: (error: unknown) => void): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.doOnError.call(this, callback).iterate(signal, context));
};

Mono.prototype.doOnNext = function doOnNext<T>(this: Mono<T>, callback: (value: T) => void): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.doOnNext.call(this, callback).iterate(signal, context));
};

Mono.prototype.doOnSuccess = function doOnSuccess<T>(this: Mono<T>, callback: (value: T | undefined) => void): Mono<T> {
    const source = this;
    return new Mono(async function* (signal, context) {
        let seen = false;
        let value: T | undefined;
        for await (const next of source.iterate(signal, context)) {
            seen = true;
            value = next;
            yield next;
        }
        callback(seen ? value : undefined);
    });
};

Mono.prototype.doOnTerminate = function doOnTerminate<T>(this: Mono<T>, callback: () => void): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.doFinally.call(this, event => {
        if (event !== "cancel") {
            callback();
        }
    }).iterate(signal, context));
};

Mono.prototype.elapsed = function elapsed<T>(
    this: Mono<T>,
    scheduler: Scheduler = Schedulers.timeout()
): Mono<readonly [number, T]> {
    return new Mono((signal, context) => Flux.prototype.elapsed.call(this, scheduler).iterate(signal, context));
};

Mono.prototype.flux = function flux<T>(this: Mono<T>): Flux<T> {
    return new Flux((signal, context) => this.iterate(signal, context));
};

Mono.prototype.handle = function handle<T, R>(
    this: Mono<T>,
    handler: (value: T, sink: SynchronousSink<R>) => void
): Mono<R> {
    return new Mono((signal, context) => (Flux.prototype.handle.call(this, handler) as Flux<R>).iterate(signal, context));
};

Mono.prototype.hasElement = function hasElement<T>(this: Mono<T>): Mono<boolean> {
    return this.map(() => true).defaultIfEmpty(false);
};

Mono.prototype.ignoreElement = function ignoreElement<T>(this: Mono<T>): Mono<void> {
    return this.then();
};

Mono.prototype.materialize = function materialize<T>(this: Mono<T>): Mono<Signal<T>> {
    return new Mono((signal, context) => Flux.prototype.materialize.call(this).iterate(signal, context));
};

Mono.prototype.mergeWith = function mergeWith<T>(this: Mono<T>, other: PublisherInput<T>): Flux<T> {
    return this.flux().mergeWith(other);
};

Mono.prototype.onErrorMap = function onErrorMap<T>(this: Mono<T>, mapper: (error: unknown) => unknown): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.onErrorMap.call(this, mapper).iterate(signal, context));
};

Mono.prototype.onErrorResume = function onErrorResume<T, R = T>(
    this: Mono<T>,
    fallback: (error: unknown) => PublisherInput<R>
): Mono<T | R> {
    return new Mono((signal, context) => Flux.prototype.onErrorResume.call(this, fallback).iterate(signal, context));
};

Mono.prototype.onErrorReturn = function onErrorReturn<T>(this: Mono<T>, fallbackValue: T): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.onErrorReturn.call(this, fallbackValue).iterate(signal, context));
};

Mono.prototype.publishOn = function publishOn<T>(this: Mono<T>, scheduler: Scheduler): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.publishOn.call(this, scheduler).iterate(signal, context));
};

Mono.prototype.repeat = function repeat<T>(this: Mono<T>, repeatCount?: number): Flux<T> {
    return Flux.prototype.repeat.call(this, repeatCount) as Flux<T>;
};

Mono.prototype.retry = function retry<T>(this: Mono<T>, maxRetries?: number): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.retry.call(this, maxRetries).iterate(signal, context));
};

Mono.prototype.single = function single<T>(this: Mono<T>): Mono<T> {
    return this;
};

Mono.prototype.subscribeOn = function subscribeOn<T>(this: Mono<T>, scheduler: Scheduler): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.subscribeOn.call(this, scheduler).iterate(signal, context));
};

Mono.prototype.subscribeWith = function subscribeWith<T, S extends Subscriber<T>>(this: Mono<T>, subscriber: S): S {
    this.subscribe(subscriber);
    return subscriber;
};

Mono.prototype.take = function take<T>(this: Mono<T>): Mono<T> {
    return this;
};

Mono.prototype.thenEmpty = function thenEmpty<T>(this: Mono<T>, other: PublisherInput<unknown>): Mono<void> {
    return Flux.prototype.thenEmpty.call(this, other);
};

Mono.prototype.thenMany = function thenMany<T, R>(this: Mono<T>, other: PublisherInput<R>): Flux<R> {
    return Flux.prototype.thenMany.call(this, other) as Flux<R>;
};

Mono.prototype.timeout = function timeout<T>(
    this: Mono<T>,
    timeout: DurationInput,
    fallback?: PublisherInput<T>,
    scheduler?: Scheduler
): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.timeout.call(this, timeout, fallback, scheduler).iterate(signal, context));
};

Mono.prototype.timestamp = function timestamp<T>(
    this: Mono<T>,
    scheduler: Scheduler = Schedulers.timeout()
): Mono<readonly [number, T]> {
    return new Mono((signal, context) => Flux.prototype.timestamp.call(this, scheduler).iterate(signal, context));
};

Mono.prototype.transform = function transform<T, R>(
    this: Mono<T>,
    transformer: (source: Mono<T>) => PublisherInput<R>
): Mono<R> {
    return Mono.from(transformer(this));
};

Mono.prototype.transformDeferred = function transformDeferred<T, R>(
    this: Mono<T>,
    transformer: (source: Mono<T>) => PublisherInput<R>
): Mono<R> {
    const source = this;
    return Mono.defer(() => transformer(source));
};

Mono.prototype.transformDeferredContextual = function transformDeferredContextual<T, R>(
    this: Mono<T>,
    transformer: (source: Mono<T>, context: import("@/context/index.js").ContextView) => PublisherInput<R>
): Mono<R> {
    const source = this;
    return new Mono((signal, context) => Mono.from(transformer(source, context.readOnly())).iterate(signal, context));
};
