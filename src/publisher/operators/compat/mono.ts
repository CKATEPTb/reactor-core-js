/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import {Flux} from "@/publisher/flux.js";
import {Mono} from "@/publisher/mono.js";
import type {PublisherInput, SynchronousSink} from "@/publisher/types.js";
import {Schedulers} from "@/schedulers/schedulers.js";
import type {DurationInput, Scheduler} from "@/schedulers/types.js";
import {Signal} from "@/signal/signal.js";
import type {Subscriber} from "@/subscription/subscriber.js";
import type {ContextView} from "@/context/context-view.js";

declare module "@/publisher/mono.js" {
    /** Reactor compatibility operators added to Mono. */
    interface Mono<T> {
        /** Waits for this Mono and another publisher, ignoring values from both. */
        and(other: PublisherInput<unknown>): Mono<void>;

        /** Passes this Mono to a transformer and returns the transformer's result. */
        as<R>(transformer: (source: Mono<T>) => R): R;

        /** Treats this Mono as a Mono of another TypeScript type. */
        cast<R>(): Mono<R>;

        /** Emits this Mono value and then values from another publisher. */
        concatWith(other: PublisherInput<T>): Flux<T>;

        /** Invokes a callback once after completion, error, or cancellation. */
        doFinally(callback: (signal: "complete" | "error" | "cancel") => void): Mono<T>;

        /** Invokes a callback when this Mono fails. */
        doOnError(callback: (error: unknown) => void): Mono<T>;

        /** Invokes a callback when this Mono emits a value. */
        doOnNext(callback: (value: T) => void): Mono<T>;

        /** Invokes a callback when this Mono completes successfully, with undefined when empty. */
        doOnSuccess(callback: (value: T | undefined) => void): Mono<T>;

        /** Invokes a callback before successful or failed termination is propagated. */
        doOnTerminate(callback: () => void): Mono<T>;

        /** Emits the elapsed scheduler time together with the Mono value. */
        elapsed(scheduler?: Scheduler): Mono<readonly [number, T]>;

        /** Views this Mono as a Flux that emits zero or one value. */
        flux(): Flux<T>;

        /** Handles the value with a synchronous sink that may emit, complete, or fail. */
        handle<R>(handler: (value: T, sink: SynchronousSink<R>) => void): Mono<R>;

        /** Emits true when this Mono emits a value. */
        hasElement(): Mono<boolean>;

        /** Drops the value and completes when this Mono completes successfully. */
        ignoreElement(): Mono<void>;

        /** Converts the Mono value or terminal signal into a `Signal` value. */
        materialize(): Mono<Signal<T>>;

        /** Merges this Mono with another publisher and returns the combined Flux. */
        mergeWith(other: PublisherInput<T>): Flux<T>;

        /** Replaces an error with the result of the provided mapper. */
        onErrorMap(mapper: (error: unknown) => unknown): Mono<T>;

        /** Switches to a fallback publisher when this Mono fails. */
        onErrorResume<R = T>(fallback: (error: unknown) => PublisherInput<R>): Mono<T | R>;

        /** Emits the fallback value when this Mono fails. */
        onErrorReturn(fallbackValue: T): Mono<T>;

        /** Reschedules value delivery on the provided scheduler. */
        publishOn(scheduler: Scheduler): Mono<T>;

        /** Re-subscribes after successful completion and emits repeated values as a Flux. */
        repeat(repeatCount?: number): Flux<T>;

        /** Re-subscribes when this Mono fails, up to the configured retry count. */
        retry(maxRetries?: number): Mono<T>;

        /** Returns this Mono while preserving single-value semantics. */
        single(): Mono<T>;

        /** Schedules subscription work on the provided scheduler. */
        subscribeOn(scheduler: Scheduler): Mono<T>;

        /** Subscribes the provided subscriber and returns the same subscriber instance. */
        subscribeWith<S extends Subscriber<T>>(subscriber: S): S;

        /** Returns this Mono, matching Reactor's zero-or-one `take` semantics. */
        take(): Mono<T>;

        /** Waits for this Mono and then waits for `other`, ignoring all values. */
        thenEmpty(other: PublisherInput<unknown>): Mono<void>;

        /** Waits for this Mono to complete and then emits values from `other`. */
        thenMany<R>(other: PublisherInput<R>): Flux<R>;

        /** Fails or switches to a fallback when this Mono does not signal in time. */
        timeout(timeout: DurationInput, fallback?: PublisherInput<T>, scheduler?: Scheduler): Mono<T>;

        /** Emits the scheduler timestamp together with the Mono value. */
        timestamp(scheduler?: Scheduler): Mono<readonly [number, T]>;

        /** Applies a transformer to this Mono immediately and adapts its result back to Mono. */
        transform<R>(transformer: (source: Mono<T>) => PublisherInput<R>): Mono<R>;

        /** Applies a transformer separately for each subscription. */
        transformDeferred<R>(transformer: (source: Mono<T>) => PublisherInput<R>): Mono<R>;

        /** Applies a transformer per subscription with access to the subscriber context. */
        transformDeferredContextual<R>(
            transformer: (source: Mono<T>, context: ContextView) => PublisherInput<R>
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
    transformer: (source: Mono<T>, context: ContextView) => PublisherInput<R>
): Mono<R> {
    const source = this;
    return new Mono((signal, context) => Mono.from(transformer(source, context.readOnly())).iterate(signal, context));
};
