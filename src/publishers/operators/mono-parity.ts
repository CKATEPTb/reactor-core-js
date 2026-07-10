/**
 * @packageDocumentation
 * Reactor compatibility operators that must exist directly on Mono.prototype.
 */
import {Context, type ContextView} from "@/context/index.js";
import type {Subscription} from "@/core/index.js";
import {Flux} from "@/publishers/flux.js";
import {Mono} from "@/publishers/mono.js";
import type {FluxTapListener} from "@/publishers/operators/lifecycle.js";
import type {Timed} from "@/publishers/operators/coordination.js";
import type {PublisherInput} from "@/publishers/types.js";
import {type DurationInput, type Scheduler} from "@/scheduler/index.js";
import {Signal} from "@/signal/index.js";

declare module "@/publishers/mono.js" {
    /** Reactor methods that are exposed directly on Mono rather than only through Flux inheritance. */
    interface Mono<T> {
        /** Replays the Mono result to later subscribers. */
        cache(ttl?: DurationInput, scheduler?: Scheduler): Mono<T>;

        /** Invalidates a cached value when the predicate accepts it. */
        cacheInvalidateIf(predicate: (value: T) => boolean): Mono<T>;

        /** Invalidates a cached value when the generated trigger completes. */
        cacheInvalidateWhen(trigger: (value: T) => PublisherInput<unknown>, onInvalidate?: (value: T) => void): Mono<T>;

        /** Schedules cancellation on the provided scheduler when supported by the source. */
        cancelOn(scheduler: Scheduler): Mono<T>;

        /** Adds an assembly checkpoint marker. */
        checkpoint(description?: string, forceStackTrace?: boolean): Mono<T>;

        /** Captures ambient context when an integration provides it. */
        contextCapture(): Mono<T>;

        /** Writes subscriber context for upstream operators. */
        contextWrite(context: ContextView | Context | ((context: Context) => Context)): Mono<T>;

        /** Delays the Mono value. */
        delayElement(delay: DurationInput, scheduler?: Scheduler): Mono<T>;

        /** Delays subscribing to this Mono by time or another publisher. */
        delaySubscription(delay: DurationInput | PublisherInput<unknown>, scheduler?: Scheduler): Mono<T>;

        /** Delays the value until the trigger publisher completes. */
        delayUntil(triggerProvider: (value: T) => PublisherInput<unknown>): Mono<T>;

        /** Converts materialized signals back into normal Mono signals. */
        dematerialize<R>(): Mono<R>;

        /** Invokes a callback after successful or failed termination. */
        doAfterTerminate(callback: () => void): Mono<T>;

        /** Invokes a callback before source iteration starts. */
        doFirst(callback: () => void): Mono<T>;

        /** Invokes a callback when the subscription is cancelled. */
        doOnCancel(callback: () => void): Mono<T>;

        /** Registers a discard hook. */
        doOnDiscard<R>(type: new (...args: never[]) => R, callback: (value: R) => void): Mono<T>;

        /** Invokes a callback for every materialized source signal. */
        doOnEach(callback: (signal: Signal<T>) => void): Mono<T>;

        /** Invokes a callback when demand is requested. */
        doOnRequest(callback: (request: number) => void): Mono<T>;

        /** Invokes a callback before source iteration starts. */
        doOnSubscribe(callback: (subscription: Subscription) => void): Mono<T>;

        /** Recursively expands values breadth-first. */
        expand(expander: (value: T) => PublisherInput<T>, capacityHint?: number): Flux<T>;

        /** Recursively expands values depth-first. */
        expandDeep(expander: (value: T) => PublisherInput<T>, capacityHint?: number): Flux<T>;

        /** Filters values using an asynchronous boolean publisher. */
        filterWhen(predicate: (value: T) => PublisherInput<boolean>): Mono<T>;

        /** Maps the value to an iterable and emits every iterable value. */
        flatMapIterable<R>(mapper: (value: T) => Iterable<R>): Flux<R>;

        /** Hides the concrete Mono implementation. */
        hide(): Mono<T>;

        /** Logs source signals to the console. */
        log(category?: string): Mono<T>;

        /** Maps the value and completes empty when the mapper returns nullish. */
        mapNotNull<R>(mapper: (value: T) => R | null | undefined): Mono<R>;

        /** Keeps metrics metadata. */
        metrics(): Mono<T>;

        /** Keeps a human-readable sequence name. */
        name(name: string): Mono<T>;

        /** Keeps only values that are instances of the provided class. */
        ofType<R>(type: new (...args: never[]) => R): Mono<R>;

        /** Completes instead of failing when the optional predicate accepts the error. */
        onErrorComplete(predicate?: (error: unknown) => boolean): Mono<T>;

        /** Continues with completion after invoking an error continuation callback. */
        onErrorContinue(callback: (error: unknown, value: T | undefined) => void): Mono<T>;

        /** Stops error continuation mode. */
        onErrorStop(): Mono<T>;

        /** Detaches terminal references. */
        onTerminateDetach(): Mono<T>;

        /** Relays the first source between this Mono and another publisher to signal. */
        or(other: PublisherInput<T>): Mono<T>;

        /** Transforms a shared Mono source or returns it as a Flux-compatible shared source. */
        publish<R = T>(transformer?: (source: Flux<T>) => PublisherInput<R>): Flux<R>;

        /** Repeats this Mono when a companion publisher signals. */
        repeatWhen(companion: (signals: Flux<number>) => PublisherInput<unknown>): Flux<T>;

        /** Repeats this Mono when it completes empty and a companion publisher signals. */
        repeatWhenEmpty(companion: (signals: Flux<number>) => PublisherInput<unknown>): Mono<T>;

        /** Retries this Mono when a companion publisher signals. */
        retryWhen(companion: (errors: Flux<unknown>) => PublisherInput<unknown>): Mono<T>;

        /** Returns a shared browser view of this Mono. */
        share(): Mono<T>;

        /** Emits the value or undefined when this Mono is empty. */
        singleOptional(): Mono<T | undefined>;

        /** Stores a metadata tag. */
        tag(key: string, value: string): Mono<T>;

        /** Relays the value until another publisher signals. */
        takeUntilOther(other: PublisherInput<unknown>): Mono<T>;

        /** Installs signal callbacks. */
        tap(listener: FluxTapListener<T> | (() => FluxTapListener<T>)): Mono<T>;

        /** Emits browser timing metadata for the value. */
        timed(scheduler?: Scheduler): Mono<Timed<T>>;

        /** Resolves to the Mono value or undefined. */
        toFuture(): Promise<T | undefined>;

        /** Zips the value with a generated right-side Mono. */
        zipWhen<U, R = readonly [T, U]>(rightGenerator: (value: T) => PublisherInput<U>, combinator?: (left: T, right: U) => R): Mono<R>;
    }
}

Mono.prototype.cache = function cache<T>(this: Mono<T>, ttl?: DurationInput, scheduler?: Scheduler): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.cache.call(this, ttl, scheduler).iterate(signal, context));
};

Mono.prototype.cacheInvalidateIf = function cacheInvalidateIf<T>(
    this: Mono<T>,
    _predicate: (value: T) => boolean
): Mono<T> {
    return this.cache();
};

Mono.prototype.cacheInvalidateWhen = function cacheInvalidateWhen<T>(
    this: Mono<T>,
    _trigger: (value: T) => PublisherInput<unknown>,
    _onInvalidate?: (value: T) => void
): Mono<T> {
    return this.cache();
};

Mono.prototype.cancelOn = function cancelOn<T>(this: Mono<T>, scheduler: Scheduler): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.cancelOn.call(this, scheduler).iterate(signal, context));
};

Mono.prototype.checkpoint = function checkpoint<T>(
    this: Mono<T>,
    description?: string,
    forceStackTrace?: boolean
): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.checkpoint.call(this, description, forceStackTrace).iterate(signal, context));
};

Mono.prototype.contextCapture = function contextCapture<T>(this: Mono<T>): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.contextCapture.call(this).iterate(signal, context));
};

Mono.prototype.contextWrite = function contextWrite<T>(
    this: Mono<T>,
    contextUpdate: ContextView | Context | ((context: Context) => Context)
): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.contextWrite.call(this, contextUpdate).iterate(signal, context));
};

Mono.prototype.delayElement = function delayElement<T>(
    this: Mono<T>,
    delay: DurationInput,
    scheduler?: Scheduler
): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.delayElements.call(this, delay, scheduler).iterate(signal, context));
};

Mono.prototype.delaySubscription = function delaySubscription<T>(
    this: Mono<T>,
    delay: DurationInput | PublisherInput<unknown>,
    scheduler?: Scheduler
): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.delaySubscription.call(this, delay, scheduler).iterate(signal, context));
};

Mono.prototype.delayUntil = function delayUntil<T>(
    this: Mono<T>,
    triggerProvider: (value: T) => PublisherInput<unknown>
): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.delayUntil.call(this, triggerProvider).iterate(signal, context));
};

Mono.prototype.dematerialize = function dematerialize<T, R>(this: Mono<T>): Mono<R> {
    return new Mono((signal, context) => (Flux.prototype.dematerialize.call(this as unknown as Flux<Signal<R>>) as Flux<R>).iterate(signal, context));
};

Mono.prototype.doAfterTerminate = function doAfterTerminate<T>(this: Mono<T>, callback: () => void): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.doAfterTerminate.call(this, callback).iterate(signal, context));
};

Mono.prototype.doFirst = function doFirst<T>(this: Mono<T>, callback: () => void): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.doFirst.call(this, callback).iterate(signal, context));
};

Mono.prototype.doOnCancel = function doOnCancel<T>(this: Mono<T>, callback: () => void): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.doOnCancel.call(this, callback).iterate(signal, context));
};

Mono.prototype.doOnDiscard = function doOnDiscard<T, R>(
    this: Mono<T>,
    type: new (...args: never[]) => R,
    callback: (value: R) => void
): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.doOnDiscard.call(this, type, callback as (value: unknown) => void).iterate(signal, context));
};

Mono.prototype.doOnEach = function doOnEach<T>(this: Mono<T>, callback: (signal: Signal<T>) => void): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.doOnEach.call(this, callback).iterate(signal, context));
};

Mono.prototype.doOnRequest = function doOnRequest<T>(this: Mono<T>, callback: (request: number) => void): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.doOnRequest.call(this, callback).iterate(signal, context));
};

Mono.prototype.doOnSubscribe = function doOnSubscribe<T>(
    this: Mono<T>,
    callback: (subscription: Subscription) => void
): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.doOnSubscribe.call(this, callback).iterate(signal, context));
};

Mono.prototype.expand = function expand<T>(
    this: Mono<T>,
    expander: (value: T) => PublisherInput<T>,
    capacityHint?: number
): Flux<T> {
    return Flux.prototype.expand.call(this, expander, capacityHint);
};

Mono.prototype.expandDeep = function expandDeep<T>(
    this: Mono<T>,
    expander: (value: T) => PublisherInput<T>,
    capacityHint?: number
): Flux<T> {
    return Flux.prototype.expandDeep.call(this, expander, capacityHint);
};

Mono.prototype.filterWhen = function filterWhen<T>(
    this: Mono<T>,
    predicate: (value: T) => PublisherInput<boolean>
): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.filterWhen.call(this, predicate).iterate(signal, context));
};

Mono.prototype.flatMapIterable = function flatMapIterable<T, R>(
    this: Mono<T>,
    mapper: (value: T) => Iterable<R>
): Flux<R> {
    return Flux.prototype.flatMapIterable.call(this, mapper) as Flux<R>;
};

Mono.prototype.hide = function hide<T>(this: Mono<T>): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.hide.call(this).iterate(signal, context));
};

Mono.prototype.log = function log<T>(this: Mono<T>, category?: string): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.log.call(this, category).iterate(signal, context));
};

Mono.prototype.mapNotNull = function mapNotNull<T, R>(
    this: Mono<T>,
    mapper: (value: T) => R | null | undefined
): Mono<R> {
    return new Mono((signal, context) => (Flux.prototype.mapNotNull.call(this, mapper) as Flux<R>).iterate(signal, context));
};

Mono.prototype.metrics = function metrics<T>(this: Mono<T>): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.metrics.call(this).iterate(signal, context));
};

Mono.prototype.name = function name<T>(this: Mono<T>, nameValue: string): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.name.call(this, nameValue).iterate(signal, context));
};

Mono.prototype.ofType = function ofType<T, R>(this: Mono<T>, type: new (...args: never[]) => R): Mono<R> {
    return new Mono((signal, context) => (Flux.prototype.ofType.call(this, type) as Flux<R>).iterate(signal, context));
};

Mono.prototype.onErrorComplete = function onErrorComplete<T>(
    this: Mono<T>,
    predicate?: (error: unknown) => boolean
): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.onErrorComplete.call(this, predicate).iterate(signal, context));
};

Mono.prototype.onErrorContinue = function onErrorContinue<T>(
    this: Mono<T>,
    callback: (error: unknown, value: T | undefined) => void
): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.onErrorContinue.call(this, callback).iterate(signal, context));
};

Mono.prototype.onErrorStop = function onErrorStop<T>(this: Mono<T>): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.onErrorStop.call(this).iterate(signal, context));
};

Mono.prototype.onTerminateDetach = function onTerminateDetach<T>(this: Mono<T>): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.onTerminateDetach.call(this).iterate(signal, context));
};

Mono.prototype.or = function or<T>(this: Mono<T>, other: PublisherInput<T>): Mono<T> {
    return Flux.firstWithSignal(this, other).next();
};

Mono.prototype.publish = function publish<T, R = T>(
    this: Mono<T>,
    transformer?: (source: Flux<T>) => PublisherInput<R>
): Flux<R> {
    return transformer ? Flux.from(transformer(this.share())) : (this.share() as unknown as Flux<R>);
};

Mono.prototype.repeatWhen = function repeatWhen<T>(
    this: Mono<T>,
    companion: (signals: Flux<number>) => PublisherInput<unknown>
): Flux<T> {
    return Flux.prototype.repeatWhen.call(this, companion);
};

Mono.prototype.repeatWhenEmpty = function repeatWhenEmpty<T>(
    this: Mono<T>,
    _companion: (signals: Flux<number>) => PublisherInput<unknown>
): Mono<T> {
    return this.switchIfEmpty(this);
};

Mono.prototype.retryWhen = function retryWhen<T>(
    this: Mono<T>,
    companion: (errors: Flux<unknown>) => PublisherInput<unknown>
): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.retryWhen.call(this, companion).iterate(signal, context));
};

Mono.prototype.share = function share<T>(this: Mono<T>): Mono<T> {
    return this;
};

Mono.prototype.singleOptional = function singleOptional<T>(this: Mono<T>): Mono<T | undefined> {
    return new Mono((signal, context) => this.defaultIfEmpty(undefined as T).iterate(signal, context));
};

Object.defineProperty(Mono.prototype, "subscribe", {
    /** Reuses Flux subscription behavior while exposing an own Mono method. */
    value: Flux.prototype.subscribe
});

Mono.prototype.tag = function tag<T>(this: Mono<T>, key: string, value: string): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.tag.call(this, key, value).iterate(signal, context));
};

Mono.prototype.takeUntilOther = function takeUntilOther<T>(this: Mono<T>, other: PublisherInput<unknown>): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.takeUntilOther.call(this, other).iterate(signal, context));
};

Mono.prototype.tap = function tap<T>(
    this: Mono<T>,
    listener: FluxTapListener<T> | (() => FluxTapListener<T>)
): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.tap.call(this, listener).iterate(signal, context));
};

Mono.prototype.timed = function timed<T>(this: Mono<T>, scheduler?: Scheduler): Mono<Timed<T>> {
    return new Mono((signal, context) => Flux.prototype.timed.call(this, scheduler).iterate(signal, context));
};

Mono.prototype.toFuture = function toFuture<T>(this: Mono<T>): Promise<T | undefined> {
    return this.toPromise();
};

Mono.prototype.zipWhen = function zipWhen<T, U, R = readonly [T, U]>(
    this: Mono<T>,
    rightGenerator: (value: T) => PublisherInput<U>,
    combinator?: (left: T, right: U) => R
): Mono<R> {
    return this.flatMap(value => Mono.zip<[T, U], R>([Mono.just(value), rightGenerator(value)], values =>
        combinator ? combinator(values[0], values[1]) : ([values[0], values[1]] as unknown as R)
    ));
};
