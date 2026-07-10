/**
 * @packageDocumentation
 * Reactor lifecycle, context, logging and browser backpressure compatibility operators.
 */
import {Context, ContextView} from "@/context/index.js";
import {Flux} from "@/publishers/flux.js";
import {scheduleDelay} from "@/publishers/helpers.js";
import type {PublisherInput} from "@/publishers/types.js";
import {type DurationInput, type Scheduler, Schedulers} from "@/scheduler/index.js";
import {Signal} from "@/signal/index.js";
import type {Subscription} from "@/subscriptions/index.js";

/** Listener shape accepted by the lightweight `tap` compatibility operator. */
export interface FluxTapListener<T> {
    /** Invoked for each next value. */
    onNext?(value: T): void;

    /** Invoked when the sequence fails. */
    onError?(error: unknown): void;

    /** Invoked when the sequence completes. */
    onComplete?(): void;

    /** Invoked when the sequence is cancelled. */
    onCancel?(): void;

    /** Invoked for materialized next, error and complete signals. */
    onSignal?(signal: Signal<T>): void;
}

declare module "@/publishers/flux.js" {
    /** Lifecycle, context and browser backpressure compatibility operators added to Flux. */
    interface Flux<T> {
        /** Replays the collected source values to later subscribers. */
        cache(ttl?: DurationInput, scheduler?: Scheduler): Flux<T>;

        /** Schedules cancellation on the provided scheduler when supported by the source. */
        cancelOn(scheduler: Scheduler): Flux<T>;

        /** Adds an assembly checkpoint marker. Browser implementation keeps the source unchanged. */
        checkpoint(description?: string, forceStackTrace?: boolean): Flux<T>;

        /** Captures ambient context when an integration provides it. Browser implementation keeps the source unchanged. */
        contextCapture(): Flux<T>;

        /** Writes subscriber context for upstream operators. */
        contextWrite(context: ContextView | Context | ((context: Context) => Context)): Flux<T>;

        /** Delays subscribing to the source. */
        delaySequence(delay: DurationInput, scheduler?: Scheduler): Flux<T>;

        /** Delays subscribing to the source by time or until another publisher signals. */
        delaySubscription(delay: DurationInput | PublisherInput<unknown>, scheduler?: Scheduler): Flux<T>;

        /** Delays each value until the trigger publisher derived from that value completes. */
        delayUntil(triggerProvider: (value: T) => PublisherInput<unknown>): Flux<T>;

        /** Invokes a callback after successful or failed termination. */
        doAfterTerminate(callback: () => void): Flux<T>;

        /** Invokes a callback before source iteration starts. */
        doFirst(callback: () => void): Flux<T>;

        /** Invokes a callback when the subscription is cancelled. */
        doOnCancel(callback: () => void): Flux<T>;

        /** Registers a discard hook. Browser implementation keeps values unchanged. */
        doOnDiscard<R>(type: new (...args: never[]) => R, callback: (value: R) => void): Flux<T>;

        /** Invokes a callback for every materialized source signal. */
        doOnEach(callback: (signal: Signal<T>) => void): Flux<T>;

        /** Invokes a callback when unbounded demand is requested by the async bridge. */
        doOnRequest(callback: (request: number) => void): Flux<T>;

        /** Invokes a callback with a synthetic subscription before source iteration starts. */
        doOnSubscribe(callback: (subscription: Subscription) => void): Flux<T>;

        /** Invokes a callback before successful or failed termination. */
        doOnTerminate(callback: () => void): Flux<T>;

        /** Keeps request batching metadata. Browser async iteration returns this source unchanged. */
        limitRate(highTide: number, lowTide?: number): Flux<T>;

        /** Limits the total number of requested values. */
        limitRequest(n: number): Flux<T>;

        /** Logs source signals to the console. */
        log(category?: string): Flux<T>;

        /** Keeps metrics metadata. Browser implementation returns this source unchanged. */
        metrics(): Flux<T>;

        /** Keeps a human-readable sequence name. Browser implementation returns this source unchanged. */
        name(name: string): Flux<T>;

        /** Buffers on backpressure. Browser async iteration already buffers by pull demand. */
        onBackpressureBuffer(...args: unknown[]): Flux<T>;

        /** Drops on backpressure. Browser async iteration keeps pull demand and returns this source unchanged. */
        onBackpressureDrop(callback?: (value: T) => void): Flux<T>;

        /** Fails on backpressure. Browser async iteration keeps pull demand and returns this source unchanged. */
        onBackpressureError(): Flux<T>;

        /** Keeps only the latest value on backpressure. Browser async iteration returns this source unchanged. */
        onBackpressureLatest(): Flux<T>;

        /** Completes instead of failing when the optional predicate accepts the error. */
        onErrorComplete(predicate?: (error: unknown) => boolean): Flux<T>;

        /** Continues with completion after invoking an error continuation callback. */
        onErrorContinue(callback: (error: unknown, value: T | undefined) => void): Flux<T>;

        /** Stops error continuation mode. Browser implementation returns this source unchanged. */
        onErrorStop(): Flux<T>;

        /** Detaches terminal references. Browser implementation returns this source unchanged. */
        onTerminateDetach(): Flux<T>;

        /** Transforms a shared source or returns a shared source view. */
        publish<R = T>(transformer?: (source: Flux<T>) => PublisherInput<R>): Flux<R>;

        /** Returns a Mono with the next value from this source. */
        publishNext(): import("@/publishers/mono.js").Mono<T>;

        /** Returns a replaying view of this source. */
        replay(...args: unknown[]): Flux<T>;

        /** Returns a shared browser view of this source. */
        share(): Flux<T>;

        /** Returns a Mono with the next value from a shared source. */
        shareNext(): import("@/publishers/mono.js").Mono<T>;

        /** Stores a metadata tag. Browser implementation returns this source unchanged. */
        tag(key: string, value: string): Flux<T>;

        /** Installs signal callbacks. */
        tap(listener: FluxTapListener<T> | (() => FluxTapListener<T>)): Flux<T>;

        /** Returns an async iterator stream view of this Flux. */
        toStream(): AsyncIterable<T>;
    }
}

Flux.prototype.cache = function cache<T>(this: Flux<T>, _ttl?: DurationInput, _scheduler?: Scheduler): Flux<T> {
    const source = this;
    let values: T[] | undefined;
    let failure: unknown;
    let loaded: Promise<void> | undefined;
    return new Flux(async function* (signal, context) {
        loaded ??= (async () => {
            const nextValues: T[] = [];
            try {
                for await (const value of source.iterate(signal, context)) {
                    nextValues.push(value);
                }
                values = nextValues;
            } catch (error) {
                failure = error;
            }
        })();
        await loaded;
        if (failure !== undefined) {
            throw failure;
        }
        yield* values ?? [];
    });
};

Flux.prototype.cancelOn = function cancelOn<T>(this: Flux<T>, _scheduler: Scheduler): Flux<T> {
    return this;
};

Flux.prototype.checkpoint = function checkpoint<T>(
    this: Flux<T>,
    _description?: string,
    _forceStackTrace?: boolean
): Flux<T> {
    return this;
};

Flux.prototype.contextCapture = function contextCapture<T>(this: Flux<T>): Flux<T> {
    return this;
};

Flux.prototype.contextWrite = function contextWrite<T>(
    this: Flux<T>,
    contextUpdate: ContextView | Context | ((context: Context) => Context)
): Flux<T> {
    const source = this;
    return new Flux((signal, context) => {
        const nextContext =
            typeof contextUpdate === "function"
                ? contextUpdate(context)
                : context.putAll(contextUpdate);
        return source.iterate(signal, nextContext);
    });
};

Flux.prototype.delaySequence = function delaySequence<T>(
    this: Flux<T>,
    delay: DurationInput,
    scheduler?: Scheduler
): Flux<T> {
    return this.delaySubscription(delay, scheduler);
};

Flux.prototype.delaySubscription = function delaySubscription<T>(
    this: Flux<T>,
    delay: DurationInput | PublisherInput<unknown>,
    scheduler?: Scheduler
): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        if (isDuration(delay)) {
            await scheduleDelay(scheduler ?? Schedulers.timeout(), delay, signal);
        } else {
            await drain(delay, signal, context);
        }
        for await (const value of source.iterate(signal, context)) {
            yield value;
        }
    });
};

Flux.prototype.delayUntil = function delayUntil<T>(
    this: Flux<T>,
    triggerProvider: (value: T) => PublisherInput<unknown>
): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        for await (const value of source.iterate(signal, context)) {
            await drain(triggerProvider(value), signal, context);
            yield value;
        }
    });
};

Flux.prototype.doAfterTerminate = function doAfterTerminate<T>(this: Flux<T>, callback: () => void): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        let terminated = false;
        try {
            for await (const value of source.iterate(signal, context)) {
                yield value;
            }
            terminated = true;
        } finally {
            if (terminated) {
                callback();
            }
        }
    });
};

Flux.prototype.doFirst = function doFirst<T>(this: Flux<T>, callback: () => void): Flux<T> {
    const source = this;
    return new Flux((signal, context) => {
        callback();
        return source.iterate(signal, context);
    });
};

Flux.prototype.doOnCancel = function doOnCancel<T>(this: Flux<T>, callback: () => void): Flux<T> {
    const source = this;
    return new Flux((signal, context) => {
        signal.addEventListener("abort", callback, {once: true});
        return source.iterate(signal, context);
    });
};

Flux.prototype.doOnDiscard = function doOnDiscard<T, R>(
    this: Flux<T>,
    _type: new (...args: never[]) => R,
    _callback: (value: R) => void
): Flux<T> {
    return this;
};

Flux.prototype.doOnEach = function doOnEach<T>(this: Flux<T>, callback: (signal: Signal<T>) => void): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        try {
            for await (const value of source.iterate(signal, context)) {
                callback(Signal.next(value));
                yield value;
            }
            callback(Signal.complete());
        } catch (error) {
            callback(Signal.error(error));
            throw error;
        }
    });
};

Flux.prototype.doOnRequest = function doOnRequest<T>(this: Flux<T>, callback: (request: number) => void): Flux<T> {
    return this.doFirst(() => callback(Number.POSITIVE_INFINITY));
};

Flux.prototype.doOnSubscribe = function doOnSubscribe<T>(
    this: Flux<T>,
    callback: (subscription: Subscription) => void
): Flux<T> {
    return this.doFirst(() => callback(noopSubscription()));
};

Flux.prototype.doOnTerminate = function doOnTerminate<T>(this: Flux<T>, callback: () => void): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        try {
            for await (const value of source.iterate(signal, context)) {
                yield value;
            }
            callback();
        } catch (error) {
            callback();
            throw error;
        }
    });
};

Flux.prototype.limitRate = function limitRate<T>(this: Flux<T>, _highTide: number, _lowTide?: number): Flux<T> {
    return this;
};

Flux.prototype.limitRequest = function limitRequest<T>(this: Flux<T>, n: number): Flux<T> {
    return this.take(n);
};

Flux.prototype.log = function log<T>(this: Flux<T>, category = "Flux"): Flux<T> {
    return this.doOnEach(signal => console.debug(category, signal.type, signal.value ?? signal.error));
};

Flux.prototype.metrics = function metrics<T>(this: Flux<T>): Flux<T> {
    return this;
};

Flux.prototype.name = function name<T>(this: Flux<T>, _name: string): Flux<T> {
    return this;
};

Flux.prototype.onBackpressureBuffer = function onBackpressureBuffer<T>(this: Flux<T>, ..._args: unknown[]): Flux<T> {
    return this;
};

Flux.prototype.onBackpressureDrop = function onBackpressureDrop<T>(
    this: Flux<T>,
    _callback?: (value: T) => void
): Flux<T> {
    return this;
};

Flux.prototype.onBackpressureError = function onBackpressureError<T>(this: Flux<T>): Flux<T> {
    return this;
};

Flux.prototype.onBackpressureLatest = function onBackpressureLatest<T>(this: Flux<T>): Flux<T> {
    return this;
};

Flux.prototype.onErrorComplete = function onErrorComplete<T>(
    this: Flux<T>,
    predicate: (error: unknown) => boolean = () => true
): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        try {
            for await (const value of source.iterate(signal, context)) {
                yield value;
            }
        } catch (error) {
            if (!predicate(error)) {
                throw error;
            }
        }
    });
};

Flux.prototype.onErrorContinue = function onErrorContinue<T>(
    this: Flux<T>,
    callback: (error: unknown, value: T | undefined) => void
): Flux<T> {
    return this.onErrorResume(error => {
        callback(error, undefined);
        return Flux.empty<T>();
    });
};

Flux.prototype.onErrorStop = function onErrorStop<T>(this: Flux<T>): Flux<T> {
    return this;
};

Flux.prototype.onTerminateDetach = function onTerminateDetach<T>(this: Flux<T>): Flux<T> {
    return this;
};

Flux.prototype.publish = function publish<T, R = T>(
    this: Flux<T>,
    transformer?: (source: Flux<T>) => PublisherInput<R>
): Flux<R> {
    return transformer ? Flux.from(transformer(this.share())) : (this.share() as unknown as Flux<R>);
};

Flux.prototype.publishNext = function publishNext<T>(this: Flux<T>) {
    return this.next();
};

Flux.prototype.replay = function replay<T>(this: Flux<T>, ..._args: unknown[]): Flux<T> {
    return this.cache();
};

Flux.prototype.share = function share<T>(this: Flux<T>): Flux<T> {
    return this;
};

Flux.prototype.shareNext = function shareNext<T>(this: Flux<T>) {
    return this.next();
};

Flux.prototype.tag = function tag<T>(this: Flux<T>, _key: string, _value: string): Flux<T> {
    return this;
};

Flux.prototype.tap = function tap<T>(this: Flux<T>, listener: FluxTapListener<T> | (() => FluxTapListener<T>)): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        const actual = typeof listener === "function" ? listener() : listener;
        signal.addEventListener("abort", () => actual.onCancel?.(), {once: true});
        try {
            for await (const value of source.iterate(signal, context)) {
                actual.onNext?.(value);
                actual.onSignal?.(Signal.next(value));
                yield value;
            }
            actual.onComplete?.();
            actual.onSignal?.(Signal.complete());
        } catch (error) {
            actual.onError?.(error);
            actual.onSignal?.(Signal.error(error));
            throw error;
        }
    });
};

Flux.prototype.toStream = function toStream<T>(this: Flux<T>): AsyncIterable<T> {
    return this;
};

/** Returns true when a value can be treated as a duration input. */
function isDuration(value: unknown): value is DurationInput {
    return typeof value === "number" || (typeof value === "object" && value !== null && !isPublisherLike(value));
}

/** Returns true when a value looks like a publisher or iterable source. */
function isPublisherLike(value: object): boolean {
    return (
        typeof (value as { subscribe?: unknown }).subscribe === "function" ||
        typeof (value as { then?: unknown }).then === "function" ||
        typeof (value as { [Symbol.iterator]?: unknown })[Symbol.iterator] === "function" ||
        typeof (value as { [Symbol.asyncIterator]?: unknown })[Symbol.asyncIterator] === "function"
    );
}

/** Consumes a publisher input and ignores all emitted values. */
async function drain(source: PublisherInput<unknown>, signal: AbortSignal, context: Context): Promise<void> {
    for await (const _ of Flux.from(source).iterate(signal, context)) {
        // ignored
    }
}

/** Creates a no-op subscription object for doOnSubscribe callbacks. */
function noopSubscription(): Subscription {
    return {
        /** Ignores request accounting for synthetic subscriptions. */
        request() {
            // no-op
        },
        /** Ignores cancellation for synthetic subscriptions. */
        cancel() {
            // no-op
        }
    };
}
