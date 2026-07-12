/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import type {ContextView} from "@/context/context-view.js";
import {filterIterable, mapIterable} from "@/internal/iterable-transform.js";
import {Flux} from "@/publisher/flux.js";
import {liftFilter, liftOneToOne} from "@/publisher/operators/lift.js";
import type {Mapper, PublisherInput, SynchronousSink} from "@/publisher/types.js";
import {Signal} from "@/signal/signal.js";

declare module "@/publisher/flux.js" {
    /** Transforming and materialization operators added to Flux. */
    interface Flux<T> {
        /** Maps each source value with the provided function. */
        map<R>(mapper: Mapper<T, R>): Flux<R>;

        /** Treats this Flux as a Flux of another TypeScript type. */
        cast<R>(): Flux<R>;

        /** Keeps only source values accepted by the predicate. */
        filter(predicate: (value: T) => boolean): Flux<T>;

        /** Handles each value with a synchronous sink that may emit, complete, or fail. */
        handle<R>(handler: (value: T, sink: SynchronousSink<R>) => void): Flux<R>;

        /** Converts values and terminal events into `Signal` values. */
        materialize(): Flux<Signal<T>>;

        /** Converts `Signal` values back into normal values and terminal events. */
        dematerialize<R>(this: Flux<Signal<R>>): Flux<R>;

        /** Applies a transformer to this Flux immediately and adapts its result. */
        transform<R>(transformer: (source: Flux<T>) => PublisherInput<R>): Flux<R>;

        /** Applies a transformer separately for each subscription. */
        transformDeferred<R>(transformer: (source: Flux<T>) => PublisherInput<R>): Flux<R>;

        /** Applies a transformer per subscription with access to the subscriber context. */
        transformDeferredContextual<R>(transformer: (source: Flux<T>, context: ContextView) => PublisherInput<R>): Flux<R>;
    }
}

Flux.prototype.map = function map<T, R>(this: Flux<T>, mapper: Mapper<T, R>): Flux<R> {
    const source = this;
    return liftOneToOne(
        source,
        (signal, context) => mapIterable(source.iterate(signal, context), mapper),
        () => ({onNext: mapper})
    );
};

Flux.prototype.cast = function cast<T, R>(this: Flux<T>): Flux<R> {
    return this as unknown as Flux<R>;
};

Flux.prototype.filter = function filter<T>(this: Flux<T>, predicate: (value: T) => boolean): Flux<T> {
    const source = this;
    return liftFilter(
        source,
        (signal, context) => filterIterable(source.iterate(signal, context), predicate),
        predicate
    );
};

Flux.prototype.handle = function handle<T, R>(
    this: Flux<T>,
    handler: (value: T, sink: SynchronousSink<R>) => void
): Flux<R> {
    const source = this;
    return new Flux(async function* (signal, context) {
        for await (const value of source.iterate(signal, context)) {
            let done = false;
            let output: R | undefined;
            const sink: SynchronousSink<R> = {
                /** Emits one mapped value for the current source value. */
                next(nextValue) {
                    if (done) {
                        throw new Error("SynchronousSink allows only one signal per source value");
                    }
                    output = nextValue;
                    done = true;
                },
                /** Suppresses emission for the current source value. */
                complete() {
                    done = true;
                },
                /** Fails the sequence with the provided error. */
                error(error) {
                    throw error;
                }
            };
            handler(value, sink);
            if (done && output !== undefined) {
                yield output;
            }
        }
    });
};

Flux.prototype.materialize = function materialize<T>(this: Flux<T>): Flux<Signal<T>> {
    const source = this;
    return new Flux(async function* (signal, context) {
        try {
            for await (const value of source.iterate(signal, context)) {
                yield Signal.next(value);
            }
            yield Signal.complete();
        } catch (error) {
            yield Signal.error(error);
        }
    });
};

Flux.prototype.dematerialize = function dematerialize<R>(this: Flux<Signal<R>>): Flux<R> {
    const source = this;
    return new Flux(async function* (signal, context) {
        for await (const signalValue of source.iterate(signal, context)) {
            if (signalValue.isOnNext()) {
                yield signalValue.get() as R;
            } else if (signalValue.isOnError()) {
                throw signalValue.getThrowable();
            } else if (signalValue.isOnComplete()) {
                return;
            }
        }
    });
};

Flux.prototype.transform = function transform<T, R>(
    this: Flux<T>,
    transformer: (source: Flux<T>) => PublisherInput<R>
): Flux<R> {
    return Flux.from(transformer(this));
};

Flux.prototype.transformDeferred = function transformDeferred<T, R>(
    this: Flux<T>,
    transformer: (source: Flux<T>) => PublisherInput<R>
): Flux<R> {
    const source = this;
    return Flux.defer(() => transformer(source));
};

Flux.prototype.transformDeferredContextual = function transformDeferredContextual<T, R>(
    this: Flux<T>,
    transformer: (source: Flux<T>, context: ContextView) => PublisherInput<R>
): Flux<R> {
    const source = this;
    return new Flux((signal, context) => Flux.from(transformer(source, context.readOnly())).iterate(signal, context));
};
