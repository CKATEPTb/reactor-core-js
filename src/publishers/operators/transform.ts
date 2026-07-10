/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import type {ContextView} from "@/context/index.js";
import {Signal} from "@/signal/index.js";
import {Flux} from "@/publishers/flux.js";
import type {Mapper, PublisherInput, SynchronousSink} from "@/publishers/types.js";

declare module "@/publishers/flux.js" {
    /** Transforming and materialization operators added to Flux. */
    interface Flux<T> {
        /** Reactor map operator. */
        map<R>(mapper: Mapper<T, R>): Flux<R>;

        /** Reactor cast operator. */
        cast<R>(): Flux<R>;

        /** Reactor filter operator. */
        filter(predicate: (value: T) => boolean): Flux<T>;

        /** Reactor handle operator. */
        handle<R>(handler: (value: T, sink: SynchronousSink<R>) => void): Flux<R>;

        /** Reactor materialize operator. */
        materialize(): Flux<Signal<T>>;

        /** Reactor dematerialize operator. */
        dematerialize<R>(this: Flux<Signal<R>>): Flux<R>;

        /** Reactor transform operator. */
        transform<R>(transformer: (source: Flux<T>) => PublisherInput<R>): Flux<R>;

        /** Reactor transformDeferred operator. */
        transformDeferred<R>(transformer: (source: Flux<T>) => PublisherInput<R>): Flux<R>;

        /** Reactor transformDeferredContextual operator. */
        transformDeferredContextual<R>(transformer: (source: Flux<T>, context: ContextView) => PublisherInput<R>): Flux<R>;
    }
}

Flux.prototype.map = function map<T, R>(this: Flux<T>, mapper: Mapper<T, R>): Flux<R> {
    const source = this;
    return new Flux(async function* (signal, context) {
        for await (const value of source.iterate(signal, context)) {
            yield mapper(value);
        }
    });
};

Flux.prototype.cast = function cast<T, R>(this: Flux<T>): Flux<R> {
    return this as unknown as Flux<R>;
};

Flux.prototype.filter = function filter<T>(this: Flux<T>, predicate: (value: T) => boolean): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        for await (const value of source.iterate(signal, context)) {
            if (predicate(value)) {
                yield value;
            }
        }
    });
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
