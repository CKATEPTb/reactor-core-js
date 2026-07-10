/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import {Flux} from "@/publisher/flux.js";
import {Mono} from "@/publisher/mono.js";
import {isAsyncIterable} from "@/internal/iterable.js";
import type {PublisherInput} from "@/publisher/types.js";

declare module "@/publisher/flux.js" {
    /** Terminal continuation and zip operators added to Flux. */
    interface Flux<T> {
        /** Ignores all values and completes when the source completes successfully. */
        then(): Mono<void>;

        /** Waits for this source to complete and then emits values from `other`. */
        thenMany<R>(other: PublisherInput<R>): Flux<R>;

        /** Ignores all values and emits `value` after successful completion. */
        thenReturn<R>(value: R): Mono<R>;

        /** Zips this Flux with another publisher and combines paired values. */
        zipWith<U, R = readonly [T, U]>(other: PublisherInput<U>, combinator?: (left: T, right: U) => R): Flux<R>;
    }
}

Flux.prototype.then = function then<T>(this: Flux<T>): Mono<void> {
    const source = this;
    return new Mono((signal, context) => {
        const values = source.iterate(signal, context);
        return isAsyncIterable<T>(values) ? drainThenEmitAsync(values, undefined) : drainThenEmitSync(values, undefined);
    });
};

Flux.prototype.thenMany = function thenMany<T, R>(this: Flux<T>, other: PublisherInput<R>): Flux<R> {
    const source = this;
    return new Flux(async function* (signal, context) {
        for await (const _ of source.iterate(signal, context)) {
            // ignore values
        }
        for await (const value of Flux.from(other).iterate(signal, context)) {
            yield value;
        }
    });
};

Flux.prototype.thenReturn = function thenReturn<T, R>(this: Flux<T>, value: R): Mono<R> {
    const source = this;
    return new Mono((signal, context) => {
        const values = source.iterate(signal, context);
        return isAsyncIterable<T>(values) ? drainThenEmitAsync(values, value) : drainThenEmitSync(values, value);
    });
};

/** Drains a synchronous source and then emits one completion value. */
function drainThenEmitSync<T, R>(values: Iterable<T>, value: R): Iterable<R> {
    return (function* () {
        for (const _ of values) {
            // ignore values
        }
        yield value;
    })();
}

/** Drains an asynchronous source and then emits one completion value. */
async function* drainThenEmitAsync<T, R>(values: AsyncIterable<T>, value: R): AsyncIterable<R> {
    for await (const _ of values) {
        // ignore values
    }
    yield value;
}

Flux.prototype.zipWith = function zipWith<T, U, R = readonly [T, U]>(
    this: Flux<T>,
    other: PublisherInput<U>,
    combinator?: (left: T, right: U) => R
): Flux<R> {
    return Flux.zip<[T, U], R>([this, other], values =>
        combinator ? combinator(values[0], values[1]) : ([values[0], values[1]] as const as R)
    );
};
