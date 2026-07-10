/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import {Flux} from "@/publishers/flux.js";
import {Mono} from "@/publishers/mono.js";
import type {PublisherInput} from "@/publishers/types.js";

declare module "@/publishers/flux.js" {
    /** Terminal continuation and zip operators added to Flux. */
    interface Flux<T> {
        /** Reactor then operator. */
        then(): Mono<void>;

        /** Reactor thenMany operator. */
        thenMany<R>(other: PublisherInput<R>): Flux<R>;

        /** Reactor thenReturn operator. */
        thenReturn<R>(value: R): Mono<R>;

        /** Reactor zipWith operator. */
        zipWith<U, R = readonly [T, U]>(other: PublisherInput<U>, combinator?: (left: T, right: U) => R): Flux<R>;
    }
}

Flux.prototype.then = function then<T>(this: Flux<T>): Mono<void> {
    const source = this;
    return new Mono(async function* (signal, context) {
        for await (const _ of source.iterate(signal, context)) {
            // ignore values
        }
        yield undefined;
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
    return this.then().map(() => value);
};

Flux.prototype.zipWith = function zipWith<T, U, R = readonly [T, U]>(
    this: Flux<T>,
    other: PublisherInput<U>,
    combinator?: (left: T, right: U) => R
): Flux<R> {
    return Flux.zip<[T, U], R>([this, other], values =>
        combinator ? combinator(values[0], values[1]) : ([values[0], values[1]] as const as R)
    );
};
