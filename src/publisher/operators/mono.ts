/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import {Flux} from "@/publisher/flux.js";
import {Mono} from "@/publisher/mono.js";
import type {Mapper, PublisherInput} from "@/publisher/types.js";

declare module "@/publisher/mono.js" {
    /** Core Mono instance operators added to Mono. */
    interface Mono<T> {
        /** Maps the Mono value when one is present. */
        map<R>(mapper: Mapper<T, R>): Mono<R>;

        /** Maps the Mono value to another publisher and keeps at most one resulting value. */
        flatMap<R>(mapper: (value: T) => PublisherInput<R>): Mono<R>;

        /** Maps the Mono value to a publisher and exposes all resulting values as Flux. */
        flatMapMany<R>(mapper: (value: T) => PublisherInput<R>): Flux<R>;

        /** Keeps the Mono value only when it matches the predicate. */
        filter(predicate: (value: T) => boolean): Mono<T>;

        /** Emits `defaultValue` when this Mono completes empty. */
        defaultIfEmpty(defaultValue: T): Mono<T>;

        /** Switches to `alternate` when this Mono completes empty. */
        switchIfEmpty(alternate: PublisherInput<T>): Mono<T>;

        /** Ignores the value and completes when this Mono completes successfully. */
        then(): Mono<void>;

        /** Ignores the value and emits `value` after successful completion. */
        thenReturn<R>(value: R): Mono<R>;

        /** Combines this Mono value with another publisher's value. */
        zipWith<U, R = readonly [T, U]>(other: PublisherInput<U>, combinator?: (left: T, right: U) => R): Mono<R>;
    }
}

Mono.prototype.map = function map<T, R>(this: Mono<T>, mapper: Mapper<T, R>): Mono<R> {
    return new Mono((signal, context) => (Flux.prototype.map.call(this, mapper) as Flux<R>).iterate(signal, context));
};

Mono.prototype.flatMap = function flatMap<T, R>(
    this: Mono<T>,
    mapper: (value: T) => PublisherInput<R>
): Mono<R> {
    return new Mono((signal, context) => (Flux.prototype.flatMap.call(this, mapper, 1) as Flux<R>).next().iterate(signal, context));
};

Mono.prototype.flatMapMany = function flatMapMany<T, R>(
    this: Mono<T>,
    mapper: (value: T) => PublisherInput<R>
): Flux<R> {
    return Flux.prototype.flatMap.call(this, mapper, 1) as Flux<R>;
};

Mono.prototype.filter = function filter<T>(this: Mono<T>, predicate: (value: T) => boolean): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.filter.call(this, predicate).iterate(signal, context));
};

Mono.prototype.defaultIfEmpty = function defaultIfEmpty<T>(this: Mono<T>, defaultValue: T): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.defaultIfEmpty.call(this, defaultValue).iterate(signal, context));
};

Mono.prototype.switchIfEmpty = function switchIfEmpty<T>(this: Mono<T>, alternate: PublisherInput<T>): Mono<T> {
    return new Mono((signal, context) => Flux.prototype.switchIfEmpty.call(this, alternate).next().iterate(signal, context));
};

Mono.prototype.then = function then<T>(this: Mono<T>): Mono<void> {
    return Flux.prototype.then.call(this);
};

Mono.prototype.thenReturn = function thenReturn<T, R>(this: Mono<T>, value: R): Mono<R> {
    return Flux.prototype.thenReturn.call(this, value) as Mono<R>;
};

Mono.prototype.zipWith = function zipWith<T, U, R = readonly [T, U]>(
    this: Mono<T>,
    other: PublisherInput<U>,
    combinator?: (left: T, right: U) => R
): Mono<R> {
    return Mono.zip<[T, U], R>([this, other], values =>
        combinator ? combinator(values[0], values[1]) : ([values[0], values[1]] as const as R)
    );
};
