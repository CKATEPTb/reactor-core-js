/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import {Flux} from "@/publishers/flux.js";
import type {PublisherInput} from "@/publishers/types.js";

declare module "@/publishers/flux.js" {
    /** Error recovery and retry operators added to Flux. */
    interface Flux<T> {
        /** Reactor onErrorResume operator. */
        onErrorResume<R = T>(fallback: (error: unknown) => PublisherInput<R>): Flux<T | R>;

        /** Reactor onErrorReturn operator. */
        onErrorReturn(fallbackValue: T): Flux<T>;

        /** Reactor onErrorMap operator. */
        onErrorMap(mapper: (error: unknown) => unknown): Flux<T>;

        /** Reactor retry operator. */
        retry(maxRetries?: number): Flux<T>;

        /** Reactor repeat operator. */
        repeat(repeatCount?: number): Flux<T>;
    }
}

Flux.prototype.onErrorResume = function onErrorResume<T, R = T>(
    this: Flux<T>,
    fallback: (error: unknown) => PublisherInput<R>
): Flux<T | R> {
    const source = this;
    return new Flux(async function* (signal, context) {
        try {
            for await (const value of source.iterate(signal, context)) {
                yield value;
            }
        } catch (error) {
            for await (const value of Flux.from(fallback(error)).iterate(signal, context)) {
                yield value;
            }
        }
    });
};

Flux.prototype.onErrorReturn = function onErrorReturn<T>(this: Flux<T>, fallbackValue: T): Flux<T> {
    return this.onErrorResume(() => Flux.just(fallbackValue));
};

Flux.prototype.onErrorMap = function onErrorMap<T>(this: Flux<T>, mapper: (error: unknown) => unknown): Flux<T> {
    return this.onErrorResume(error => Flux.error(mapper(error)));
};

Flux.prototype.retry = function retry<T>(this: Flux<T>, maxRetries = Number.POSITIVE_INFINITY): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        let attempt = 0;
        while (!signal.aborted) {
            try {
                for await (const value of source.iterate(signal, context)) {
                    yield value;
                }
                return;
            } catch (error) {
                if (attempt >= maxRetries) {
                    throw error;
                }
                attempt += 1;
            }
        }
    });
};

Flux.prototype.repeat = function repeat<T>(this: Flux<T>, repeatCount = Number.POSITIVE_INFINITY): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        let repeated = 0;
        do {
            for await (const value of source.iterate(signal, context)) {
                yield value;
            }
            repeated += 1;
        } while (!signal.aborted && repeated <= repeatCount);
    });
};
