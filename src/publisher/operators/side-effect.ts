/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import {Flux} from "@/publisher/flux.js";

declare module "@/publisher/flux.js" {
    /** Side-effect callback operators added to Flux. */
    interface Flux<T> {
        /** Invokes a callback for each value without changing the sequence. */
        doOnNext(callback: (value: T) => void): Flux<T>;

        /** Invokes a callback when the source fails without recovering the error. */
        doOnError(callback: (error: unknown) => void): Flux<T>;

        /** Invokes a callback after the source completes successfully. */
        doOnComplete(callback: () => void): Flux<T>;

        /** Invokes a callback once after completion, error, or cancellation. */
        doFinally(callback: (signal: "complete" | "error" | "cancel") => void): Flux<T>;
    }
}

Flux.prototype.doOnNext = function doOnNext<T>(this: Flux<T>, callback: (value: T) => void): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        for await (const value of source.iterate(signal, context)) {
            callback(value);
            yield value;
        }
    });
};

Flux.prototype.doOnError = function doOnError<T>(this: Flux<T>, callback: (error: unknown) => void): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        try {
            for await (const value of source.iterate(signal, context)) {
                yield value;
            }
        } catch (error) {
            callback(error);
            throw error;
        }
    });
};

Flux.prototype.doOnComplete = function doOnComplete<T>(this: Flux<T>, callback: () => void): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        for await (const value of source.iterate(signal, context)) {
            yield value;
        }
        callback();
    });
};

Flux.prototype.doFinally = function doFinally<T>(
    this: Flux<T>,
    callback: (signal: "complete" | "error" | "cancel") => void
): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        let terminal: "complete" | "error" | "cancel" = "complete";
        try {
            for await (const value of source.iterate(signal, context)) {
                if (signal.aborted) {
                    terminal = "cancel";
                    return;
                }
                yield value;
            }
        } catch (error) {
            terminal = signal.aborted ? "cancel" : "error";
            throw error;
        } finally {
            callback(signal.aborted ? "cancel" : terminal);
        }
    });
};
