/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import {Flux} from "@/publisher/flux.js";
import {liftOneToOne} from "@/publisher/operators/lift.js";

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
    return liftOneToOne(
        source,
        async function* (signal, context) {
            for await (const value of source.iterate(signal, context)) {
                callback(value);
                yield value;
            }
        },
        () => ({
            /** Runs the side effect and retains the original value. */
            onNext(value) {
                callback(value);
                return value;
            }
        })
    );
};

Flux.prototype.doOnError = function doOnError<T>(this: Flux<T>, callback: (error: unknown) => void): Flux<T> {
    const source = this;
    return liftOneToOne(
        source,
        async function* (signal, context) {
            try {
                for await (const value of source.iterate(signal, context)) {
                    yield value;
                }
            } catch (error) {
                callback(error);
                throw error;
            }
        },
        () => ({
            onNext: passThrough,
            onError: callback
        })
    );
};

Flux.prototype.doOnComplete = function doOnComplete<T>(this: Flux<T>, callback: () => void): Flux<T> {
    const source = this;
    return liftOneToOne(
        source,
        async function* (signal, context) {
            for await (const value of source.iterate(signal, context)) {
                yield value;
            }
            callback();
        },
        () => ({
            onNext: passThrough,
            onComplete: callback
        })
    );
};

Flux.prototype.doFinally = function doFinally<T>(
    this: Flux<T>,
    callback: (signal: "complete" | "error" | "cancel") => void
): Flux<T> {
    const source = this;
    return liftOneToOne(
        source,
        async function* (signal, context) {
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
                runFinallyCallback(callback, signal.aborted ? "cancel" : terminal);
            }
        },
        () => ({
            onNext: passThrough,
            onFinally: callback
        })
    );
};

/** Returns an operator value unchanged. */
function passThrough<T>(value: T): T {
    return value;
}

/** Runs a final observer without allowing it to replace sequence termination. */
function runFinallyCallback(
    callback: (signal: "complete" | "error" | "cancel") => void,
    signal: "complete" | "error" | "cancel"
): void {
    try {
        callback(signal);
    } catch {
        // Final observers run after propagation and cannot replace the terminal signal.
    }
}
