/**
 * @packageDocumentation
 * Error types and exception helpers used by the runtime.
 */
import {CompositeError} from "@/errors/classes.js";
import {asError} from "@/errors/helpers.js";

/** Error helper facade mirroring Reactor-style exception helpers. */
export const Exceptions = {
    /** Throws the provided value after converting it to an Error when necessary. */
    propagate(error: unknown): never {
        throw asError(error);
    },

    /** Creates a composite error from several causes. */
    multiple(errors: readonly unknown[]): CompositeError {
        return new CompositeError(errors);
    },

    /** Returns an Error cause when present, otherwise the original value. */
    unwrap(error: unknown): unknown {
        return error instanceof Error && "cause" in error && error.cause !== undefined ? error.cause : error;
    }
};
