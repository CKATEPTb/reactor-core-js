/**
 * @packageDocumentation
 * Error types and exception helpers used by the runtime.
 */
/** Base error used by Reactor runtime helpers. */
export class ReactorError extends Error {
    /** Creates an error with the provided message. */
    public constructor(message: string) {
        super(message);
        this.name = new.target.name;
    }
}

/** Error raised when a value was required but the source was empty. */
export class NoSuchElementError extends ReactorError {
    /** Creates an empty-source error. */
    public constructor(message = "Source was empty") {
        super(message);
    }
}

/** Error raised when an index is outside the valid range. */
export class IndexOutOfBoundsError extends ReactorError {
    /** Creates an index bounds error. */
    public constructor(message: string) {
        super(message);
    }
}

/** Error raised when a timeout expires before a signal is observed. */
export class TimeoutError extends ReactorError {
    /** Creates a timeout error. */
    public constructor(message = "Did not observe any item or terminal signal within timeout") {
        super(message);
    }
}

/** Error used internally when an abort signal cancels a sequence. */
export class CancelledError extends ReactorError {
    /** Creates a cancellation error. */
    public constructor(message = "Subscription has been cancelled") {
        super(message);
    }
}

/** Error that groups several failures. */
export class CompositeError extends ReactorError {
    /** Causes that were grouped into this composite error. */
    readonly errors: readonly unknown[];

    /** Creates a composite error from multiple causes. */
    public constructor(errors: readonly unknown[], message = "Multiple errors occurred") {
        super(message);
        this.errors = [...errors];
    }
}
