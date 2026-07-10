/**
 * @packageDocumentation
 * Error types and exception helpers used by the runtime.
 */
/** Converts any thrown value to an Error instance. */
export function asError(error: unknown): Error {
    return error instanceof Error ? error : new Error(String(error));
}

/** Ensures a value is neither null nor undefined. */
export function throwIfNullish<T>(value: T | null | undefined, name: string): T {
    if (value === null || value === undefined) {
        throw new TypeError(`${name} must not be null or undefined`);
    }
    return value;
}
