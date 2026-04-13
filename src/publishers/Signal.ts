/**
 * Represents a reactive signal — one of the three events that can be emitted
 * by a {@link Publisher}: a value (`next`), a terminal error (`error`), or
 * a normal completion (`complete`).
 *
 * `Signal` is the type used by {@link Flux#materialize} /
 * {@link Flux#dematerialize} to pass reactive events as plain data values.
 *
 * @typeParam T - The type of the value carried by a `next` signal.
 */
export type Signal<T> =
    | { readonly kind: 'next'; readonly value: T }
    | { readonly kind: 'error'; readonly error: Error }
    | { readonly kind: 'complete' };

/**
 * Namespace of factory helpers for creating {@link Signal} instances.
 */
export const Signal = {
    /**
     * Creates a `next` signal carrying `value`.
     * @param value - The item to wrap.
     */
    next<T>(value: T): Signal<T> { return { kind: 'next', value }; },

    /**
     * Creates an `error` signal carrying `error`.
     * @param error - The error to wrap.
     */
    error<T>(error: Error): Signal<T> { return { kind: 'error', error }; },

    /**
     * Creates a `complete` signal.
     */
    complete<T>(): Signal<T> { return { kind: 'complete' }; },
};
