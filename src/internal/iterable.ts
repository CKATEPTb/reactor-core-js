/**
 * @packageDocumentation
 * Internal async iteration and cancellation utilities.
 */
/** Source that can be consumed through either sync or async iteration. */
export type AnyIterable<T> = Iterable<T> | AsyncIterable<T>;

/** Returns true when `value` implements the async iterable protocol. */
export function isAsyncIterable<T>(value: unknown): value is AsyncIterable<T> {
    return typeof (value as { [Symbol.asyncIterator]?: unknown })?.[Symbol.asyncIterator] === "function";
}

/** Returns true when `value` implements the synchronous iterable protocol. */
export function isIterable<T>(value: unknown): value is Iterable<T> {
    return typeof (value as { [Symbol.iterator]?: unknown })?.[Symbol.iterator] === "function";
}

/** Converts a synchronous or asynchronous iterable into an async iterator. */
export function toAsyncIterator<T>(source: AnyIterable<T>): AsyncIterator<T> {
    if (isAsyncIterable<T>(source)) {
        return source[Symbol.asyncIterator]();
    }
    const iterator = source[Symbol.iterator]();
    return {
        /** Returns the next synchronous iterator value as a promise. */
        next() {
            return Promise.resolve(iterator.next());
        },
        /** Delegates return to the synchronous iterator when available. */
        return(value?: unknown) {
            const returnFn = iterator.return;
            if (returnFn) {
                return Promise.resolve(returnFn.call(iterator, value));
            }
            return Promise.resolve({done: true, value: value as T});
        },
        /** Delegates throw to the synchronous iterator when available. */
        throw(error?: unknown) {
            const throwFn = iterator.throw;
            if (throwFn) {
                return Promise.resolve(throwFn.call(iterator, error));
            }
            return Promise.reject(error);
        }
    };
}
