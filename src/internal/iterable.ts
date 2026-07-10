/**
 * @packageDocumentation
 * Internal async iteration and cancellation utilities.
 */
/** Source that can be consumed through either sync or async iteration. */
export type AnyIterable<T> = Iterable<T> | AsyncIterable<T>;

/** Shared completed iterator result reused by internal iterators. */
const DONE_RESULT: IteratorReturnResult<never> = Object.freeze({done: true, value: undefined as never});

/** Shared resolved promise for completed iterator results. */
const DONE_PROMISE = Promise.resolve(DONE_RESULT);

/** Shared empty iterator used by stateless empty publishers. */
const EMPTY_ITERATOR: Iterator<never> & Iterable<never> = Object.freeze({
    /** Returns the shared completed iterator result. */
    next: doneResult,
    /** Returns this stateless iterator. */
    [Symbol.iterator]() {
        return this;
    }
});

/** Shared empty iterable reused by stateless empty publishers. */
export const EMPTY_ITERABLE: Iterable<never> = EMPTY_ITERATOR;

/** Returns true when `value` implements the async iterable protocol. */
export function isAsyncIterable<T>(value: unknown): value is AsyncIterable<T> {
    return typeof (value as { [Symbol.asyncIterator]?: unknown })?.[Symbol.asyncIterator] === "function";
}

/** Returns true when `value` implements the synchronous iterable protocol. */
export function isIterable<T>(value: unknown): value is Iterable<T> {
    return typeof (value as { [Symbol.iterator]?: unknown })?.[Symbol.iterator] === "function";
}

/** Returns a reusable completed iterator result when no return value is needed. */
export function doneResult<T>(): IteratorResult<T> {
    return DONE_RESULT as IteratorResult<T>;
}

/** Returns a reusable resolved completed iterator result promise. */
export function resolvedDoneResult<T>(): Promise<IteratorResult<T>> {
    return DONE_PROMISE as Promise<IteratorResult<T>>;
}

/** Returns an async iterable that stays pending until the signal is aborted. */
export function neverIterable(signal: AbortSignal): AsyncIterable<never> {
    return (async function* () {
        await new Promise<void>((_resolve, reject) => {
            if (signal.aborted) {
                reject(signal.reason);
                return;
            }
            signal.addEventListener("abort", () => reject(signal.reason), {once: true});
        });
    })();
}

/** Closes an async iterator and ignores cleanup failures. */
export async function closeAsyncIterator<T>(iterator: AsyncIterator<T>): Promise<void> {
    try {
        await iterator.return?.();
    } catch {
        // best-effort cleanup
    }
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
            return value === undefined ? resolvedDoneResult<T>() : Promise.resolve({done: true, value: value as T});
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
