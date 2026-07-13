/**
 * @packageDocumentation
 * Internal iterable transformation helpers.
 */
import {type AnyIterable, isAsyncIterable} from "@/internal/iterable.js";

/** Defines how adjacent selected keys are compared and retained. */
export interface DistinctUntilChangedStrategy<K> {
    /** Returns true when adjacent keys represent the same state. */
    equals(previous: unknown, current: K): boolean;

    /** Captures the state retained for the next comparison. */
    snapshot(value: K): unknown;
}

/** Maps an iterable, preserving synchronous iteration when possible. */
export function mapIterable<T, R>(source: AnyIterable<T>, mapper: (value: T) => R): AnyIterable<R> {
    if (isAsyncIterable<T>(source)) {
        return (async function* () {
            for await (const value of source) {
                yield mapper(value);
            }
        })();
    }
    return (function* () {
        for (const value of source) {
            yield mapper(value);
        }
    })();
}

/** Filters an iterable, preserving synchronous iteration when possible. */
export function filterIterable<T>(source: AnyIterable<T>, predicate: (value: T) => boolean): AnyIterable<T> {
    if (isAsyncIterable<T>(source)) {
        return (async function* () {
            for await (const value of source) {
                if (predicate(value)) {
                    yield value;
                }
            }
        })();
    }
    return (function* () {
        for (const value of source) {
            if (predicate(value)) {
                yield value;
            }
        }
    })();
}

/** Takes values while a predicate returns true, preserving synchronous iteration when possible. */
export function takeWhileIterable<T>(source: AnyIterable<T>, predicate: (value: T) => boolean): AnyIterable<T> {
    if (isAsyncIterable<T>(source)) {
        return (async function* () {
            for await (const value of source) {
                if (!predicate(value)) {
                    return;
                }
                yield value;
            }
        })();
    }
    return (function* () {
        for (const value of source) {
            if (!predicate(value)) {
                return;
            }
            yield value;
        }
    })();
}

/** Takes values until a predicate returns true, preserving synchronous iteration when possible. */
export function takeUntilIterable<T>(source: AnyIterable<T>, predicate: (value: T) => boolean): AnyIterable<T> {
    if (isAsyncIterable<T>(source)) {
        return (async function* () {
            for await (const value of source) {
                yield value;
                if (predicate(value)) {
                    return;
                }
            }
        })();
    }
    return (function* () {
        for (const value of source) {
            yield value;
            if (predicate(value)) {
                return;
            }
        }
    })();
}

/** Skips a fixed number of values, preserving synchronous iteration when possible. */
export function skipIterable<T>(source: AnyIterable<T>, count: number): AnyIterable<T> {
    if (isAsyncIterable<T>(source)) {
        return (async function* () {
            let skipped = 0;
            for await (const value of source) {
                if (skipped < count) {
                    skipped += 1;
                } else {
                    yield value;
                }
            }
        })();
    }
    return (function* () {
        let skipped = 0;
        for (const value of source) {
            if (skipped < count) {
                skipped += 1;
            } else {
                yield value;
            }
        }
    })();
}

/** Skips values while a predicate returns true, preserving synchronous iteration when possible. */
export function skipWhileIterable<T>(source: AnyIterable<T>, predicate: (value: T) => boolean): AnyIterable<T> {
    if (isAsyncIterable<T>(source)) {
        return (async function* () {
            let skipping = true;
            for await (const value of source) {
                if (skipping && predicate(value)) {
                    continue;
                }
                skipping = false;
                yield value;
            }
        })();
    }
    return (function* () {
        let skipping = true;
        for (const value of source) {
            if (skipping && predicate(value)) {
                continue;
            }
            skipping = false;
            yield value;
        }
    })();
}

/** Keeps the first value for every selected key, preserving synchronous iteration when possible. */
export function distinctIterable<T, K>(source: AnyIterable<T>, keySelector: (value: T) => K): AnyIterable<T> {
    if (isAsyncIterable<T>(source)) {
        return (async function* () {
            const seen = new Set<K>();
            for await (const value of source) {
                const key = keySelector(value);
                if (!seen.has(key)) {
                    seen.add(key);
                    yield value;
                }
            }
        })();
    }
    return (function* () {
        const seen = new Set<K>();
        for (const value of source) {
            const key = keySelector(value);
            if (!seen.has(key)) {
                seen.add(key);
                yield value;
            }
        }
    })();
}

/** Keeps values whose selected key differs from the previous key. */
export function distinctUntilChangedIterable<T, K>(
    source: AnyIterable<T>,
    keySelector: (value: T) => K,
    strategy?: DistinctUntilChangedStrategy<K>
): AnyIterable<T> {
    const equals = strategy?.equals ?? Object.is;
    const snapshot = strategy?.snapshot ?? ((value: K): unknown => value);
    if (isAsyncIterable<T>(source)) {
        return (async function* () {
            let hasPrevious = false;
            let previous: unknown;
            for await (const value of source) {
                const key = keySelector(value);
                if (!hasPrevious || !equals(previous, key)) {
                    hasPrevious = true;
                    previous = snapshot(key);
                    yield value;
                }
            }
        })();
    }
    return (function* () {
        let hasPrevious = false;
        let previous: unknown;
        for (const value of source) {
            const key = keySelector(value);
            if (!hasPrevious || !equals(previous, key)) {
                hasPrevious = true;
                previous = snapshot(key);
                yield value;
            }
        }
    })();
}

/** Emits a fallback value when the source is empty. */
export function defaultIfEmptyIterable<T>(source: AnyIterable<T>, defaultValue: T): AnyIterable<T> {
    if (isAsyncIterable<T>(source)) {
        return (async function* () {
            let empty = true;
            for await (const value of source) {
                empty = false;
                yield value;
            }
            if (empty) {
                yield defaultValue;
            }
        })();
    }
    return (function* () {
        let empty = true;
        for (const value of source) {
            empty = false;
            yield value;
        }
        if (empty) {
            yield defaultValue;
        }
    })();
}
