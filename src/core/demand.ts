/**
 * @packageDocumentation
 * Reactive Streams core contracts and disposable helpers.
 */
/** Validates and normalizes a Reactive Streams request amount. */
export function normalizeRequest(n: number): number {
    if (!Number.isFinite(n)) {
        return Number.POSITIVE_INFINITY;
    }
    if (!Number.isInteger(n) || n <= 0) {
        throw new RangeError("request amount must be a strictly positive integer");
    }
    return n;
}

/** Adds demand values with saturation at positive infinity. */
export function addCap(a: number, b: number): number {
    if (a === Number.POSITIVE_INFINITY || b === Number.POSITIVE_INFINITY) {
        return Number.POSITIVE_INFINITY;
    }
    const next = a + b;
    return next >= Number.MAX_SAFE_INTEGER ? Number.POSITIVE_INFINITY : next;
}
