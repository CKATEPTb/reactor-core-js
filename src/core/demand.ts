/**
 * @packageDocumentation
 * Reactive Streams core contracts and disposable helpers.
 */
/** Demand sentinel used when a subscriber can consume the whole source without batching. */
export const UNBOUNDED_DEMAND = Number.POSITIVE_INFINITY;

/** Validates and normalizes a Reactive Streams request amount. */
export function normalizeRequest(n: number): number {
    if (n === UNBOUNDED_DEMAND) {
        return UNBOUNDED_DEMAND;
    }
    if (!Number.isFinite(n) || !Number.isInteger(n) || n <= 0) {
        throw new RangeError("request amount must be a strictly positive integer");
    }
    return n;
}

/** Adds demand values with saturation at positive infinity. */
export function addCap(a: number, b: number): number {
    if (a === UNBOUNDED_DEMAND || b === UNBOUNDED_DEMAND) {
        return UNBOUNDED_DEMAND;
    }
    const next = a + b;
    return next >= Number.MAX_SAFE_INTEGER ? UNBOUNDED_DEMAND : next;
}

/** Multiplies demand values with saturation at positive infinity. */
export function multiplyCap(a: number, b: number): number {
    if (a === UNBOUNDED_DEMAND || b === UNBOUNDED_DEMAND) {
        return UNBOUNDED_DEMAND;
    }
    const next = a * b;
    return next >= Number.MAX_SAFE_INTEGER ? UNBOUNDED_DEMAND : next;
}
