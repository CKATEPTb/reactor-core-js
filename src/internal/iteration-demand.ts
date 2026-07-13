/**
 * @packageDocumentation
 * Internal demand metadata propagated through nested iterable operator paths.
 */
import {UNBOUNDED_DEMAND} from "@/core/demand.js";

/** Internal subscriber hook that exposes an already-known upstream demand batch. */
export const ITERATION_DEMAND_HINT = Symbol("iteration-demand-hint");

/** Subscriber shape that can propagate a demand hint before source assembly. */
export interface IterationDemandAware {
    /** Returns the demand batch that this subscriber will request upstream. */
    [ITERATION_DEMAND_HINT]?(): number | undefined;
}

/** Signals whose enclosing subscriber requested unbounded demand. */
const UNBOUNDED_SIGNALS = new WeakSet<AbortSignal>();

/** Marks nested iteration as an unbounded terminal drain. */
export function markUnboundedIteration(signal: AbortSignal): void {
    UNBOUNDED_SIGNALS.add(signal);
}

/** Marks a signal when a subscriber already knows it will request unbounded demand. */
export function applyIterationDemandHint(signal: AbortSignal, subscriber: unknown): void {
    const demand = iterationDemandHint(subscriber);
    if (demand === UNBOUNDED_DEMAND) {
        markUnboundedIteration(signal);
    }
}

/** Reads an internal demand hint without exposing it through the public Subscriber contract. */
export function iterationDemandHint(subscriber: unknown): number | undefined {
    return (subscriber as IterationDemandAware | null | undefined)?.[ITERATION_DEMAND_HINT]?.();
}

/** Returns true when nested Publisher bridges may request unbounded demand once. */
export function isUnboundedIteration(signal: AbortSignal): boolean {
    return UNBOUNDED_SIGNALS.has(signal);
}

/** Propagates unbounded iteration metadata to a derived cancellation signal. */
export function inheritIterationDemand(parent: AbortSignal, child: AbortSignal): void {
    if (isUnboundedIteration(parent)) {
        markUnboundedIteration(child);
    }
}
