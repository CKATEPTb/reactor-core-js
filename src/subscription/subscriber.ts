/**
 * @packageDocumentation
 * Reactive Streams subscriber contract.
 */
import type {Subscription} from "@/subscription/subscription.js";

/** Reactive Streams subscriber receiving subscription, data and terminal signals. */
export interface Subscriber<T> {
    /** Called once with the upstream subscription before any other signal. */
    onSubscribe(subscription: Subscription): void;

    /** Called for each emitted item while demand is available. */
    onNext(value: T): void;

    /** Called once when the sequence fails. */
    onError(error: unknown): void;

    /** Called once when the sequence completes successfully. */
    onComplete(): void;
}
