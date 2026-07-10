/**
 * @packageDocumentation
 * Compact subscriber storage shared by sink implementations.
 */
import type {AsyncQueue} from "@/internal/async-queue.js";

/** Compact storage for one or many active sink subscriber queues. */
export type SinkSubscribers<T> = AsyncQueue<T> | Set<AsyncQueue<T>>;

/** Adds a queue without allocating a Set for the common single-subscriber case. */
export function addSinkSubscriber<T>(
    subscribers: SinkSubscribers<T> | undefined,
    queue: AsyncQueue<T>
): SinkSubscribers<T> {
    if (subscribers === undefined) {
        return queue;
    }
    if (subscribers instanceof Set) {
        subscribers.add(queue);
        return subscribers;
    }
    return subscribers === queue ? subscribers : new Set([subscribers, queue]);
}

/** Removes a queue and releases multi-subscriber storage when it becomes empty. */
export function removeSinkSubscriber<T>(
    subscribers: SinkSubscribers<T> | undefined,
    queue: AsyncQueue<T>
): SinkSubscribers<T> | undefined {
    if (subscribers === undefined) {
        return undefined;
    }
    if (subscribers instanceof Set) {
        subscribers.delete(queue);
        return subscribers.size === 0 ? undefined : subscribers;
    }
    return subscribers === queue ? undefined : subscribers;
}

/** Clears multi-subscriber storage before the owner drops its reference. */
export function clearSinkSubscribers<T>(subscribers: SinkSubscribers<T> | undefined): void {
    if (subscribers instanceof Set) {
        subscribers.clear();
    }
}

/** Returns the number of active queues in compact subscriber storage. */
export function sinkSubscriberCount<T>(subscribers: SinkSubscribers<T> | undefined): number {
    if (subscribers === undefined) {
        return 0;
    }
    return subscribers instanceof Set ? subscribers.size : 1;
}
