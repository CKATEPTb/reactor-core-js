/**
 * @packageDocumentation
 * Internal browser task scheduling utilities.
 */

/** Promise reused when a browser does not expose `queueMicrotask`. */
const resolvedMicrotask = Promise.resolve();

/** Schedules a task in the browser microtask queue with a small fallback. */
export function scheduleMicrotask(task: () => void): void {
    if (typeof queueMicrotask === "function") {
        queueMicrotask(task);
        return;
    }
    void resolvedMicrotask.then(task);
}
