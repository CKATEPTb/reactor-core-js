/**
 * @packageDocumentation
 * Reactive Streams core contracts and disposable helpers.
 */
import {BooleanDisposable} from "@/core/boolean-disposable.js";
import {Disposables} from "@/core/disposables.js";
import type {Disposable, Teardown} from "@/core/types.js";

/** Converts a teardown representation into a disposable. */
export function toDisposable(teardown: Teardown): Disposable {
    if (!teardown) {
        return Disposables.disposed();
    }
    if (typeof teardown === "function") {
        return new BooleanDisposable(teardown);
    }
    return teardown;
}
