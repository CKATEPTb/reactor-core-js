/**
 * @packageDocumentation
 * Browser scheduler implementations and contracts.
 */
import {Disposables} from "@/core/disposables.js";
import type {Disposable} from "@/core/types.js";

/** Returns a no-op disposable that is already disposed. */
export function disposed(): Disposable {
    return Disposables.disposed();
}
