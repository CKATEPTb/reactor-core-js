/**
 * @packageDocumentation
 * Browser scheduler implementations and contracts.
 */
import {type Disposable, Disposables} from "@/core/index.js";

/** Returns a no-op disposable that is already disposed. */
export function disposed(): Disposable {
    return Disposables.disposed();
}
