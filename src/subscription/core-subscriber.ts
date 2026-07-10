/**
 * @packageDocumentation
 * Context-aware subscriber contract.
 */
import type {Context} from "@/context/context.js";
import type {Subscriber} from "@/subscription/subscriber.js";

/** Subscriber variant that can expose a Reactor context to operators. */
export interface CoreSubscriber<T> extends Subscriber<T> {
    /** Returns the subscriber context used for contextual operators. */
    currentContext?(): Context;
}
