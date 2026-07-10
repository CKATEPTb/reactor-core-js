/**
 * @packageDocumentation
 * Publisher contract used by Flux, Mono and Reactive Streams adapters.
 */
import type {Subscriber} from "@/subscription/subscriber.js";

/** Reactive Streams publisher that can be subscribed to by a subscriber. */
export interface Publisher<T> {
    /** Subscribes the given subscriber to this publisher. */
    subscribe(subscriber: Subscriber<T>): void;
}
