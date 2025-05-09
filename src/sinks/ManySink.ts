import {BackpressureSink} from "@/sinks/BackpressureSink";

/**
 * A sink that supports multiple subscribers and handles backpressure.
 * Extends `BackpressureSink` to allow broadcasting emitted values to multiple subscribers.
 *
 * @template T - The type of data being emitted.
 */
export default class ManySink<T> extends BackpressureSink<T> {
    // Inherits all behavior from BackpressureSink without any modifications.
}