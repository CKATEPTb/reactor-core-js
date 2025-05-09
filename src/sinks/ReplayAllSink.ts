import {ReplaySink} from "@/sinks/ReplaySink";

/**
 * A replay sink that retains all emitted events indefinitely.
 * Extends `ReplaySink` to store every event without any limitation.
 *
 * @template T - The type of data being emitted.
 */
export class ReplayAllSink<T> extends ReplaySink<T> {
    // Inherits all behavior from ReplaySink without any modifications.
}
