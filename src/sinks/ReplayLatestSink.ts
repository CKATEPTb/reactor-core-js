import {ReplaySink} from "@/sinks/ReplaySink";

/**
 * A replay sink that only retains the latest emitted events up to a specified limit.
 * Extends `ReplaySink` to store a fixed number of the most recent events.
 *
 * @template T - The type of data being emitted.
 */
export class ReplayLatestSink<T> extends ReplaySink<T> {
    /**
     * Creates a new `ReplayLatestSink` with a specified limit on the number of stored events.
     * Ensures that only the most recent events are kept, discarding older ones.
     *
     * @param {number} limit - The maximum number of recent events to retain.
     * @throws {Error} If the limit is less than 1.
     */
    public constructor(private readonly limit: number) {
        super()
        if (limit < 1) throw new Error("LatestSink: limit must be > 0")
    }

    /**
     * Stores an emitted action (next, error, complete) in the buffer.
     * Keeps only the most recent events, removing older ones when the limit is exceeded.
     *
     * @protected
     * @param {'next' | 'error' | 'complete'} emit - The type of emission.
     * @param {T | Error} [data] - The data associated with the emission, if any.
     */
    protected override store(emit: "next" | "error" | "complete", data?: Error | T) {
        super.store(emit, data)
        if (this.buffer.length > this.limit) this.buffer.shift()
    }
}