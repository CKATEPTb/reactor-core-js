import {ReplaySink} from "@/sinks/ReplaySink";

/**
 * A replay sink that limits the number of stored events.
 * Extends `ReplaySink` to restrict the buffer size to a specified limit.
 *
 * @template T - The type of data being emitted.
 */
export class ReplayLimitSink<T> extends ReplaySink<T> {
    /**
     * Creates a new `ReplayLimitSink` with a specified limit on the number of stored events.
     * Throws an error if the limit is less than 1.
     *
     * @param {number} limit - The maximum number of events to retain in the buffer.
     * @throws {Error} If the limit is less than 1.
     */
    public constructor(private readonly limit: number) {
        super()
        if (limit < 1) throw new Error("LimitSink: limit must be > 0")
    }

    /**
     * Stores an emitted action (next, error, complete) in the buffer.
     * Ensures that the buffer does not exceed the specified limit.
     * @protected
     * @param {'next' | 'error' | 'complete'} emit - The type of emission.
     * @param {T | Error} [data] - The data associated with the emission, if any.
     */
    protected override store(emit: "next" | "error" | "complete", data?: Error | T) {
        if (this.buffer.length < this.limit) super.store(emit, data)
    }
}