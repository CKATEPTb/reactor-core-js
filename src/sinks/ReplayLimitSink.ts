import {ReplaySink} from "@/sinks/ReplaySink";

export class ReplayLimitSink<T> extends ReplaySink<T> {
    public constructor(private readonly limit: number) {
        super()
        if (limit < 1) throw new Error("LimitSink: limit must be > 0")
    }

    protected override store(emit: "next" | "error" | "complete", data?: Error | T) {
        if (this.buffer.length <= this.limit) super.store(emit, data)
    }
}