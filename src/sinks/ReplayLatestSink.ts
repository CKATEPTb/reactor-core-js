import {ReplaySink} from "@/sinks/ReplaySink";

export class ReplayLatestSink<T> extends ReplaySink<T> {
    public constructor(private readonly limit: number) {
        super()
        if (limit < 1) throw new Error("LatestSink: limit must be > 0")
    }

    protected override store(emit: "next" | "error" | "complete", data?: Error | T) {
        super.store(emit, data)
        if (this.buffer.length > this.limit) this.buffer.shift()
    }
}