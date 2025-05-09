import {Sink} from "@/sinks/Sink";
import OneSink from "@/sinks/OneSink";
import ManySink from "@/sinks/ManySink";
import {ReplayAllSink} from "@/sinks/ReplayAllSink";
import {ReplayLatestSink} from "@/sinks/ReplayLatestSink";
import {ReplayLimitSink} from "@/sinks/ReplayLimitSink";

export {type Sink, OneSink, ManySink, ReplayAllSink, ReplayLatestSink, ReplayLimitSink}

export const Sinks = {
    one: () => new OneSink(),
    many: () => ({
        multicast: () => new ManySink(),
        replay: () => ({
            all: () => new ReplayAllSink(),
            latest: (limit: number) => new ReplayLatestSink(limit),
            limit: (limit: number) => new ReplayLimitSink(limit)
        })
    })
}