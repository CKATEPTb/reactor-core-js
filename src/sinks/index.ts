import {Sink} from "@/sinks/Sink";
import OneSink from "@/sinks/OneSink";
import EmptySink from "@/sinks/EmptySink";
import UnicastOnBackpressureBufferSink from "@/sinks/UnicastOnBackpressureBufferSink";
import UnicastOnBackpressureErrorSink from "@/sinks/UnicastOnBackpressureErrorSink";
import MulticastDirectAllOrNothingSink from "@/sinks/MulticastDirectAllOrNothingSink";
import MulticastDirectBestEffortSink from "@/sinks/MulticastDirectBestEffortSink";
import MulticastOnBackpressureBufferSink from "@/sinks/MulticastOnBackpressureBufferSink";
import ReplayAllSink from "@/sinks/ReplayAllSink";
import ReplayLatestSink from "@/sinks/ReplayLatestSink";
import ReplayLatestOrDefaultSink from "@/sinks/ReplayLatestOrDefaultSink";

export {
    type Sink,
    OneSink,
    EmptySink,
    UnicastOnBackpressureBufferSink,
    UnicastOnBackpressureErrorSink,
    MulticastDirectAllOrNothingSink,
    MulticastDirectBestEffortSink,
    MulticastOnBackpressureBufferSink,
    ReplayAllSink,
    ReplayLatestSink,
    ReplayLatestOrDefaultSink,
}

export const Sinks = {
    empty: <T>(): EmptySink<T> => new EmptySink(),
    one: <T>(): OneSink<T> => new OneSink(),
    many: () => ({
        multicast: () => ({
            directAllOrNothing: <T>(): MulticastDirectAllOrNothingSink<T> =>
                new MulticastDirectAllOrNothingSink(),
            directBestEffort: <T>(): MulticastDirectBestEffortSink<T> =>
                new MulticastDirectBestEffortSink(),
            onBackpressureBuffer: <T>(bufferSize: number = 256, autoCancel: boolean = true): MulticastOnBackpressureBufferSink<T> =>
                new MulticastOnBackpressureBufferSink(bufferSize, autoCancel),
        }),
        unicast: () => ({
            onBackpressureBuffer: <T>(): UnicastOnBackpressureBufferSink<T> =>
                new UnicastOnBackpressureBufferSink(),
            onBackpressureError: <T>(): UnicastOnBackpressureErrorSink<T> =>
                new UnicastOnBackpressureErrorSink(),
        }),
        replay: () => ({
            all: <T>(): ReplayAllSink<T> =>
                new ReplayAllSink(),
            latest: <T>(limit: number): ReplayLatestSink<T> =>
                new ReplayLatestSink(limit),
            latestOrDefault: <T>(value: T): ReplayLatestOrDefaultSink<T> =>
                new ReplayLatestOrDefaultSink(value),
            limit: <T>(limit: number): ReplayLatestSink<T> =>
                new ReplayLatestSink(limit),
        }),
    }),
}
