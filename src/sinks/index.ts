import {Publisher} from "@/publishers";
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

/** A sink that is also a cold Publisher — callers can both push values and subscribe to it. */
export type SinkPublisher<T> = Sink<T> & Publisher<T>;

export {type Sink};

export const Sinks = {
    empty: <T>(): SinkPublisher<T> => new EmptySink(),
    one: <T>(): SinkPublisher<T> => new OneSink(),
    many: () => ({
        multicast: () => ({
            directAllOrNothing: <T>(): SinkPublisher<T> =>
                new MulticastDirectAllOrNothingSink(),
            directBestEffort: <T>(): SinkPublisher<T> =>
                new MulticastDirectBestEffortSink(),
            onBackpressureBuffer: <T>(bufferSize: number = 256, autoCancel: boolean = true): SinkPublisher<T> =>
                new MulticastOnBackpressureBufferSink(bufferSize, autoCancel),
        }),
        unicast: () => ({
            onBackpressureBuffer: <T>(): SinkPublisher<T> =>
                new UnicastOnBackpressureBufferSink(),
            onBackpressureError: <T>(): SinkPublisher<T> =>
                new UnicastOnBackpressureErrorSink(),
        }),
        replay: () => ({
            all: <T>(): SinkPublisher<T> =>
                new ReplayAllSink(),
            latest: <T>(limit: number): SinkPublisher<T> =>
                new ReplayLatestSink(limit),
            latestOrDefault: <T>(value: T): SinkPublisher<T> =>
                new ReplayLatestOrDefaultSink(value),
            limit: <T>(limit: number): SinkPublisher<T> =>
                new ReplayLatestSink(limit),
        }),
    }),
}
