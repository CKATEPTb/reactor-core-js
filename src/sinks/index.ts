import {Sink} from "@/sinks/Sink";
import OneSink from "@/sinks/OneSink";
import EmptySink from "@/sinks/EmptySink";

export {type Sink, OneSink /*todo*/}
export const Sinks = {
    empty: <T>(): EmptySink<T> => new EmptySink(),
    one: <T>(): OneSink<T> => new OneSink(),
    many: () => ({
        multicast: () => ({
            directAllOrNothing: <T>() => null, // todo
            directBestEffort: <T>() => null, // todo
            onBackpressureBuffer: <T>(bufferSize: number = 256, autoCancel: boolean = true) => null // todo
        }),
        unicast: () => ({
            onBackpressureBuffer: <T>() => null, // todo
            onBackpressureError: <T>() => null // todo
        }),
        replay: () => ({
            all: <T>() => null, // todo
            latest: <T>(limit: number) => null, // todo
            latestOrDefault: <T>(value: T) => null, // todo
            limit: <T>(limit: number) => null // todo
        })
    })
}