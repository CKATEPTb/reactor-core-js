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

/**
 * A {@link Sink} that is also a cold {@link Publisher}.
 * Callers can push values via the `Sink` interface and subscribe via the `Publisher` interface.
 */
export type SinkPublisher<T> = Sink<T> & Publisher<T>;

export {type Sink};

/**
 * Factory namespace for creating hot or replayable sink/publisher pairs.
 *
 * Each factory returns a {@link SinkPublisher} that implements both {@link Sink} (for pushing values)
 * and {@link Publisher} (for subscribing).
 *
 * @example
 * ```typescript
 * const sink = Sinks.many().multicast().directBestEffort<number>();
 * const flux = Flux.from(sink);
 * flux.subscribe(v => console.log(v));
 * sink.next(1);
 * sink.next(2);
 * sink.complete();
 * ```
 */
export const Sinks = {
    /**
     * A sink that immediately completes any subscriber without emitting values.
     * Equivalent to `Flux.empty()` but hot/shared.
     */
    empty: <T>(): SinkPublisher<T> => new EmptySink(),

    /**
     * A sink for exactly one value. Replays that value (and subsequent completion) to all subscribers.
     * Use for single-result async operations.
     */
    one: <T>(): SinkPublisher<T> => new OneSink(),

    /** Creates a multi-subscriber sink factory. */
    many: () => ({
        /** Creates multicast (fan-out) sinks that deliver to all current subscribers. */
        multicast: () => ({
            /**
             * Delivers each item to all current subscribers simultaneously.
             * If any subscriber cannot accept the item (due to backpressure), **none** of them receive it.
             */
            directAllOrNothing: <T>(): SinkPublisher<T> =>
                new MulticastDirectAllOrNothingSink(),

            /**
             * Delivers each item to all current subscribers on a best-effort basis.
             * Subscribers that are not ready (backpressure) are skipped for that item.
             */
            directBestEffort: <T>(): SinkPublisher<T> =>
                new MulticastDirectBestEffortSink(),

            /**
             * Buffers items and delivers them to all subscribers respecting backpressure.
             *
             * @param bufferSize - Maximum number of items to buffer (default: `256`).
             * @param autoCancel - If `true`, cancels the sink when all subscribers unsubscribe (default: `true`).
             */
            onBackpressureBuffer: <T>(bufferSize: number = 256, autoCancel: boolean = true): SinkPublisher<T> =>
                new MulticastOnBackpressureBufferSink(bufferSize, autoCancel),
        }),

        /** Creates unicast (single-subscriber) sinks. */
        unicast: () => ({
            /**
             * Unicast sink that buffers items until the single subscriber requests them.
             * A second `subscribe()` call throws.
             */
            onBackpressureBuffer: <T>(): SinkPublisher<T> =>
                new UnicastOnBackpressureBufferSink(),

            /**
             * Unicast sink that errors if an item is pushed when the subscriber has no outstanding demand.
             * A second `subscribe()` call throws.
             */
            onBackpressureError: <T>(): SinkPublisher<T> =>
                new UnicastOnBackpressureErrorSink(),
        }),

        /** Creates replay sinks that buffer past items for late subscribers. */
        replay: () => ({
            /**
             * Replays **all** previously emitted items to each new subscriber.
             * Items are accumulated indefinitely.
             */
            all: <T>(): SinkPublisher<T> =>
                new ReplayAllSink(),

            /**
             * Replays the most recent `limit` items to each new subscriber.
             *
             * @param limit - Maximum number of items to replay.
             */
            latest: <T>(limit: number): SinkPublisher<T> =>
                new ReplayLatestSink(limit),

            /**
             * Replays the most recent item (or `value` if nothing has been emitted yet) to each new subscriber.
             *
             * @param value - Default value replayed until the first `next()` call.
             */
            latestOrDefault: <T>(value: T): SinkPublisher<T> =>
                new ReplayLatestOrDefaultSink(value),

            /**
             * Alias for {@link latest}. Replays the most recent `limit` items.
             *
             * @param limit - Maximum number of items to replay.
             */
            limit: <T>(limit: number): SinkPublisher<T> =>
                new ReplayLatestSink(limit),
        }),
    }),
}
