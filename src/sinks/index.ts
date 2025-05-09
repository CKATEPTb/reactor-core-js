import {Sink} from "@/sinks/Sink";
import OneSink from "@/sinks/OneSink";
import ManySink from "@/sinks/ManySink";
import {ReplayAllSink} from "@/sinks/ReplayAllSink";
import {ReplayLatestSink} from "@/sinks/ReplayLatestSink";
import {ReplayLimitSink} from "@/sinks/ReplayLimitSink";

export {type Sink, OneSink, ManySink, ReplayAllSink, ReplayLatestSink, ReplayLimitSink}
/**
 * A collection of commonly used sinks for data emission and subscription management.
 * Provides factory functions for creating instances of various sink types.
 */
export const Sinks = {
    /**
     * Creates a new `OneSink` instance.
     * Suitable for single-value emissions with unicast behavior.
     *
     * @template T - The type of data being emitted.
     * @returns {OneSink<T>} An instance of `OneSink`.
     */
    one: <T>(): OneSink<T> => new OneSink(),
    many: () => ({
        /**
         * Creates a new `ManySink` instance for multicast data emission.
         * Allows broadcasting data to multiple subscribers.
         *
         * @template T - The type of data being emitted.
         * @returns {ManySink<T>} An instance of `ManySink`.
         */
        multicast: <T>(): ManySink<T> => new ManySink(),
        replay: () => ({
            /**
             * Creates a `ReplayAllSink` instance that stores all emitted events.
             * Allows replaying the entire event history to new subscribers.
             *
             * @template T - The type of data being emitted.
             * @returns {ReplayAllSink<T>} An instance of `ReplayAllSink`.
             */
            all: <T>(): ReplayAllSink<T> => new ReplayAllSink(),
            /**
             * Creates a `ReplayLatestSink` instance that stores the most recent N events.
             * Allows replaying the latest events to new subscribers.
             *
             * @template T - The type of data being emitted.
             * @param {number} limit - The maximum number of recent events to retain.
             * @returns {ReplayLatestSink<T>} An instance of `ReplayLatestSink`.
             * @throws {Error} If the limit is less than 1.
             */
            latest: <T>(limit: number): ReplayLatestSink<T> => new ReplayLatestSink(limit),
            /**
             * Creates a `ReplayLimitSink` instance that stores up to a specified number of events.
             * Useful when keeping the entire event history is unnecessary.
             *
             * @template T - The type of data being emitted.
             * @param {number} limit - The maximum number of events to retain.
             * @returns {ReplayLimitSink<T>} An instance of `ReplayLimitSink`.
             * @throws {Error} If the limit is less than 1.
             */
            limit: <T>(limit: number): ReplayLimitSink<T> => new ReplayLimitSink(limit)
        })
    })
}