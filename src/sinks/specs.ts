/**
 * @packageDocumentation
 * Sink specifications and default browser-oriented sink implementations.
 */
import {DefaultManySink} from "@/sinks/many.js";
import {DefaultOneSink} from "@/sinks/one.js";
import type {
  EmptySink,
  ManySink,
  ManySpec,
  ManyWithUpstreamSpec,
  MulticastSpec,
  OneSink,
  ReplaySpec,
  UnicastSpec
} from "@/sinks/types.js";

/** Shared builders for unicast many-valued sinks. */
const unicastSpec: UnicastSpec = Object.freeze({
    /** Creates a unicast sink with backpressure buffering. */
    onBackpressureBuffer<T>() {
        return new DefaultManySink<T>({mode: "unicast"});
    },

    /** Creates a unicast sink that fails emission when demand cannot be honored. */
    onBackpressureError<T>() {
        return new DefaultManySink<T>({mode: "unicast-error"});
    }
});

/** Shared builders for multicast many-valued sinks. */
const multicastSpec: MulticastSpec = Object.freeze({
    /** Creates a multicast sink that buffers when subscribers are temporarily absent. */
    onBackpressureBuffer<T>() {
        return new DefaultManySink<T>({mode: "multicast-buffer"});
    },

    /** Creates a multicast sink that fails emission when no subscriber can receive it. */
    directAllOrNothing<T>() {
        return new DefaultManySink<T>({mode: "multicast-direct"});
    },

    /** Creates a multicast sink that emits to currently available subscribers. */
    directBestEffort<T>() {
        return new DefaultManySink<T>({mode: "multicast-direct"});
    }
});

/** Shared builders for replaying many-valued sinks. */
const replaySpec: ReplaySpec = Object.freeze({
    /** Creates a replay sink retaining every emitted value. */
    all<T>() {
        return new DefaultManySink<T>({mode: "replay"});
    },

    /** Creates a replay sink retaining at most `historySize` values. */
    limit<T>(historySize: number) {
        if (!Number.isInteger(historySize) || historySize <= 0) {
            throw new RangeError("historySize must be a strictly positive integer");
        }
        return new DefaultManySink<T>({mode: "replay", replayLimit: historySize});
    },

    /** Creates a replay sink retaining only the latest emitted value. */
    latest<T>() {
        return new DefaultManySink<T>({mode: "replay", replayLimit: 1});
    },

    /** Creates a replay sink with an initial default latest value. */
    latestOrDefault<T>(value: T) {
        return new DefaultManySink<T>({mode: "replay", replayLimit: 1, latestDefault: value});
    }
});

/** Shared builder tree for many-valued sink variants. */
const manySpec: ManySpec = Object.freeze({
    /** Returns builders for unicast many-valued sinks. */
    unicast(): UnicastSpec {
        return unicastSpec;
    },

    /** Returns builders for multicast many-valued sinks. */
    multicast(): MulticastSpec {
        return multicastSpec;
    },

    /** Returns builders for replaying many-valued sinks. */
    replay(): ReplaySpec {
        return replaySpec;
    }
});

/** Shared unsafe builders for many-valued sinks that subscribe to upstream publishers. */
const manyWithUpstreamSpec: ManyWithUpstreamSpec = Object.freeze({
    /** Creates a multicast sink that buffers and can subscribe to one upstream publisher. */
    multicastOnBackpressureBuffer<T>() {
        return new DefaultManySink<T>({mode: "multicast-buffer"});
    }
});

/** Builder tree for unsafe sinks that can subscribe to upstream publishers. */
const unsafeSpec: {
    /** Creates an empty sink that can only terminate. */
    empty<T>(): EmptySink<T>;
    /** Creates a one-valued sink that replays its terminal result. */
    one<T>(): OneSink<T>;
    /** Returns builders for many-valued sinks. */
    many(): ManySpec;
    /** Returns builders for many-valued sinks that can subscribe to one upstream publisher. */
    manyWithUpstream(): ManyWithUpstreamSpec;
} = Object.freeze({
    /** Creates an empty sink that can only terminate. */
    empty<T>(): EmptySink<T> {
        return new DefaultOneSink<T>();
    },

    /** Creates a one-valued sink that replays its terminal result. */
    one<T>(): OneSink<T> {
        return new DefaultOneSink<T>();
    },

    /** Returns builders for many-valued sinks. */
    many(): ManySpec {
        return manySpec;
    },

    /** Returns builders for many-valued sinks that can subscribe to one upstream publisher. */
    manyWithUpstream(): ManyWithUpstreamSpec {
        return manyWithUpstreamSpec;
    }
});

/** Public Sinks factory facade. */
export const Sinks = Object.freeze({
    /** Creates an empty sink that can only terminate. */
    empty<T>(): EmptySink<T> {
        return new DefaultOneSink<T>();
    },

    /** Creates a one-valued sink that replays its terminal result. */
    one<T>(): OneSink<T> {
        return new DefaultOneSink<T>();
    },

    /** Returns builders for many-valued sinks. */
    many(): ManySpec {
        return manySpec;
    },

    /** Returns unsafe sink builders. */
    unsafe(): typeof unsafeSpec {
        return unsafeSpec;
    }
});

export type {ManySink};
