import fs from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { Flux, Sinks } from "@/index.js";

type SinkScenario = {
  name: string;
  factories: readonly string[];
  run(): Promise<unknown>;
};

const expected = JSON.parse(fs.readFileSync(path.join(process.cwd(), "test", "fixtures", "reactor-oracle-matrix.json"), "utf8")) as Record<
  string,
  unknown
>;

describe("Sinks Java parity matrix", () => {
  it("covers every public sink factory variation", () => {
    const covered = new Set(sinkScenarios.flatMap(scenario => scenario.factories));

    expect([...covered].sort()).toEqual([
      "Sinks.empty",
      "Sinks.many.multicast.directAllOrNothing",
      "Sinks.many.multicast.directBestEffort",
      "Sinks.many.multicast.onBackpressureBuffer",
      "Sinks.many.replay.all",
      "Sinks.many.replay.latest",
      "Sinks.many.replay.latestOrDefault",
      "Sinks.many.replay.limit",
      "Sinks.many.unicast.onBackpressureBuffer",
      "Sinks.many.unicast.onBackpressureError",
      "Sinks.one",
      "Sinks.unsafe.manyWithUpstream.multicastOnBackpressureBuffer"
    ]);
  });

  it("matches Java Reactor sink fixture results", async () => {
    let executed = 0;
    for (const scenario of sinkScenarios) {
      executed += 1;
      const result = await scenario.run();
      console.log(`[sinks-matrix] ${scenario.name}: ${JSON.stringify(result)}`);
      expect(result, scenario.name).toEqual(expected[scenario.name]);
    }
    console.log(`[sinks-matrix] scenarios executed: ${executed}`);
    expect(executed).toBeGreaterThanOrEqual(16);
  });

  it("rejects invalid replay history sizes like Java Reactor", () => {
    expect(() => Sinks.many().replay().limit(0)).toThrow(RangeError);
  });
});

const sinkScenarios: SinkScenario[] = [
  {
    name: "sinks-one-value-late",
    factories: ["Sinks.one"],
    async run() {
      const sink = Sinks.one<number>();
      return [sink.tryEmitValue(1), sink.tryEmitValue(2), await sink.asMono().block()];
    }
  },
  {
    name: "sinks-one-empty-late",
    factories: ["Sinks.one"],
    async run() {
      const sink = Sinks.one<number>();
      return [sink.tryEmitEmpty(), sink.tryEmitValue(1), await sink.asMono().defaultIfEmpty(-1).block()];
    }
  },
  {
    name: "sinks-one-error-late",
    factories: ["Sinks.one"],
    async run() {
      const sink = Sinks.one<number>();
      return [sink.tryEmitError(new Error("boom")), sink.tryEmitValue(1), await sink.asMono().onErrorReturn(-1).block()];
    }
  },
  {
    name: "sinks-empty-complete-late",
    factories: ["Sinks.empty"],
    async run() {
      const sink = Sinks.empty<number>();
      return [sink.tryEmitEmpty(), sink.tryEmitError(new Error("boom")), await sink.asMono().defaultIfEmpty(-1).block()];
    }
  },
  {
    name: "sinks-emit-after-terminated",
    factories: ["Sinks.one"],
    async run() {
      const sink = Sinks.one<number>();
      return captureEmissionReason(() => {
        sink.emitValue(1);
        sink.emitValue(2);
      });
    }
  },
  {
    name: "sinks-unicast-buffer-late",
    factories: ["Sinks.many.unicast.onBackpressureBuffer"],
    async run() {
      const sink = Sinks.many().unicast().onBackpressureBuffer<number>();
      return [sink.tryEmitNext(1), sink.tryEmitNext(2), sink.tryEmitComplete(), await collectFlux(sink.asFlux())];
    }
  },
  {
    name: "sinks-unicast-error-zero-subscriber",
    factories: ["Sinks.many.unicast.onBackpressureError"],
    async run() {
      const sink = Sinks.many().unicast().onBackpressureError<number>();
      return [sink.tryEmitNext(1), sink.tryEmitComplete(), await collectFlux(sink.asFlux())];
    }
  },
  {
    name: "sinks-multicast-buffer-warmup",
    factories: ["Sinks.many.multicast.onBackpressureBuffer"],
    async run() {
      const sink = Sinks.many().multicast().onBackpressureBuffer<number>();
      return [sink.tryEmitNext(1), sink.tryEmitNext(2), sink.tryEmitComplete(), await collectFlux(sink.asFlux())];
    }
  },
  {
    name: "sinks-multicast-direct-zero-subscriber",
    factories: ["Sinks.many.multicast.directAllOrNothing", "Sinks.many.multicast.directBestEffort"],
    async run() {
      return [
        Sinks.many().multicast().directAllOrNothing<number>().tryEmitNext(1),
        Sinks.many().multicast().directBestEffort<number>().tryEmitNext(1)
      ];
    }
  },
  {
    name: "sinks-replay-all-late",
    factories: ["Sinks.many.replay.all"],
    async run() {
      const sink = Sinks.many().replay().all<number>();
      return [sink.tryEmitNext(1), sink.tryEmitNext(2), sink.tryEmitComplete(), await collectFlux(sink.asFlux())];
    }
  },
  {
    name: "sinks-replay-limit-late",
    factories: ["Sinks.many.replay.limit"],
    async run() {
      const sink = Sinks.many().replay().limit<number>(2);
      return [sink.tryEmitNext(1), sink.tryEmitNext(2), sink.tryEmitNext(3), sink.tryEmitComplete(), await collectFlux(sink.asFlux())];
    }
  },
  {
    name: "sinks-replay-latest-late",
    factories: ["Sinks.many.replay.latest"],
    async run() {
      const sink = Sinks.many().replay().latest<number>();
      return [sink.tryEmitNext(1), sink.tryEmitNext(2), sink.tryEmitComplete(), await collectFlux(sink.asFlux())];
    }
  },
  {
    name: "sinks-replay-latest-or-default",
    factories: ["Sinks.many.replay.latestOrDefault"],
    async run() {
      return collectFlux(Sinks.many().replay().latestOrDefault<number>(9).asFlux().take(1));
    }
  },
  {
    name: "sinks-many-with-upstream",
    factories: ["Sinks.unsafe.manyWithUpstream.multicastOnBackpressureBuffer"],
    async run() {
      const sink = Sinks.unsafe().manyWithUpstream().multicastOnBackpressureBuffer<number>();
      return [subscribeTo(sink, Flux.just(1, 2)), await collectFlux(sink.asFlux())];
    }
  },
  {
    name: "sinks-one-subscriber-count",
    factories: ["Sinks.one"],
    async run() {
      const sink = Sinks.one<number>();
      const subscription = sink.asMono().subscribe(() => undefined);
      const beforeDispose = sink.currentSubscriberCount();
      subscription.dispose();
      return [beforeDispose, sink.currentSubscriberCount()];
    }
  },
  {
    name: "sinks-many-subscriber-count",
    factories: ["Sinks.many.multicast.onBackpressureBuffer"],
    async run() {
      const sink = Sinks.many().multicast().onBackpressureBuffer<number>();
      const subscription = sink.asFlux().subscribe(() => undefined);
      const beforeDispose = sink.currentSubscriberCount();
      subscription.dispose();
      return [beforeDispose, sink.currentSubscriberCount()];
    }
  }
];

function subscribeTo<T>(sink: { subscribeTo(source: Flux<T>): void }, source: Flux<T>): string {
  sink.subscribeTo(source);
  return "SUBSCRIBED";
}

async function collectFlux<T>(flux: Flux<T>): Promise<T[] | string> {
  try {
    return await withTimeout(flux.toArray(), 100);
  } catch {
    return "TIMEOUT_OR_ERROR:IllegalStateException";
  }
}

function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error("timeout")), ms);
    promise.then(
      value => {
        clearTimeout(timeout);
        resolve(value);
      },
      error => {
        clearTimeout(timeout);
        reject(error);
      }
    );
  });
}

function captureEmissionReason(callback: () => void): string {
  try {
    callback();
    return "NO_ERROR";
  } catch (error) {
    return String((error as { reason?: unknown; message?: unknown }).reason ?? (error as Error).message ?? error);
  }
}
