import { describe, expect, it, vi } from "vitest";
import { Flux, Mono, Sinks, type Subscriber, type Subscription } from "@/index.js";

describe("Flux", () => {
  it("composes common synchronous operators", async () => {
    await expect(
      Flux.range(1, 6)
        .filter(value => value % 2 === 0)
        .map(value => value * 10)
        .collectList()
        .block()
    ).resolves.toEqual([20, 40, 60]);
  });

  it("honors downstream request amounts", async () => {
    const seen: number[] = [];
    let subscription: Subscription | undefined;
    let completed = false;

    const subscriber: Subscriber<number> = {
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
        nextSubscription.request(2);
      },
      onNext(value) {
        seen.push(value);
        if (value === 2) {
          subscription?.request(2);
        }
      },
      onError(error) {
        throw error;
      },
      onComplete() {
        completed = true;
      }
    };

    Flux.range(1, 5).subscribe(subscriber);
    await vi.waitFor(() => expect(seen).toEqual([1, 2, 3, 4]));
    expect(completed).toBe(false);

    subscription?.request(1);
    await vi.waitFor(() => expect(completed).toBe(true));
    expect(seen).toEqual([1, 2, 3, 4, 5]);
  });

  it("supports asynchronous interval sources", async () => {
    await expect(Flux.interval(1).take(3).toArray()).resolves.toEqual([0, 1, 2]);
  });

  it("recovers from errors", async () => {
    await expect(
      Flux.concat(Flux.just(1), Flux.error<number>(new Error("boom")))
        .onErrorResume(() => Flux.just(2, 3))
        .toArray()
    ).resolves.toEqual([1, 2, 3]);
  });

  it("zips publishers", async () => {
    await expect(
      Flux.zip<[number, string]>([Flux.range(1, 2), Flux.just("a", "b")])
        .map(values => `${values[0]}${values[1]}`)
        .toArray()
    ).resolves.toEqual(["1a", "2b"]);
  });

  it("uses plain arrays for tuple-like operator results", async () => {
    await expect(Flux.just(1).zipWith(Flux.just("a")).toArray()).resolves.toEqual([[1, "a"]]);

    const timestamped = await Flux.just("value").timestamp().next().block();
    expect(Array.isArray(timestamped)).toBe(true);
    expect(timestamped?.[1]).toBe("value");

    const elapsed = await Flux.just("value").elapsed().next().block();
    expect(Array.isArray(elapsed)).toBe(true);
    expect(elapsed?.[1]).toBe("value");
  });
});

describe("Mono", () => {
  it("maps and flatMaps one value", async () => {
    await expect(
      Mono.just(2)
        .map(value => value + 1)
        .flatMap(value => Mono.just(value * 3))
        .block()
    ).resolves.toBe(9);
  });

  it("supports promise and empty factories", async () => {
    await expect(Mono.fromPromise(Promise.resolve("ok")).block()).resolves.toBe("ok");
    await expect(Mono.justOrEmpty(undefined).defaultIfEmpty("fallback").block()).resolves.toBe("fallback");
  });

  it("waits for several publishers with when", async () => {
    await expect(Mono.when(Flux.just(1, 2), Mono.delay(1)).block()).resolves.toBeUndefined();
  });
});

describe("Sinks", () => {
  it("replays one sink completion to late subscribers", async () => {
    const sink = Sinks.one<number>();
    expect(sink.tryEmitValue(42)).toBe("OK");
    expect(await sink.asMono().block()).toBe(42);
    expect(sink.tryEmitValue(43)).toBe("FAIL_TERMINATED");
  });

  it("buffers unicast values before the first subscriber", async () => {
    const sink = Sinks.many().unicast().onBackpressureBuffer<number>();
    expect(sink.tryEmitNext(1)).toBe("OK");
    expect(sink.tryEmitNext(2)).toBe("OK");
    expect(sink.tryEmitComplete()).toBe("OK");
    await expect(sink.asFlux().toArray()).resolves.toEqual([1, 2]);
  });

  it("replays latest value to late subscribers", async () => {
    const sink = Sinks.many().replay().latest<number>();
    sink.emitNext(1);
    sink.emitNext(2);
    sink.emitComplete();
    await expect(sink.asFlux().toArray()).resolves.toEqual([2]);
  });

  it("reports zero subscribers for direct multicast", () => {
    const sink = Sinks.many().multicast().directAllOrNothing<number>();
    expect(sink.tryEmitNext(1)).toBe("FAIL_ZERO_SUBSCRIBER");
  });
});
