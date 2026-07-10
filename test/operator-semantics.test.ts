import { describe, expect, it } from "vitest";
import { Flux, Mono, Schedulers } from "@/index.js";
import type { Subscription } from "@/index.js";

describe("operator-specific semantics", () => {
  it("flatMap emits concurrently completed inner publishers", async () => {
    await expect(
      Flux.just(1, 2)
        .flatMap(value => Mono.delay(value === 1 ? 20 : 0, Schedulers.timeout()).map(() => value), 2)
        .toArray()
    ).resolves.toEqual([2, 1]);
  });

  it("flatMap uses unbounded concurrency by default", async () => {
    await expect(
      Flux.just(1, 2)
        .flatMap(value => Mono.delay(value === 1 ? 20 : 0, Schedulers.timeout()).map(() => value))
        .toArray()
    ).resolves.toEqual([2, 1]);
  });

  it("flatMap with concurrency one behaves like concatMap", async () => {
    await expect(
      Flux.just(1, 2)
        .flatMap(value => Mono.delay(value === 1 ? 20 : 0, Schedulers.timeout()).map(() => value), 1)
        .toArray()
    ).resolves.toEqual([1, 2]);
  });

  it("concatMap preserves source order while flattening inner publishers", async () => {
    await expect(
      Flux.just(1, 2)
        .concatMap(value => Flux.just(value, value * 10))
        .toArray()
    ).resolves.toEqual([1, 10, 2, 20]);
  });

  it("switchMap emits only values from the latest active inner publisher", async () => {
    await expect(
      Flux.just(1, 2)
        .switchMap(value => Mono.delay(value === 1 ? 20 : 0, Schedulers.timeout()).map(() => value))
        .toArray()
    ).resolves.toEqual([2]);
  });

  it("distinct keeps the first value for every selected key", async () => {
    await expect(Flux.just("a", "bb", "cc", "ddd").distinct(value => value.length).toArray()).resolves.toEqual([
      "a",
      "bb",
      "ddd"
    ]);
  });

  it("distinctUntilChanged only compares adjacent selected keys", async () => {
    await expect(Flux.just("a", "b", "bb", "cc", "d").distinctUntilChanged(value => value.length).toArray()).resolves.toEqual([
      "a",
      "bb",
      "d"
    ]);
  });

  it("window emits usable Flux values instead of awaited thenables", async () => {
    await expect(collectWindows(Flux.range(1, 5).window(2))).resolves.toEqual([[1, 2], [3, 4], [5]]);
  });

  it("timeout cancels a never source and switches to fallback", async () => {
    await expect(Flux.never<number>().timeout(1, Flux.just(9), Schedulers.timeout()).toArray()).resolves.toEqual([9]);
  });

  it("take creates async upstream only once", async () => {
    let factories = 0;
    const source = new Flux(() => {
      factories += 1;
      return (async function* () {
        yield 1;
        yield 2;
      })();
    });

    await expect(source.take(1).toArray()).resolves.toEqual([1]);
    expect(factories).toBe(1);
  });

  it("reduceWith and scanWith create supplier state per subscription", async () => {
    let reducedSeeds = 0;
    const reduced = Flux.just(1, 2).reduceWith(() => {
      reducedSeeds += 1;
      return 0;
    }, (sum, value) => sum + value);

    expect(reducedSeeds).toBe(0);
    await expect(reduced.block()).resolves.toBe(3);
    await expect(reduced.block()).resolves.toBe(3);
    expect(reducedSeeds).toBe(2);

    let scannedSeeds = 0;
    const scanned = Flux.just(1, 2).scanWith(() => {
      scannedSeeds += 1;
      return 0;
    }, (sum, value) => sum + value);

    expect(scannedSeeds).toBe(0);
    await expect(scanned.toArray()).resolves.toEqual([0, 1, 3]);
    await expect(scanned.toArray()).resolves.toEqual([0, 1, 3]);
    expect(scannedSeeds).toBe(2);
  });

  it("collect supplier is not invoked before demand", () => {
    let suppliers = 0;
    let subscription: Subscription | undefined;
    const values: number[][] = [];

    Flux.just(1).collect(() => {
      suppliers += 1;
      return [] as number[];
    }, (container, value) => {
      container.push(value);
    }).subscribe({
      onSubscribe(next) {
        subscription = next;
      },
      onNext(value) {
        values.push(value);
      },
      onError(error) {
        throw error;
      },
      onComplete() {
        // no-op
      }
    });

    expect(suppliers).toBe(0);
    subscription?.request(1);
    expect(suppliers).toBe(1);
    expect(values).toEqual([[1]]);
  });

  it("terminal operators do not pull before demand", () => {
    let pulls = 0;
    const source = Flux.fromIterable({
      [Symbol.iterator]() {
        return {
          next() {
            pulls += 1;
            return { done: false, value: 1 };
          }
        };
      }
    });

    source.any(value => value === 1).subscribe({
      onSubscribe() {
        // no request
      },
      onNext() {
        throw new Error("unexpected value");
      },
      onError(error) {
        throw error;
      },
      onComplete() {
        throw new Error("unexpected completion");
      }
    });

    expect(pulls).toBe(0);
  });
});

async function collectWindows<T>(windows: Flux<Flux<T>>): Promise<T[][]> {
  const result: T[][] = [];
  for await (const window of windows) {
    result.push(await window.toArray());
  }
  return result;
}
