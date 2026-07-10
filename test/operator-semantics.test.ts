import { describe, expect, it } from "vitest";
import { Flux, Mono, Schedulers } from "@/index.js";

describe("operator-specific semantics", () => {
  it("flatMap emits concurrently completed inner publishers", async () => {
    await expect(
      Flux.just(1, 2)
        .flatMap(value => Mono.delay(value === 1 ? 20 : 0, Schedulers.timeout()).map(() => value), 2)
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
});

async function collectWindows<T>(windows: Flux<Flux<T>>): Promise<T[][]> {
  const result: T[][] = [];
  for await (const window of windows) {
    result.push(await window.toArray());
  }
  return result;
}
