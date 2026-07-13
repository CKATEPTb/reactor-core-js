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

  it("distinctUntilChangedDeep detects nested mutations when the root reference is reused", async () => {
    const user = { about: { name: "Alice" } };
    const updates = Flux.fromStream(function* () {
      yield user;
      yield user;
      user.about.name = "Bob";
      yield user;
      yield user;
    });

    await expect(
      updates.distinctUntilChangedDeep().map(value => value.about.name).toArray()
    ).resolves.toEqual(["Alice", "Bob"]);
  });

  it("distinctUntilChangedDeep compares nested state across different references", async () => {
    const first = { about: { name: "Alice" } };
    const duplicate = { about: { name: "Alice" } };
    const changed = { about: { name: "Bob" } };

    const result = await Flux.just(first, duplicate, changed).distinctUntilChangedDeep().toArray();

    expect(result).toEqual([first, changed]);
    expect(result[0]).toBe(first);
  });

  it("distinctUntilChangedDeep snapshots a selected key for async sources", async () => {
    const user = { id: 1, about: { name: "Alice" } };
    const updates = Flux.from((async function* () {
      yield user;
      user.id = 2;
      yield user;
      user.about.name = "Bob";
      yield user;
    })());

    await expect(
      updates.distinctUntilChangedDeep(value => value.about).map(value => value.about.name).toArray()
    ).resolves.toEqual(["Alice", "Bob"]);
  });

  it("distinctUntilChangedDeep tracks sparse array length inside cyclic objects", async () => {
    const state: { items: unknown[]; self?: unknown } = { items: new Array(1) };
    state.self = state;
    const updates = Flux.fromStream(function* () {
      yield state;
      state.items.length = 2;
      yield state;
    });

    await expect(
      updates.distinctUntilChangedDeep().map(value => value.items.length).toArray()
    ).resolves.toEqual([1, 2]);
  });

  it("distinctUntilChangedDeep snapshots dates, regular expressions and collections", async () => {
    const state = {
      date: new Date(0),
      expression: /name/g,
      map: new Map([["name", "Alice"]]),
      set: new Set(["reader"])
    };
    const updates = Flux.fromStream(function* () {
      yield state;
      state.date.setTime(1);
      yield state;
      state.expression.lastIndex = 1;
      yield state;
      state.map.set("name", "Bob");
      yield state;
      state.set.add("writer");
      yield state;
    });

    await expect(
      updates.distinctUntilChangedDeep().map(value => [
        value.date.getTime(),
        value.expression.lastIndex,
        value.map.get("name"),
        value.set.size
      ]).toArray()
    ).resolves.toEqual([
      [0, 0, "Alice", 1],
      [1, 0, "Alice", 1],
      [1, 1, "Alice", 1],
      [1, 1, "Bob", 1],
      [1, 1, "Bob", 2]
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
