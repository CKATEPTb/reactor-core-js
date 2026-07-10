import { describe, expect, it } from "vitest";

describe("public package surface", () => {
  it("exports only the approved root symbols", async () => {
    const api = await import("@/index.js");

    expect(Object.keys(api).sort()).toEqual([
      "Flux",
      "Mono",
      "Schedulers",
      "Sinks"
    ]);
  });

  it("supports focused subpath-style source entrypoints", async () => {
    const [{ Flux }, { Mono }, { Sinks }, { Schedulers }] = await Promise.all([
      import("@/flux/index.js"),
      import("@/mono/index.js"),
      import("@/sinks/index.js"),
      import("@/schedulers/index.js")
    ]);

    await expect(Flux.range(1, 2).toArray()).resolves.toEqual([1, 2]);
    await expect(Mono.just("ok").block()).resolves.toBe("ok");
    expect(Sinks.one()).toBeTruthy();
    expect(Schedulers.immediate().now()).toEqual(expect.any(Number));
  });
});
