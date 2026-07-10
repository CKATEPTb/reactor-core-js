import { describe, expect, it } from "vitest";

describe("public package surface", () => {
  it("exports only the approved root symbols", async () => {
    const api = await import("@/index.js");

    expect(Object.keys(api).sort()).toEqual([
      "Context",
      "Flux",
      "Mono",
      "Schedulers",
      "Sinks"
    ]);
  });

  it("exposes Context from the package root", async () => {
    const { Context } = await import("@/index.js");

    expect(Context.empty().put("requestId", "abc").get("requestId")).toBe("abc");
  });

  it("supports focused subpath-style source entrypoints", async () => {
    const [{ Flux }, { Mono }, { Sinks }, { Schedulers }] = await Promise.all([
      import("@/publisher/flux-entrypoint.js"),
      import("@/publisher/mono-entrypoint.js"),
      import("@/sinks/index.js"),
      import("@/schedulers/index.js")
    ]);

    await expect(Flux.range(1, 2).toArray()).resolves.toEqual([1, 2]);
    await expect(Mono.just("ok").block()).resolves.toBe("ok");
    expect(Sinks.one()).toBeTruthy();
    expect(Schedulers.immediate().now()).toEqual(expect.any(Number));
  });
});
