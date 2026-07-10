import { pathToFileURL } from "node:url";
import { describe, expect, it } from "vitest";

describe("built entrypoints", () => {
  it("keeps root runtime exports minimal", async () => {
    const api = await import(pathToFileURL(`${process.cwd()}/dist/index.js`).href);

    expect(Object.keys(api).sort()).toEqual([
      "Flux",
      "Mono",
      "Schedulers",
      "Sinks"
    ]);
  });

  it("loads focused chunks independently", async () => {
    const flux = await import(pathToFileURL(`${process.cwd()}/dist/flux/index.js`).href);
    const mono = await import(pathToFileURL(`${process.cwd()}/dist/mono/index.js`).href);
    const sinks = await import(pathToFileURL(`${process.cwd()}/dist/sinks/index.js`).href);
    const schedulers = await import(pathToFileURL(`${process.cwd()}/dist/schedulers/index.js`).href);

    expect(Object.keys(flux)).toEqual(["Flux"]);
    expect(Object.keys(mono)).toEqual(["Mono"]);
    expect(Object.keys(sinks)).toEqual(["Sinks"]);
    expect(Object.keys(schedulers)).toEqual(["Schedulers"]);
  });
});
