import { pathToFileURL } from "node:url";
import fs from "node:fs";
import { describe, expect, it } from "vitest";

type PackageMetadata = {
  type: string;
  module: string;
  browser: string;
  sideEffects: string[];
  exports: Record<string, unknown>;
};

describe("built entrypoints", () => {
  it("describes a browser ESM package with focused subpath exports", () => {
    const metadata = JSON.parse(fs.readFileSync(`${process.cwd()}/package.json`, "utf8")) as PackageMetadata;
    const rootExport = metadata.exports["."] as Record<string, string>;
    const fluxExport = metadata.exports["./flux"] as Record<string, string>;

    expect(metadata.type).toBe("module");
    expect(metadata.module).toBe("./dist/index.js");
    expect(metadata.browser).toBe("./dist/index.js");
    expect(rootExport).toMatchObject({
      types: "./dist/index.d.ts",
      browser: "./dist/index.js",
      import: "./dist/index.js",
      default: "./dist/index.js"
    });
    expect(fluxExport).toMatchObject({
      types: "./dist/publisher/flux-entrypoint.d.ts",
      browser: "./dist/publisher/flux-entrypoint.js",
      import: "./dist/publisher/flux-entrypoint.js",
      default: "./dist/publisher/flux-entrypoint.js"
    });
    expect(metadata.sideEffects).toContain("./dist/publisher/operators/register.js");
  });

  it("keeps root runtime exports minimal", async () => {
    const api = await import(pathToFileURL(`${process.cwd()}/dist/index.js`).href);

    expect(Object.keys(api).sort()).toEqual([
      "Context",
      "Flux",
      "Mono",
      "Schedulers",
      "Sinks"
    ]);
  });

  it("keeps root declaration exports constrained to the approved TypeScript surface", () => {
    const declaration = fs.readFileSync(`${process.cwd()}/dist/index.d.ts`, "utf8");
    const names = Array.from(declaration.matchAll(/export(?: type)? \{([^}]+)\}/g))
      .flatMap(match => match[1]!.split(","))
      .map(name => name.trim().replace(/\s+as\s+.+$/, ""))
      .sort();

    expect(names).toEqual([
      "Context",
      "Disposable",
      "Flux",
      "Mono",
      "Publisher",
      "Schedulers",
      "Sinks",
      "Subscriber",
      "Subscription"
    ]);
  });

  it("loads focused chunks independently", async () => {
    const flux = await import(pathToFileURL(`${process.cwd()}/dist/publisher/flux-entrypoint.js`).href);
    const mono = await import(pathToFileURL(`${process.cwd()}/dist/publisher/mono-entrypoint.js`).href);
    const sinks = await import(pathToFileURL(`${process.cwd()}/dist/sinks/index.js`).href);
    const schedulers = await import(pathToFileURL(`${process.cwd()}/dist/schedulers/index.js`).href);

    expect(Object.keys(flux)).toEqual(["Flux"]);
    expect(Object.keys(mono)).toEqual(["Mono"]);
    expect(Object.keys(sinks)).toEqual(["Sinks"]);
    expect(Object.keys(schedulers)).toEqual(["Schedulers"]);
  });
});
