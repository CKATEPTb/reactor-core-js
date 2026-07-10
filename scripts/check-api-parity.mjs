import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const fail = process.argv.includes("--fail");
const fixturePath = path.join(root, "test", "fixtures", "reactor-oracle-matrix.json");

const distIndex = path.join(root, "dist/index.js");
if (!fs.existsSync(distIndex)) {
  console.error("dist/index.js not found. Run npm run build first.");
  process.exit(2);
}

const runtime = await import(pathToFileURL(distIndex).href);
const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
const expectedApi = fixture.api;

if (!expectedApi?.Flux || !expectedApi?.Mono || !expectedApi?.Sinks) {
  console.error(`API fixture is missing in ${fixturePath}. Run npm run generate-fixtures to refresh test fixtures.`);
  process.exit(2);
}

const report = {
  Flux: compareClassApi(expectedApi.Flux, runtime.Flux),
  Mono: compareClassApi(expectedApi.Mono, runtime.Mono),
  Sinks: compareSinksApi(expectedApi.Sinks, runtime.Sinks)
};

console.log(JSON.stringify(report, null, 2));

const missing =
  report.Flux.static.missing.length +
  report.Flux.instance.missing.length +
  report.Mono.static.missing.length +
  report.Mono.instance.missing.length +
  report.Sinks.static.missing.length +
  report.Sinks.nested.missing.length;

if (fail && missing > 0) {
  process.exit(1);
}

function compareClassApi(expected, ctor) {
  const tsStatic = new Set(Object.getOwnPropertyNames(ctor).filter(name => !["length", "name", "prototype"].includes(name)));
  const tsInstance = new Set(Object.getOwnPropertyNames(ctor.prototype).filter(name => name !== "constructor"));

  return {
    static: summarize(new Set(expected.static), tsStatic),
    instance: summarize(new Set(expected.instance), tsInstance)
  };
}

function compareSinksApi(expected, sinks) {
  const staticTs = new Set(Object.keys(sinks));
  const nestedTs = collectSinkRuntimeMethods(sinks);

  return {
    static: summarize(new Set(expected.static), staticTs),
    nested: summarize(new Set(expected.nested), nestedTs)
  };
}

function summarize(expected, actual) {
  const missing = [...expected].filter(name => !actual.has(name)).sort();
  const extra = [...actual].filter(name => !expected.has(name)).sort();
  return {
    expected: expected.size,
    implemented: [...expected].filter(name => actual.has(name)).length,
    missing,
    extra
  };
}

function collectSinkRuntimeMethods(sinks) {
  const methods = new Set();
  const samples = [
    sinks.empty(),
    sinks.one(),
    sinks.many(),
    sinks.unsafe(),
    sinks.unsafe().manyWithUpstream(),
    sinks.unsafe().manyWithUpstream().multicastOnBackpressureBuffer(),
    sinks.many().unicast(),
    sinks.many().multicast(),
    sinks.many().replay(),
    sinks.many().unicast().onBackpressureBuffer(),
    sinks.many().multicast().onBackpressureBuffer(),
    sinks.many().replay().latest()
  ];
  for (const sample of samples) {
    let object = sample;
    while (object && object !== Object.prototype) {
      for (const name of Object.getOwnPropertyNames(object)) {
        if (name !== "constructor" && typeof sample[name] === "function") {
          methods.add(name);
        }
      }
      object = Object.getPrototypeOf(object);
    }
  }
  return methods;
}
