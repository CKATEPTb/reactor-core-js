import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const fail = process.argv.includes("--fail");

const reactorFiles = {
  Flux: path.join(root, "reactor-core/reactor-core/src/main/java/reactor/core/publisher/Flux.java"),
  Mono: path.join(root, "reactor-core/reactor-core/src/main/java/reactor/core/publisher/Mono.java"),
  Sinks: path.join(root, "reactor-core/reactor-core/src/main/java/reactor/core/publisher/Sinks.java")
};

const distIndex = path.join(root, "dist/index.js");
if (!fs.existsSync(distIndex)) {
  console.error("dist/index.js not found. Run npm run build first.");
  process.exit(2);
}

const runtime = await import(pathToFileURL(distIndex).href);

const fluxSource = fs.readFileSync(reactorFiles.Flux, "utf8");
const monoSource = fs.readFileSync(reactorFiles.Mono, "utf8");
const sinksSource = fs.readFileSync(reactorFiles.Sinks, "utf8");

const report = {
  Flux: compareClassApi(fluxSource, runtime.Flux),
  Mono: compareClassApi(monoSource, runtime.Mono),
  Sinks: compareSinksApi(sinksSource, runtime.Sinks)
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

function compareClassApi(source, ctor) {
  const javaApi = extractTopLevelClassMethods(source);
  const tsStatic = new Set(Object.getOwnPropertyNames(ctor).filter(name => !["length", "name", "prototype"].includes(name)));
  const tsInstance = new Set(Object.getOwnPropertyNames(ctor.prototype).filter(name => name !== "constructor"));

  return {
    static: summarize(javaApi.static, tsStatic),
    instance: summarize(javaApi.instance, tsInstance)
  };
}

function compareSinksApi(source, sinks) {
  const staticJava = extractTopLevelClassMethods(source).static;
  const nestedJava = extractNestedInterfaceMethods(source);
  const staticTs = new Set(Object.keys(sinks));
  const nestedTs = collectSinkRuntimeMethods(sinks);

  return {
    static: summarize(staticJava, staticTs),
    nested: summarize(nestedJava, nestedTs)
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

function extractTopLevelClassMethods(source) {
  source = stripBlockComments(source);
  const classMatch = source.match(/public\s+(?:abstract\s+|final\s+)?(?:class|interface)\s+\w+/);
  if (!classMatch) {
    return { static: new Set(), instance: new Set() };
  }
  const classOpen = source.indexOf("{", classMatch.index);
  const body = source.slice(classOpen + 1).replace(/\s+/g, " ");
  const staticMethods = new Set();
  const instanceMethods = new Set();
  const methodPattern = /\bpublic\s+(static|final|abstract)\s+([^;{}=]*?)\s+([a-zA-Z_$][\w$]*)\s*\(/g;
  for (const match of body.matchAll(methodPattern)) {
    const kind = match[1];
    const name = match[3];
    if (["Flux", "Mono", "Sinks"].includes(name)) {
      continue;
    }
    if (kind === "static") {
      staticMethods.add(name);
    } else {
      instanceMethods.add(name);
    }
  }

  return { static: staticMethods, instance: instanceMethods };
}

function extractNestedInterfaceMethods(source) {
  source = stripBlockComments(source);
  const methods = new Set();
  const normalized = source.replace(/\s+/g, " ");
  const methodPattern = /\b(?:[\w$<>,.?&\s@]+)\s+([a-zA-Z_$][\w$]*)\s*\([^;{}]*\)\s*;/g;
  for (const match of normalized.matchAll(methodPattern)) {
    const name = match[1];
    if (!["for", "if", "while", "switch"].includes(name) && /^[a-z]/.test(name)) {
      methods.add(name);
    }
  }
  return methods;
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

function stripLineComment(line) {
  return line.replace(/\/\/.*$/, "");
}

function stripBlockComments(source) {
  return source.replace(/\/\*[\s\S]*?\*\//g, "");
}

function count(value, char) {
  return [...value].filter(next => next === char).length;
}
