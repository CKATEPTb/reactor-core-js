/**
 * Runs the compact Reactor oracle against the TypeScript build.
 *
 * CI uses the committed fixture so every run does not need a JVM, Gradle or a
 * local reactor-core checkout. Use `--java` to compare against Maven artifacts
 * or `--refresh` to regenerate the fixture from the Maven oracle.
 */
import { execFileSync } from "node:child_process";
import { createWriteStream, existsSync, mkdirSync, mkdtempSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { get } from "node:https";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { pipeline } from "node:stream/promises";
import { pathToFileURL } from "node:url";

/** Repository root directory. */
const root = resolve(".");
/** Cached Maven artifact directory. */
const cacheDir = join(root, ".reactor-oracle-cache");
/** Committed Java oracle fixture path. */
const fixturePath = join(root, "test", "fixtures", "reactor-oracle-matrix.json");
/** Reactor Core Maven version used by the fixture refresh mode. */
const reactorCoreVersion = process.env.REACTOR_CORE_VERSION ?? "3.8.6";
/** Reactive Streams Maven version used by Reactor Core. */
const reactiveStreamsVersion = process.env.REACTIVE_STREAMS_VERSION ?? "1.0.4";
/** Temporary directory for the compiled Java oracle. */
const workDir = mkdtempSync(join(tmpdir(), "reactor-oracle-"));
/** Whether this invocation should regenerate the fixture. */
const refreshFixture = process.argv.includes("--refresh");
/** Whether this invocation should execute the Java oracle. */
const runJava = refreshFixture || process.argv.includes("--java");

try {
  log(`mode=${refreshFixture ? "refresh" : runJava ? "java" : "fixture"}`);
  log("load expected result");
  const expected = runJava ? await runJavaOracle() : readFixture();
  if (refreshFixture) {
    log("write refreshed fixture");
    writeFixture(expected);
  }

  log("run TypeScript oracle");
  const actual = await runTypeScriptOracle();
  log("compare oracle output");
  if (JSON.stringify(expected) !== JSON.stringify(actual)) {
    console.error(JSON.stringify({ diffs: diffValues(expected, actual).slice(0, 50) }, null, 2));
    process.exit(1);
  }
  console.log(JSON.stringify({ ok: true, scenarios: Object.keys(expected).length, result: actual }, null, 2));
} finally {
  rmSync(workDir, { force: true, recursive: true });
}

/** Writes a visible sequence marker for CI logs. */
function log(message) {
  console.log(`[java-oracle] ${message}`);
}

/** Reads the committed Java oracle fixture. */
function readFixture() {
  return JSON.parse(readFileSync(fixturePath, "utf8"));
}

/** Writes a deterministic JSON fixture. */
function writeFixture(value) {
  writeFileSync(fixturePath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

/** Returns human-readable differences between two JSON-compatible values. */
function diffValues(expected, actual, path = "$", diffs = []) {
  if (diffs.length >= 50) {
    return diffs;
  }
  if (JSON.stringify(expected) === JSON.stringify(actual)) {
    return diffs;
  }
  if (Array.isArray(expected) && Array.isArray(actual)) {
    const length = Math.max(expected.length, actual.length);
    for (let index = 0; index < length; index += 1) {
      diffValues(expected[index], actual[index], `${path}[${index}]`, diffs);
      if (diffs.length >= 50) {
        return diffs;
      }
    }
    return diffs;
  }
  if (isPlainObject(expected) && isPlainObject(actual)) {
    const keys = [...new Set([...Object.keys(expected), ...Object.keys(actual)])].sort();
    for (const key of keys) {
      diffValues(expected[key], actual[key], `${path}.${key}`, diffs);
      if (diffs.length >= 50) {
        return diffs;
      }
    }
    return diffs;
  }
  diffs.push({ path, expected, actual });
  return diffs;
}

/** Returns true for plain JSON object values. */
function isPlainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/** Runs the Java oracle program against Maven-downloaded Reactor artifacts. */
async function runJavaOracle() {
  log(`download reactor-core ${reactorCoreVersion}`);
  const reactorJar = await downloadMavenArtifact("io.projectreactor", "reactor-core", reactorCoreVersion);
  log(`download reactive-streams ${reactiveStreamsVersion}`);
  const reactiveStreamsJar = await downloadMavenArtifact("org.reactivestreams", "reactive-streams", reactiveStreamsVersion);
  log("compile Java oracle");
  const source = join(workDir, "ReactorOracleMatrix.java");
  writeFileSync(source, javaSource(), "utf8");
  const separator = process.platform === "win32" ? ";" : ":";
  const classpath = [reactorJar, reactiveStreamsJar].join(separator);
  execFileSync("javac", ["-cp", classpath, source], { stdio: "inherit" });
  log("run Java oracle");
  const output = execFileSync("java", ["-cp", `${classpath}${separator}${workDir}`, "ReactorOracleMatrix"], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "inherit"]
  });
  return JSON.parse(output);
}

/** Downloads one Maven artifact into the local oracle cache. */
async function downloadMavenArtifact(groupId, artifactId, version) {
  const groupPath = groupId.replaceAll(".", "/");
  const fileName = `${artifactId}-${version}.jar`;
  const target = join(cacheDir, groupPath, artifactId, version, fileName);
  if (existsSync(target)) {
    log(`use cached ${fileName}`);
    return target;
  }

  mkdirSync(dirname(target), { recursive: true });
  const url = `https://repo1.maven.org/maven2/${groupPath}/${artifactId}/${version}/${fileName}`;
  const temporary = `${target}.tmp-${process.pid}`;
  log(`GET ${url}`);
  try {
    await download(url, temporary);
    renameSync(temporary, target);
  } catch (error) {
    rmSync(temporary, { force: true });
    throw error;
  }
  return target;
}

/** Downloads a URL to a local file, following redirects from Maven mirrors. */
async function download(url, target, redirects = 0) {
  if (redirects > 5) {
    throw new Error(`Too many redirects while downloading ${url}`);
  }
  await new Promise((resolvePromise, rejectPromise) => {
    get(url, response => {
      const status = response.statusCode ?? 0;
      const location = response.headers.location;
      if (status >= 300 && status < 400 && location) {
        response.resume();
        download(new URL(location, url).toString(), target, redirects + 1).then(resolvePromise, rejectPromise);
        return;
      }
      if (status !== 200) {
        response.resume();
        rejectPromise(new Error(`Failed to download ${url}: HTTP ${status}`));
        return;
      }
      pipeline(response, createWriteStream(target)).then(resolvePromise, rejectPromise);
    }).on("error", rejectPromise);
  });
}

/** Runs equivalent scenarios against the built TypeScript package. */
async function runTypeScriptOracle() {
  const { Flux, Mono, Schedulers, Sinks } = await import(pathToFileURL(join(root, "dist", "index.js")).href);
  return {
    "flux-map-filter": await Flux.range(1, 5).filter(value => value % 2 === 1).map(value => value * 10).toArray(),
    "flux-concat-map": await Flux.just(1, 2).concatMap(value => Flux.just(value, value + 10)).toArray(),
    "flux-flat-map": await Flux.just(1, 2)
      .flatMap(value => Mono.delay(value === 1 ? 20 : 0, Schedulers.boundedElastic()).map(() => value), 2)
      .toArray(),
    "flux-distinct": await Flux.just("a", "bb", "cc", "ddd").distinct(value => value.length).toArray(),
    "flux-distinct-until-changed": await Flux.just("a", "b", "bb", "cc", "d").distinctUntilChanged(value => value.length).toArray(),
    "flux-zip": await Flux.zip([Flux.just(1, 2), Flux.just("a", "b")]).toArray(),
    "flux-switch-empty": await Flux.empty().switchIfEmpty(Flux.just(7)).toArray(),
    "flux-error-resume": await Flux.concat(Flux.just(1), Flux.error(new Error("boom"))).onErrorResume(() => Flux.just(9)).toArray(),
    "flux-buffer": await Flux.range(1, 5).buffer(2).toArray(),
    "flux-window": await collectWindows(Flux.range(1, 5).window(2)),
    "mono-flat-map": await Mono.just(2).map(value => value + 1).flatMap(value => Mono.just(value * 3)).block(),
    "mono-then-many": await Mono.just(1).thenMany(Flux.just(2, 3)).toArray(),
    "mono-zip": await Mono.zip([Mono.just(1), Mono.just("a")]).block(),
    "scheduler-delay": await Mono.delay(0, Schedulers.boundedElastic()).block(),
    "method-matrix": await runTypeScriptMethodMatrixOracle(Flux, Mono, Schedulers),
    ...(await runTypeScriptSinkOracle(Sinks, Flux))
  };
}

/** Collects nested Flux windows into plain arrays for JSON comparison. */
async function collectWindows(windows) {
  const result = [];
  for await (const window of windows) {
    result.push(await window.toArray());
  }
  return result;
}

/** Runs the Java-comparable method matrix against the TypeScript build. */
async function runTypeScriptMethodMatrixOracle(Flux, Mono, Schedulers) {
  const fluxStatic = [
    ["just", () => Flux.just(1, 2)],
    ["defer", () => Flux.defer(() => Flux.just(1, 2))],
    ["fromIterable", () => Flux.fromIterable([1, 2])],
    ["range", () => Flux.range(1, 2)],
    ["empty", () => Flux.empty()],
    ["error", () => Flux.error(new Error("boom"))],
    ["concat", () => Flux.concat(Flux.just(1), Flux.just(2))],
    ["zip", () => Flux.zip([Flux.just(1), Flux.just(2)]).map(values => values[0] + values[1])],
    ["create", () => Flux.create(sink => { sink.next(1); sink.next(2); sink.complete(); })],
    ["generate", () => generatedFlux(Flux)]
  ];
  const monoStatic = [
    ["just", () => Mono.just(1)],
    ["defer", () => Mono.defer(() => Mono.just(1))],
    ["empty", () => Mono.empty()],
    ["error", () => Mono.error(new Error("boom"))],
    ["fromSupplier", () => Mono.fromSupplier(() => 1)],
    ["fromCallable", () => Mono.fromCallable(() => 1)],
    ["fromRunnable", () => Mono.fromRunnable(() => undefined)],
    ["justOrEmpty", () => Mono.justOrEmpty(1)],
    ["zip", () => Mono.zip([Mono.just(1), Mono.just(2)]).map(values => values[0] + values[1])],
    ["delay", () => Mono.delay(0, Schedulers.boundedElastic()).map(value => Number(value))]
  ];
  const fluxOps = [
    ["identity", source => source],
    ["map", source => source.map(value => Number(value) + 1)],
    ["filter", source => source.filter(value => Number(value) % 2 === 1)],
    ["take", source => source.take(1)],
    ["skip", source => source.skip(1)],
    ["defaultIfEmpty", source => source.defaultIfEmpty(9)],
    ["switchIfEmpty", source => source.switchIfEmpty(Flux.just(7))],
    ["concatMap", source => source.concatMap(value => Flux.just(value, Number(value) + 10))],
    ["flatMap", source => source.flatMap(value => Flux.just(Number(value) + 10), 1)],
    ["distinct", source => source.distinct(value => Number(value) % 2)],
    ["distinctUntilChanged", source => source.distinctUntilChanged(value => Number(value) % 2)],
    ["buffer", source => source.buffer(2)],
    ["collectList", source => source.collectList()],
    ["count", source => source.count()],
    ["next", source => source.next()],
    ["reduce", source => source.reduce((left, right) => Number(left) + Number(right))],
    ["scan", source => source.scan((left, right) => Number(left) + Number(right))],
    ["startWith", source => source.startWith(0)],
    ["concatWith", source => source.concatWith(Flux.just(9))],
    ["zipWith", source => source.zipWith(Flux.just(9)).map(values => Number(values[0]) + Number(values[1]))],
    ["onErrorReturn", source => source.onErrorReturn(-1)],
    ["onErrorResume", source => source.onErrorResume(() => Flux.just(-2))]
  ];
  const monoOps = [
    ["identity", source => source],
    ["map", source => source.map(value => Number(value) + 1)],
    ["filter", source => source.filter(value => Number(value) % 2 === 1)],
    ["defaultIfEmpty", source => source.defaultIfEmpty(9)],
    ["switchIfEmpty", source => source.switchIfEmpty(Mono.just(7))],
    ["flatMap", source => source.flatMap(value => Mono.just(Number(value) + 10))],
    ["thenReturn", source => source.thenReturn(5)],
    ["thenMany", source => source.thenMany(Flux.just(1, 2))],
    ["zipWith", source => source.zipWith(Mono.just(9)).map(values => Number(values[0]) + Number(values[1]))],
    ["onErrorReturn", source => source.onErrorReturn(-1)],
    ["onErrorResume", source => source.onErrorResume(() => Mono.just(-2))],
    ["flux", source => source.flux()]
  ];
  const fluxChainOps = fluxOps.filter(([name]) => !["buffer", "collectList", "count", "next", "reduce"].includes(name));
  const monoChainOps = monoOps.filter(([name]) => !["thenMany", "flux"].includes(name));

  return {
    staticLaziness: {
      fluxJust: await doubleCollect(() => Flux.just(1)),
      fluxDefer: await doubleCollect(counterFlux(Flux)),
      monoJust: await doubleBlock(() => Mono.just(1)),
      monoDefer: await doubleBlock(counterMono(Mono)),
      monoFromSupplier: await doubleBlock(counterSupplierMono(Mono))
    },
    traces: await runTypeScriptTraceMatrixOracle(Flux, Mono, Schedulers),
    sections: {
      fluxStaticStandalone: await summarizeMatrix("fluxStaticStandalone", fluxStatic, [["collect", source => source]], (factory, op) => op(factory())),
      monoStaticStandalone: await summarizeMatrix("monoStaticStandalone", monoStatic, [["block", source => source]], (factory, op) => op(factory())),
      fluxStaticToFlux: await summarizeMatrix("fluxStaticToFlux", fluxStatic, fluxOps, (factory, op) => op(factory())),
      monoStaticToMono: await summarizeMatrix("monoStaticToMono", monoStatic, monoOps, (factory, op) => op(factory())),
      fluxBeforeAfter: await summarizeMatrix("fluxBeforeAfter", fluxChainOps, fluxOps, (before, after) => after(toFluxOracle(before(Flux.just(1, 2)), Flux))),
      monoBeforeAfter: await summarizeMatrix("monoBeforeAfter", monoChainOps, monoOps, (before, after) => after(toMonoOracle(before(Mono.just(1)), Mono)))
    }
  };
}

/** Runs Java-comparable signal, context and lifecycle trace scenarios. */
async function runTypeScriptTraceMatrixOracle(Flux, Mono, Schedulers) {
  const requestOneByOne = await traceTypeScriptRequestOneByOne(Flux);
  const noRequest = await traceTypeScriptNoRequest(Flux);
  const lazyDefer = await traceTypeScriptLazyDefer(Flux);
  const nestedFlatMapSchedulers = await traceTypeScriptNestedFlatMapSchedulers(Flux, Mono, Schedulers);
  const lifecyclePositions = await traceTypeScriptLifecyclePositions(Flux);
  const doFinallyCancel = await traceTypeScriptDoFinallyCancel(Flux);
  return {
    requestOneByOne,
    noRequest,
    lazyDefer,
    context: {
      basic: await Mono.deferContextual(context => Mono.just(`mono:${context.get("key")}`))
        .contextWrite(context => context.put("key", "root"))
        .block(),
      overrideOrder: await Mono.deferContextual(context => Mono.just(`override:${context.get("key")}`))
        .contextWrite(context => context.put("key", "inner"))
        .contextWrite(context => context.put("key", "outer"))
        .block(),
      flatMap: await Flux.just(1)
        .flatMap(value => Mono.deferContextual(context => Mono.just(`${value}:${context.get("key")}`)), 2)
        .contextWrite(context => context.put("key", "ctx"))
        .toArray(),
      nestedFlatMap: await Flux.just(1)
        .flatMap(value => Flux.just(value + 1)
          .flatMap(inner => Mono.deferContextual(context => Mono.just(`deep:${context.get("key")}:${inner}`)), 2), 2)
        .contextWrite(context => context.put("key", "ctx"))
        .toArray(),
      scheduler: await Mono.deferContextual(context => Mono.just(`sched:${context.get("key")}`))
        .subscribeOn(Schedulers.immediate())
        .publishOn(Schedulers.immediate())
        .contextWrite(context => context.put("key", "ctx"))
        .block(),
      monoFluxTransition: await Mono.deferContextual(context => Mono.just(`mono:${context.get("key")}`))
        .flux()
        .flatMap(value => Mono.deferContextual(context => Mono.just(`${value}:flux:${context.get("key")}`)), 2)
        .contextWrite(context => context.put("key", "ctx"))
        .toArray()
    },
    nestedFlatMapSchedulers,
    lifecyclePositions,
    doFinallyCancel,
    requestPatterns: {
      oneByOne: requestOneByOne,
      noRequest,
      batchTwo: await traceTypeScriptBatchRequests(Flux),
      cancelAfterFirst: await traceTypeScriptCancelAfterFirst(Flux)
    },
    lazyFactories: await traceTypeScriptLazyFactories(Flux, Mono),
    contextMatrix: await traceTypeScriptContextMatrix(Flux, Mono, Schedulers),
    flatMapNesting: await traceTypeScriptFlatMapNesting(Flux, Mono, Schedulers),
    lifecycleVariations: await traceTypeScriptLifecycleVariations(Flux),
    schedulerBoundaries: await traceTypeScriptSchedulerBoundaries(Flux, Mono, Schedulers),
    monoFluxTransitions: await traceTypeScriptMonoFluxTransitions(Flux, Mono, Schedulers),
    longLivedStreams: await traceTypeScriptLongLivedStreams(Flux, Mono, Schedulers)
  };
}

/** Traces one-by-one downstream demand and signal order. */
async function traceTypeScriptRequestOneByOne(Flux) {
  const events = [];
  let subscription;
  Flux.range(1, 3).subscribe({
    onSubscribe(nextSubscription) {
      events.push("onSubscribe");
      subscription = nextSubscription;
      events.push("request(1)");
      nextSubscription.request(1);
    },
    onNext(value) {
      events.push(`onNext(${value})`);
      if (value < 3) {
        events.push("request(1)");
        subscription.request(1);
      }
    },
    onError(error) {
      events.push(`onError(${String(error)})`);
    },
    onComplete() {
      events.push("onComplete");
    }
  });
  await waitForTrace(() => events.includes("onComplete"));
  return events;
}

/** Traces that no value is emitted before explicit request. */
async function traceTypeScriptNoRequest(Flux) {
  const events = [];
  Flux.range(1, 2).subscribe({
    onSubscribe() {
      events.push("onSubscribe");
    },
    onNext(value) {
      events.push(`onNext(${value})`);
    },
    onError(error) {
      events.push(`onError(${String(error)})`);
    },
    onComplete() {
      events.push("onComplete");
    }
  });
  await sleep(10);
  return events;
}

/** Traces batched downstream demand and completion after a second request. */
async function traceTypeScriptBatchRequests(Flux) {
  const events = [];
  let subscription;
  Flux.range(1, 4).subscribe({
    onSubscribe(nextSubscription) {
      events.push("onSubscribe");
      subscription = nextSubscription;
      events.push("request(2)");
      nextSubscription.request(2);
    },
    onNext(value) {
      events.push(`onNext(${value})`);
      if (value === 2) {
        events.push("request(2)");
        subscription.request(2);
      }
    },
    onError(error) {
      events.push(`onError(${String(error)})`);
    },
    onComplete() {
      events.push("onComplete");
    }
  });
  await waitForTrace(() => events.includes("onComplete"));
  return events;
}

/** Traces cancellation after the first requested value. */
async function traceTypeScriptCancelAfterFirst(Flux) {
  const events = [];
  let subscription;
  Flux.range(1, 4).subscribe({
    onSubscribe(nextSubscription) {
      events.push("onSubscribe");
      subscription = nextSubscription;
      events.push("request(1)");
      nextSubscription.request(1);
    },
    onNext(value) {
      events.push(`onNext(${value})`);
      events.push("cancel");
      subscription.cancel();
    },
    onError(error) {
      events.push(`onError(${String(error)})`);
    },
    onComplete() {
      events.push("onComplete");
    }
  });
  await sleep(10);
  return events;
}

/** Traces deferred source assembly separately from downstream demand. */
async function traceTypeScriptLazyDefer(Flux) {
  const events = ["assembled"];
  let subscription;
  const source = Flux.defer(() => {
    events.push("factory");
    return Flux.just(1).doOnSubscribe(() => events.push("innerOnSubscribe"));
  });
  source.subscribe({
    onSubscribe(nextSubscription) {
      events.push("onSubscribe");
      subscription = nextSubscription;
    },
    onNext(value) {
      events.push(`onNext(${value})`);
    },
    onError(error) {
      events.push(`onError(${String(error)})`);
    },
    onComplete() {
      events.push("onComplete");
    }
  });
  events.push("afterSubscribe");
  events.push("request(1)");
  subscription.request(1);
  await waitForTrace(() => events.includes("onComplete"));
  return events;
}

/** Traces static factory laziness, assembly capture and subscription-time callbacks. */
async function traceTypeScriptLazyFactories(Flux, Mono) {
  let value = 1;
  const fluxJust = Flux.just(value);
  const monoJust = Mono.just(value);
  value = 2;

  let fluxDeferCounter = 0;
  const fluxDefer = Flux.defer(() => Flux.just(++fluxDeferCounter));
  let monoDeferCounter = 0;
  const monoDefer = Mono.defer(() => Mono.just(++monoDeferCounter));
  let supplierCounter = 0;
  const monoSupplier = Mono.fromSupplier(() => ++supplierCounter);

  const createEvents = ["assembled"];
  const created = Flux.create(sink => {
    createEvents.push("callback");
    sink.next(1);
    sink.complete();
  });
  const createValues = await created.toArray();

  return {
    fluxJust: [await fluxJust.toArray(), await fluxJust.toArray()],
    monoJust: [await monoJust.block(), await monoJust.block()],
    fluxDefer: [await fluxDefer.toArray(), await fluxDefer.toArray()],
    monoDefer: [await monoDefer.block(), await monoDefer.block()],
    monoFromSupplier: [await monoSupplier.block(), await monoSupplier.block()],
    create: {
      events: createEvents,
      values: createValues
    }
  };
}

/** Traces context visibility through flattening, empty fallbacks, scheduler and type transitions. */
async function traceTypeScriptContextMatrix(Flux, Mono, Schedulers) {
  return {
    concatMap: await Flux.just(1, 2)
      .concatMap(value => Mono.deferContextual(context => Mono.just(`concat:${value}:${context.get("key")}`)))
      .contextWrite(context => context.put("key", "ctx"))
      .toArray(),
    switchMap: await Flux.just(1)
      .switchMap(value => Mono.deferContextual(context => Mono.just(`switch:${value}:${context.get("key")}`)))
      .contextWrite(context => context.put("key", "ctx"))
      .toArray(),
    switchIfEmpty: await Mono.empty()
      .switchIfEmpty(Mono.deferContextual(context => Mono.just(`fallback:${context.get("key")}`)))
      .contextWrite(context => context.put("key", "ctx"))
      .block(),
    innerOverride: await Flux.just(1)
      .flatMap(value => Mono.deferContextual(context => Mono.just(`inner:${value}:${context.get("key")}`))
        .contextWrite(context => context.put("key", "inner")), 1)
      .contextWrite(context => context.put("key", "outer"))
      .toArray(),
    thenMany: await Mono.just(0)
      .thenMany(Flux.deferContextual(context => Flux.just(`thenMany:${context.get("key")}`)))
      .contextWrite(context => context.put("key", "ctx"))
      .toArray(),
    fluxNext: await Flux.just(1, 2)
      .next()
      .flatMap(value => Mono.deferContextual(context => Mono.just(`next:${value}:${context.get("key")}`)))
      .contextWrite(context => context.put("key", "ctx"))
      .block(),
    nestedScheduler: await Mono.deferContextual(context => Mono.just(`outer:${context.get("key")}`))
      .flatMap(value => Mono.deferContextual(context => Mono.just(`${value}:inner:${context.get("key")}`))
        .publishOn(Schedulers.immediate())
        .subscribeOn(Schedulers.immediate()))
      .contextWrite(context => context.put("key", "ctx"))
      .block()
  };
}

/** Traces nested flatMap, inner and outer scheduler boundaries and lifecycle hooks. */
async function traceTypeScriptNestedFlatMapSchedulers(Flux, Mono, Schedulers) {
  const events = [];
  const values = await Flux.just(1, 2)
    .doFirst(() => events.push("outer.doFirst"))
    .subscribeOn(Schedulers.immediate())
    .publishOn(Schedulers.immediate())
    .flatMap(value => Mono.just(value)
      .doFirst(() => events.push(`inner${value}.doFirst`))
      .subscribeOn(Schedulers.immediate())
      .publishOn(Schedulers.immediate())
      .doOnNext(next => events.push(`inner${next}.doOnNext`))
      .doFinally(signal => events.push(`inner${value}.doFinally(${signal})`)), 1)
    .doOnNext(value => events.push(`outer.doOnNext(${value})`))
    .doFinally(signal => events.push(`outer.doFinally(${signal})`))
    .toArray();
  return { events, values };
}

/** Traces deeper flattening combinations, inner fallbacks and deterministic concurrency. */
async function traceTypeScriptFlatMapNesting(Flux, Mono, Schedulers) {
  const nestedEvents = [];
  const nestedValues = await Flux.just(1, 2)
    .flatMap(value => Mono.just(value)
      .doFirst(() => nestedEvents.push(`outerInner${value}.doFirst`))
      .flatMapMany(inner => Flux.just(inner, inner + 10)
        .publishOn(Schedulers.immediate())
        .doOnNext(next => nestedEvents.push(`deep${value}.doOnNext(${next})`)))
      .doFinally(signal => nestedEvents.push(`outerInner${value}.doFinally(${signal})`)), 1)
    .doOnNext(value => nestedEvents.push(`outer.doOnNext(${value})`))
    .toArray();

  return {
    threeLevelConcatMap: await Flux.just(1, 2)
      .concatMap(value => Flux.just(value, value + 10)
        .concatMap(inner => Mono.just(inner + 100)))
      .toArray(),
    flatMapConcurrencyOne: await Flux.just(1, 2)
      .flatMap(value => Flux.just(value, value + 10), 1)
      .toArray(),
    flatMapInnerErrorResume: await Flux.just(1, 2)
      .flatMap(value => (value === 1
        ? Flux.error(new Error("boom")).onErrorResume(() => Flux.just(10))
        : Flux.just(20)), 1)
      .toArray(),
    switchMapSingleOuter: await Flux.just(1)
      .switchMap(value => Flux.just(value, value + 10))
      .toArray(),
    nestedLifecycle: {
      events: nestedEvents,
      values: nestedValues
    }
  };
}

/** Traces doFirst, doOnNext and doFinally at different chain positions. */
async function traceTypeScriptLifecyclePositions(Flux) {
  const events = [];
  const values = await Flux.just(1)
    .doFirst(() => events.push("firstA"))
    .doFirst(() => events.push("firstB"))
    .doOnNext(value => events.push(`nextBeforeMap(${value})`))
    .map(value => {
      events.push(`map(${value})`);
      return value + 1;
    })
    .doOnNext(value => events.push(`nextAfterMap(${value})`))
    .doFinally(signal => events.push(`finallyAfterMap(${signal})`))
    .toArray();
  return { events, values };
}

/** Traces completion, error and cancellation lifecycle hook variations. */
async function traceTypeScriptLifecycleVariations(Flux) {
  const completeEvents = [];
  const completeValues = await Flux.just(1)
    .doFirst(() => completeEvents.push("firstA"))
    .doFirst(() => completeEvents.push("firstB"))
    .doOnNext(value => completeEvents.push(`nextBeforeMap(${value})`))
    .map(value => {
      completeEvents.push(`map(${value})`);
      return value + 1;
    })
    .doOnNext(value => completeEvents.push(`nextAfterMap(${value})`))
    .doOnComplete(() => completeEvents.push("complete"))
    .doFinally(signal => completeEvents.push(`finally(${signal})`))
    .toArray();

  const afterTerminateEvents = [];
  const afterTerminateValues = await Flux.just(1)
    .doOnComplete(() => afterTerminateEvents.push("complete"))
    .doAfterTerminate(() => afterTerminateEvents.push("afterTerminate"))
    .toArray();

  const errorEvents = [];
  const errorValues = await Flux.concat(Flux.just(1), Flux.error(new Error("boom")))
    .doOnNext(value => errorEvents.push(`next(${value})`))
    .doOnError(() => errorEvents.push("error"))
    .doFinally(signal => errorEvents.push(`finally(${signal})`))
    .onErrorReturn(9)
    .toArray();

  const cancelEvents = [];
  const cancelValues = await Flux.range(1, 3)
    .doOnCancel(() => cancelEvents.push("cancel"))
    .doFinally(signal => cancelEvents.push(`upstreamFinally(${signal})`))
    .take(1)
    .doFinally(signal => cancelEvents.push(`downstreamFinally(${signal})`))
    .toArray();

  return {
    complete: {
      events: completeEvents,
      values: completeValues
    },
    afterTerminate: {
      events: afterTerminateEvents,
      values: afterTerminateValues
    },
    errorResume: {
      events: errorEvents,
      values: errorValues
    },
    cancelTake: {
      events: cancelEvents,
      values: cancelValues
    }
  };
}

/** Traces upstream cancellation and downstream completion around `take`. */
async function traceTypeScriptDoFinallyCancel(Flux) {
  const events = [];
  const values = await Flux.range(1, 3)
    .doFinally(signal => events.push(`upstreamFinally(${signal})`))
    .take(1)
    .doFinally(signal => events.push(`downstreamFinally(${signal})`))
    .toArray();
  return { events, values };
}

/** Traces scheduler boundaries without relying on browser timing races. */
async function traceTypeScriptSchedulerBoundaries(Flux, Mono, Schedulers) {
  const publishEvents = [];
  const publishValues = await Flux.just(1, 2)
    .doOnNext(value => publishEvents.push(`beforePublishOn(${value})`))
    .publishOn(Schedulers.immediate())
    .doOnNext(value => publishEvents.push(`afterPublishOn(${value})`))
    .toArray();

  const nestedEvents = [];
  const nestedValue = await Mono.just(1)
    .doFirst(() => nestedEvents.push("outer.doFirst"))
    .subscribeOn(Schedulers.immediate())
    .flatMap(value => Mono.just(value + 1)
      .doFirst(() => nestedEvents.push("inner.doFirst"))
      .publishOn(Schedulers.immediate())
      .subscribeOn(Schedulers.immediate())
      .doOnNext(next => nestedEvents.push(`inner.doOnNext(${next})`)))
    .doOnNext(value => nestedEvents.push(`outer.doOnNext(${value})`))
    .block();

  return {
    publishOnOrder: {
      events: publishEvents,
      values: publishValues
    },
    nestedSubscribeOnPublishOn: {
      events: nestedEvents,
      value: nestedValue
    }
  };
}

/** Traces context and values across Mono/Flux conversion boundaries. */
async function traceTypeScriptMonoFluxTransitions(Flux, Mono, Schedulers) {
  return {
    monoFluxMono: await Mono.deferContextual(context => Mono.just(`mono:${context.get("key")}`))
      .flux()
      .next()
      .flatMap(value => Mono.deferContextual(context => Mono.just(`${value}:mono:${context.get("key")}`)))
      .contextWrite(context => context.put("key", "ctx"))
      .block(),
    monoFlatMapMany: await Mono.just(1)
      .flatMapMany(value => Flux.deferContextual(context => Flux.just(`many:${value}:${context.get("key")}`)))
      .contextWrite(context => context.put("key", "ctx"))
      .toArray(),
    fluxNextFlatMap: await Flux.just(1, 2)
      .next()
      .publishOn(Schedulers.immediate())
      .flatMap(value => Mono.deferContextual(context => Mono.just(`flat:${value}:${context.get("key")}`)))
      .contextWrite(context => context.put("key", "ctx"))
      .block(),
    thenManyRoundTrip: await Mono.just(1)
      .thenMany(Flux.just(2, 3))
      .next()
      .flatMap(value => Mono.just(value + 10))
      .block()
  };
}

/** Traces delayed, interval and never-backed sources through multi-source operators. */
async function traceTypeScriptLongLivedStreams(Flux, Mono, Schedulers) {
  const delayedFlux = (...values) => Flux.defer(() => Flux.from((async function* () {
    for (const value of values) {
      await sleep(1);
      yield value;
    }
  })()));
  const sorted = source => source.collectList().map(values => values.map(Number).sort((left, right) => left - right));

  return {
    delayedCollectList: await delayedFlux(1, 2).collectList().block(),
    delayedConcatWith: await delayedFlux(1, 2).concatWith(delayedFlux(9)).toArray(),
    delayedMergeWithSorted: await sorted(delayedFlux(2).mergeWith(delayedFlux(1))).block(),
    intervalTakeCollectList: await Flux.interval(1, Schedulers.timeout()).take(3).collectList().block(),
    neverTimeoutFallback: await Flux.never().timeout(1, Flux.just(7), Schedulers.timeout()).collectList().block(),
    nestedDelayedConcatMap: await delayedFlux(1, 2)
      .concatMap(value => delayedFlux(value, Number(value) + 10))
      .collectList()
      .block(),
    nestedDelayedFlatMapSorted: await sorted(delayedFlux(1)
      .mergeWith(delayedFlux(2).flatMap(value => delayedFlux(value, Number(value) + 10), 1))).block(),
    monoThenManyDelayed: await Mono.delay(1, Schedulers.timeout())
      .thenMany(delayedFlux(3, 4))
      .collectList()
      .block()
  };
}

/** Waits until a trace predicate becomes true. */
async function waitForTrace(predicate) {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started > 500) {
      throw new Error("Timed out while waiting for trace completion");
    }
    await sleep(0);
  }
}

/** Resolves after a small timeout. */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/** Creates a generated Flux with deterministic finite output. */
function generatedFlux(Flux) {
  let emitted = false;
  return Flux.generate(sink => {
    if (emitted) {
      sink.complete();
    } else {
      emitted = true;
      sink.next(1);
    }
  });
}

/** Creates a Flux factory that proves defer is lazy across subscriptions. */
function counterFlux(Flux) {
  let counter = 0;
  return () => Flux.defer(() => Flux.just(++counter));
}

/** Creates a Mono factory that proves defer is lazy across subscriptions. */
function counterMono(Mono) {
  let counter = 0;
  return () => Mono.defer(() => Mono.just(++counter));
}

/** Creates a Mono factory that proves fromSupplier is lazy across subscriptions. */
function counterSupplierMono(Mono) {
  let counter = 0;
  return () => Mono.fromSupplier(() => ++counter);
}

/** Collects a publisher twice and joins the two JSON-normalized results. */
async function doubleCollect(factory) {
  return `${JSON.stringify(await collectOracle(factory()))}|${JSON.stringify(await collectOracle(factory()))}`;
}

/** Blocks a Mono twice and joins the two JSON-normalized results. */
async function doubleBlock(factory) {
  return `${JSON.stringify(await collectOracle(factory()))}|${JSON.stringify(await collectOracle(factory()))}`;
}

/** Summarizes a method matrix with count, deterministic hash and selected sample entries. */
async function summarizeMatrix(label, leftCases, rightCases, apply) {
  const entries = [];
  for (const [leftName, left] of leftCases) {
    for (const [rightName, right] of rightCases) {
      const name = `${leftName}->${rightName}`;
      entries.push(`${name}=${JSON.stringify(await collectOracle(apply(left, right)))}`);
    }
  }
  return {
    count: entries.length,
    hash: fnv1a(entries.join("\n")),
    first: entries.slice(0, 5),
    last: entries.slice(-5),
    entries
  };
}

/** Converts a JavaScript oracle result into a finite JSON-compatible value. */
async function collectOracle(value) {
  try {
    if (value && typeof value.block === "function") {
      const result = await withTimeout(value.block(), 500);
      return normalizeOracleValue(result ?? null);
    }
    if (value && typeof value.toArray === "function") {
      return normalizeOracleValue(await withTimeout(value.take ? value.take(8).toArray() : value.toArray(), 500));
    }
    if (Array.isArray(value)) {
      return normalizeOracleValue(value);
    }
    return normalizeOracleValue(value ?? null);
  } catch {
    return "ERROR";
  }
}

/** Normalizes publisher values into JSON-compatible scalar and array shapes. */
function normalizeOracleValue(value) {
  if (value === undefined || value === null) {
    return null;
  }
  if (Array.isArray(value)) {
    return value.map(normalizeOracleValue);
  }
  if (typeof value === "number" || typeof value === "string" || typeof value === "boolean") {
    return value;
  }
  return String(value);
}

/** Converts an arbitrary oracle result to Flux. */
function toFluxOracle(value, Flux) {
  if (value && typeof value.toArray === "function") {
    return value;
  }
  if (value && typeof value.block === "function") {
    return value.flux();
  }
  return Flux.just(1, 2);
}

/** Converts an arbitrary oracle result to Mono. */
function toMonoOracle(value, Mono) {
  if (value && typeof value.block === "function") {
    return value;
  }
  if (value && typeof value.next === "function") {
    return value.next();
  }
  return Mono.just(1);
}

/** Returns a deterministic FNV-1a hash for compact matrix fixture storage. */
function fnv1a(input) {
  let hash = 0x811c9dc5;
  for (let index = 0; index < input.length; index += 1) {
    hash ^= input.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash.toString(16).padStart(8, "0");
}

/** Runs sink scenarios against the TypeScript package. */
async function runTypeScriptSinkOracle(Sinks, Flux) {
  const oneValue = Sinks.one();
  const oneEmpty = Sinks.one();
  const oneError = Sinks.one();
  const emptyComplete = Sinks.empty();
  const emitAfterTerminated = Sinks.one();
  const unicastBuffer = Sinks.many().unicast().onBackpressureBuffer();
  const unicastError = Sinks.many().unicast().onBackpressureError();
  const multicastBuffer = Sinks.many().multicast().onBackpressureBuffer();
  const directAllOrNothing = Sinks.many().multicast().directAllOrNothing();
  const directBestEffort = Sinks.many().multicast().directBestEffort();
  const replayAll = Sinks.many().replay().all();
  const replayLimit = Sinks.many().replay().limit(2);
  const replayLatest = Sinks.many().replay().latest();
  const replayLatestDefault = Sinks.many().replay().latestOrDefault(9);
  const manyWithUpstream = Sinks.unsafe().manyWithUpstream().multicastOnBackpressureBuffer();
  const oneSubscriberCount = Sinks.one();
  const manySubscriberCount = Sinks.many().multicast().onBackpressureBuffer();

  const emitFailureReason = captureEmissionReason(() => {
    emitAfterTerminated.emitValue(1);
    emitAfterTerminated.emitValue(2);
  });

  const oneSubscription = oneSubscriberCount.asMono().subscribe(() => undefined);
  const oneCountBeforeDispose = oneSubscriberCount.currentSubscriberCount();
  oneSubscription.dispose();

  const manySubscription = manySubscriberCount.asFlux().subscribe(() => undefined);
  const manyCountBeforeDispose = manySubscriberCount.currentSubscriberCount();
  manySubscription.dispose();

  return {
    "sinks-one-value-late": [
      oneValue.tryEmitValue(1),
      oneValue.tryEmitValue(2),
      await oneValue.asMono().block()
    ],
    "sinks-one-empty-late": [
      oneEmpty.tryEmitEmpty(),
      oneEmpty.tryEmitValue(1),
      await oneEmpty.asMono().defaultIfEmpty(-1).block()
    ],
    "sinks-one-error-late": [
      oneError.tryEmitError(new Error("boom")),
      oneError.tryEmitValue(1),
      await oneError.asMono().onErrorReturn(-1).block()
    ],
    "sinks-empty-complete-late": [
      emptyComplete.tryEmitEmpty(),
      emptyComplete.tryEmitError(new Error("boom")),
      await emptyComplete.asMono().defaultIfEmpty(-1).block()
    ],
    "sinks-emit-after-terminated": emitFailureReason,
    "sinks-unicast-buffer-late": [
      unicastBuffer.tryEmitNext(1),
      unicastBuffer.tryEmitNext(2),
      unicastBuffer.tryEmitComplete(),
      await collectFlux(unicastBuffer.asFlux())
    ],
    "sinks-unicast-error-zero-subscriber": [
      unicastError.tryEmitNext(1),
      unicastError.tryEmitComplete(),
      await collectFlux(unicastError.asFlux())
    ],
    "sinks-multicast-buffer-warmup": [
      multicastBuffer.tryEmitNext(1),
      multicastBuffer.tryEmitNext(2),
      multicastBuffer.tryEmitComplete(),
      await collectFlux(multicastBuffer.asFlux())
    ],
    "sinks-multicast-direct-zero-subscriber": [
      directAllOrNothing.tryEmitNext(1),
      directBestEffort.tryEmitNext(1)
    ],
    "sinks-replay-all-late": [
      replayAll.tryEmitNext(1),
      replayAll.tryEmitNext(2),
      replayAll.tryEmitComplete(),
      await collectFlux(replayAll.asFlux())
    ],
    "sinks-replay-limit-late": [
      replayLimit.tryEmitNext(1),
      replayLimit.tryEmitNext(2),
      replayLimit.tryEmitNext(3),
      replayLimit.tryEmitComplete(),
      await collectFlux(replayLimit.asFlux())
    ],
    "sinks-replay-latest-late": [
      replayLatest.tryEmitNext(1),
      replayLatest.tryEmitNext(2),
      replayLatest.tryEmitComplete(),
      await collectFlux(replayLatest.asFlux())
    ],
    "sinks-replay-latest-or-default": await collectFlux(replayLatestDefault.asFlux().take(1)),
    "sinks-many-with-upstream": [
      subscribeTo(manyWithUpstream, Flux.just(1, 2)),
      await collectFlux(manyWithUpstream.asFlux())
    ],
    "sinks-one-subscriber-count": [
      oneCountBeforeDispose,
      oneSubscriberCount.currentSubscriberCount()
    ],
    "sinks-many-subscriber-count": [
      manyCountBeforeDispose,
      manySubscriberCount.currentSubscriberCount()
    ]
  };
}

/** Subscribes a sink to upstream and returns a stable JSON marker. */
function subscribeTo(sink, source) {
  sink.subscribeTo(source);
  return "SUBSCRIBED";
}

/** Collects a Flux with the same timeout marker used by the Java oracle. */
async function collectFlux(flux) {
  try {
    return await withTimeout(flux.toArray(), 100);
  } catch {
    return "TIMEOUT_OR_ERROR:IllegalStateException";
  }
}

/** Resolves a promise or rejects after a fixed timeout. */
function withTimeout(promise, ms) {
  return new Promise((resolvePromise, rejectPromise) => {
    const timeout = setTimeout(() => rejectPromise(new Error("timeout")), ms);
    promise.then(
      value => {
        clearTimeout(timeout);
        resolvePromise(value);
      },
      error => {
        clearTimeout(timeout);
        rejectPromise(error);
      }
    );
  });
}

/** Captures the sink emission failure reason from throwing emit APIs. */
function captureEmissionReason(callback) {
  try {
    callback();
    return "NO_ERROR";
  } catch (error) {
    return error?.reason ?? error?.message ?? String(error);
  }
}

/** Returns the Java source used as the oracle harness. */
function javaSource() {
  return String.raw`
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import reactor.core.Disposable;

public class ReactorOracleMatrix {
  public static void main(String[] args) {
    LinkedHashMap<String, Object> out = new LinkedHashMap<>();
    out.put("flux-map-filter", Flux.range(1, 5).filter(value -> value % 2 == 1).map(value -> value * 10).collectList().block());
    out.put("flux-concat-map", Flux.just(1, 2).concatMap(value -> Flux.just(value, value + 10)).collectList().block());
    out.put("flux-flat-map", Flux.just(1, 2).flatMap(value -> Mono.delay(Duration.ofMillis(value == 1 ? 20 : 0), Schedulers.boundedElastic()).map(tick -> value), 2).collectList().block());
    out.put("flux-distinct", Flux.just("a", "bb", "cc", "ddd").distinct(value -> value.length()).collectList().block());
    out.put("flux-distinct-until-changed", Flux.just("a", "b", "bb", "cc", "d").distinctUntilChanged(value -> value.length()).collectList().block());
    out.put("flux-zip", Flux.zip(Flux.just(1, 2), Flux.just("a", "b")).map(tuple -> Arrays.asList(tuple.getT1(), tuple.getT2())).collectList().block());
    out.put("flux-switch-empty", Flux.<Integer>empty().switchIfEmpty(Flux.just(7)).collectList().block());
    out.put("flux-error-resume", Flux.concat(Flux.just(1), Flux.<Integer>error(new RuntimeException("boom"))).onErrorResume(error -> Flux.just(9)).collectList().block());
    out.put("flux-buffer", Flux.range(1, 5).buffer(2).collectList().block());
    out.put("flux-window", Flux.range(1, 5).window(2).concatMap(window -> window.collectList()).collectList().block());
    out.put("mono-flat-map", Mono.just(2).map(value -> value + 1).flatMap(value -> Mono.just(value * 3)).block());
    out.put("mono-then-many", Mono.just(1).thenMany(Flux.just(2, 3)).collectList().block());
    out.put("mono-zip", Mono.zip(Mono.just(1), Mono.just("a")).map(tuple -> Arrays.asList(tuple.getT1(), tuple.getT2())).block());
    out.put("scheduler-delay", Mono.delay(Duration.ZERO, Schedulers.boundedElastic()).block());
    out.put("method-matrix", methodMatrix());
    sinkScenarios(out);
    System.out.println(json(out));
  }

  static LinkedHashMap<String, Object> methodMatrix() {
    ArrayList<Case<Supplier<Flux<Integer>>>> fluxStatic = new ArrayList<>();
    fluxStatic.add(new Case<>("just", () -> Flux.just(1, 2)));
    fluxStatic.add(new Case<>("defer", () -> Flux.defer(() -> Flux.just(1, 2))));
    fluxStatic.add(new Case<>("fromIterable", () -> Flux.fromIterable(Arrays.asList(1, 2))));
    fluxStatic.add(new Case<>("range", () -> Flux.range(1, 2)));
    fluxStatic.add(new Case<>("empty", () -> Flux.empty()));
    fluxStatic.add(new Case<>("error", () -> Flux.error(new RuntimeException("boom"))));
    fluxStatic.add(new Case<>("concat", () -> Flux.concat(Flux.just(1), Flux.just(2))));
    fluxStatic.add(new Case<>("zip", () -> Flux.zip(Flux.just(1), Flux.just(2)).map(tuple -> tuple.getT1() + tuple.getT2())));
    fluxStatic.add(new Case<>("create", () -> Flux.create(sink -> { sink.next(1); sink.next(2); sink.complete(); })));
    fluxStatic.add(new Case<>("generate", () -> generatedFlux()));

    ArrayList<Case<Supplier<Mono<Integer>>>> monoStatic = new ArrayList<>();
    monoStatic.add(new Case<>("just", () -> Mono.just(1)));
    monoStatic.add(new Case<>("defer", () -> Mono.defer(() -> Mono.just(1))));
    monoStatic.add(new Case<>("empty", () -> Mono.empty()));
    monoStatic.add(new Case<>("error", () -> Mono.error(new RuntimeException("boom"))));
    monoStatic.add(new Case<>("fromSupplier", () -> Mono.fromSupplier(() -> 1)));
    monoStatic.add(new Case<>("fromCallable", () -> Mono.fromCallable(() -> 1)));
    monoStatic.add(new Case<>("fromRunnable", () -> Mono.<Integer>fromRunnable(() -> {})));
    monoStatic.add(new Case<>("justOrEmpty", () -> Mono.justOrEmpty(1)));
    monoStatic.add(new Case<>("zip", () -> Mono.zip(Mono.just(1), Mono.just(2)).map(tuple -> tuple.getT1() + tuple.getT2())));
    monoStatic.add(new Case<>("delay", () -> Mono.delay(Duration.ZERO, Schedulers.boundedElastic()).map(Long::intValue)));

    ArrayList<Case<Function<Flux<Integer>, Object>>> fluxOps = new ArrayList<>();
    fluxOps.add(new Case<>("identity", source -> source));
    fluxOps.add(new Case<>("map", source -> source.map(value -> value + 1)));
    fluxOps.add(new Case<>("filter", source -> source.filter(value -> value % 2 == 1)));
    fluxOps.add(new Case<>("take", source -> source.take(1)));
    fluxOps.add(new Case<>("skip", source -> source.skip(1)));
    fluxOps.add(new Case<>("defaultIfEmpty", source -> source.defaultIfEmpty(9)));
    fluxOps.add(new Case<>("switchIfEmpty", source -> source.switchIfEmpty(Flux.just(7))));
    fluxOps.add(new Case<>("concatMap", source -> source.concatMap(value -> Flux.just(value, value + 10))));
    fluxOps.add(new Case<>("flatMap", source -> source.flatMap(value -> Flux.just(value + 10), 1)));
    fluxOps.add(new Case<>("distinct", source -> source.distinct(value -> value % 2)));
    fluxOps.add(new Case<>("distinctUntilChanged", source -> source.distinctUntilChanged(value -> value % 2)));
    fluxOps.add(new Case<>("buffer", source -> source.buffer(2)));
    fluxOps.add(new Case<>("collectList", source -> source.collectList()));
    fluxOps.add(new Case<>("count", source -> source.count()));
    fluxOps.add(new Case<>("next", source -> source.next()));
    fluxOps.add(new Case<>("reduce", source -> source.reduce((left, right) -> left + right)));
    fluxOps.add(new Case<>("scan", source -> source.scan((left, right) -> left + right)));
    fluxOps.add(new Case<>("startWith", source -> source.startWith(0)));
    fluxOps.add(new Case<>("concatWith", source -> source.concatWith(Flux.just(9))));
    fluxOps.add(new Case<>("zipWith", source -> source.zipWith(Flux.just(9)).map(tuple -> tuple.getT1() + tuple.getT2())));
    fluxOps.add(new Case<>("onErrorReturn", source -> source.onErrorReturn(-1)));
    fluxOps.add(new Case<>("onErrorResume", source -> source.onErrorResume(error -> Flux.just(-2))));

    ArrayList<Case<Function<Mono<Integer>, Object>>> monoOps = new ArrayList<>();
    monoOps.add(new Case<>("identity", source -> source));
    monoOps.add(new Case<>("map", source -> source.map(value -> value + 1)));
    monoOps.add(new Case<>("filter", source -> source.filter(value -> value % 2 == 1)));
    monoOps.add(new Case<>("defaultIfEmpty", source -> source.defaultIfEmpty(9)));
    monoOps.add(new Case<>("switchIfEmpty", source -> source.switchIfEmpty(Mono.just(7))));
    monoOps.add(new Case<>("flatMap", source -> source.flatMap(value -> Mono.just(value + 10))));
    monoOps.add(new Case<>("thenReturn", source -> source.thenReturn(5)));
    monoOps.add(new Case<>("thenMany", source -> source.thenMany(Flux.just(1, 2))));
    monoOps.add(new Case<>("zipWith", source -> source.zipWith(Mono.just(9)).map(tuple -> tuple.getT1() + tuple.getT2())));
    monoOps.add(new Case<>("onErrorReturn", source -> source.onErrorReturn(-1)));
    monoOps.add(new Case<>("onErrorResume", source -> source.onErrorResume(error -> Mono.just(-2))));
    monoOps.add(new Case<>("flux", source -> source.flux()));
    ArrayList<Case<Function<Flux<Integer>, Object>>> fluxChainOps = new ArrayList<>();
    for (Case<Function<Flux<Integer>, Object>> next : fluxOps) {
      if (!Arrays.asList("buffer", "collectList", "count", "next", "reduce").contains(next.name)) {
        fluxChainOps.add(next);
      }
    }
    ArrayList<Case<Function<Mono<Integer>, Object>>> monoChainOps = new ArrayList<>();
    for (Case<Function<Mono<Integer>, Object>> next : monoOps) {
      if (!Arrays.asList("thenMany", "flux").contains(next.name)) {
        monoChainOps.add(next);
      }
    }

    LinkedHashMap<String, Object> out = new LinkedHashMap<>();
    LinkedHashMap<String, Object> laziness = new LinkedHashMap<>();
    laziness.put("fluxJust", doubleCollect(() -> Flux.just(1)));
    laziness.put("fluxDefer", doubleCollect(counterFlux()));
    laziness.put("monoJust", doubleCollect(() -> Mono.just(1)));
    laziness.put("monoDefer", doubleCollect(counterMono()));
    laziness.put("monoFromSupplier", doubleCollect(counterSupplierMono()));
    out.put("staticLaziness", laziness);
    out.put("traces", traceMatrix());

    LinkedHashMap<String, Object> sections = new LinkedHashMap<>();
    sections.put("fluxStaticStandalone", summarizeMatrix(fluxStatic, singleFluxCollector(), (factory, op) -> op.apply(factory.get())));
    sections.put("monoStaticStandalone", summarizeMatrix(monoStatic, singleMonoCollector(), (factory, op) -> op.apply(factory.get())));
    sections.put("fluxStaticToFlux", summarizeMatrix(fluxStatic, fluxOps, (factory, op) -> op.apply(factory.get())));
    sections.put("monoStaticToMono", summarizeMatrix(monoStatic, monoOps, (factory, op) -> op.apply(factory.get())));
    sections.put("fluxBeforeAfter", summarizeMatrix(fluxChainOps, fluxOps, (before, after) -> after.apply(toFluxOracle(before.apply(Flux.just(1, 2))))));
    sections.put("monoBeforeAfter", summarizeMatrix(monoChainOps, monoOps, (before, after) -> after.apply(toMonoOracle(before.apply(Mono.just(1))))));
    out.put("sections", sections);
    return out;
  }

  static LinkedHashMap<String, Object> traceMatrix() {
    LinkedHashMap<String, Object> out = new LinkedHashMap<>();
    out.put("requestOneByOne", traceRequestOneByOne());
    out.put("noRequest", traceNoRequest());
    out.put("lazyDefer", traceLazyDefer());
    out.put("context", traceContext());
    out.put("nestedFlatMapSchedulers", traceNestedFlatMapSchedulers());
    out.put("lifecyclePositions", traceLifecyclePositions());
    out.put("doFinallyCancel", traceDoFinallyCancel());
    LinkedHashMap<String, Object> requestPatterns = new LinkedHashMap<>();
    requestPatterns.put("oneByOne", traceRequestOneByOne());
    requestPatterns.put("noRequest", traceNoRequest());
    requestPatterns.put("batchTwo", traceBatchRequests());
    requestPatterns.put("cancelAfterFirst", traceCancelAfterFirst());
    out.put("requestPatterns", requestPatterns);
    out.put("lazyFactories", traceLazyFactories());
    out.put("contextMatrix", traceContextMatrix());
    out.put("flatMapNesting", traceFlatMapNesting());
    out.put("lifecycleVariations", traceLifecycleVariations());
    out.put("schedulerBoundaries", traceSchedulerBoundaries());
    out.put("monoFluxTransitions", traceMonoFluxTransitions());
    out.put("longLivedStreams", traceLongLivedStreams());
    return out;
  }

  static ArrayList<String> traceRequestOneByOne() {
    ArrayList<String> events = new ArrayList<>();
    Flux.range(1, 3).subscribe(new Subscriber<Integer>() {
      Subscription subscription;

      public void onSubscribe(Subscription nextSubscription) {
        events.add("onSubscribe");
        subscription = nextSubscription;
        events.add("request(1)");
        nextSubscription.request(1);
      }

      public void onNext(Integer value) {
        events.add("onNext(" + value + ")");
        if (value < 3) {
          events.add("request(1)");
          subscription.request(1);
        }
      }

      public void onError(Throwable error) {
        events.add("onError(" + error + ")");
      }

      public void onComplete() {
        events.add("onComplete");
      }
    });
    return events;
  }

  static ArrayList<String> traceNoRequest() {
    ArrayList<String> events = new ArrayList<>();
    Flux.range(1, 2).subscribe(new Subscriber<Integer>() {
      public void onSubscribe(Subscription subscription) {
        events.add("onSubscribe");
      }

      public void onNext(Integer value) {
        events.add("onNext(" + value + ")");
      }

      public void onError(Throwable error) {
        events.add("onError(" + error + ")");
      }

      public void onComplete() {
        events.add("onComplete");
      }
    });
    return events;
  }

  static ArrayList<String> traceBatchRequests() {
    ArrayList<String> events = new ArrayList<>();
    Flux.range(1, 4).subscribe(new Subscriber<Integer>() {
      Subscription subscription;

      public void onSubscribe(Subscription nextSubscription) {
        events.add("onSubscribe");
        subscription = nextSubscription;
        events.add("request(2)");
        nextSubscription.request(2);
      }

      public void onNext(Integer value) {
        events.add("onNext(" + value + ")");
        if (value == 2) {
          events.add("request(2)");
          subscription.request(2);
        }
      }

      public void onError(Throwable error) {
        events.add("onError(" + error + ")");
      }

      public void onComplete() {
        events.add("onComplete");
      }
    });
    return events;
  }

  static ArrayList<String> traceCancelAfterFirst() {
    ArrayList<String> events = new ArrayList<>();
    Flux.range(1, 4).subscribe(new Subscriber<Integer>() {
      Subscription subscription;

      public void onSubscribe(Subscription nextSubscription) {
        events.add("onSubscribe");
        subscription = nextSubscription;
        events.add("request(1)");
        nextSubscription.request(1);
      }

      public void onNext(Integer value) {
        events.add("onNext(" + value + ")");
        events.add("cancel");
        subscription.cancel();
      }

      public void onError(Throwable error) {
        events.add("onError(" + error + ")");
      }

      public void onComplete() {
        events.add("onComplete");
      }
    });
    return events;
  }

  static ArrayList<String> traceLazyDefer() {
    ArrayList<String> events = new ArrayList<>();
    Subscription[] subscription = new Subscription[1];
    Flux<Integer> source = Flux.defer(() -> {
      events.add("factory");
      return Flux.just(1).doOnSubscribe(ignored -> events.add("innerOnSubscribe"));
    });

    events.add("assembled");
    source.subscribe(new Subscriber<Integer>() {
      public void onSubscribe(Subscription nextSubscription) {
        events.add("onSubscribe");
        subscription[0] = nextSubscription;
      }

      public void onNext(Integer value) {
        events.add("onNext(" + value + ")");
      }

      public void onError(Throwable error) {
        events.add("onError(" + error + ")");
      }

      public void onComplete() {
        events.add("onComplete");
      }
    });
    events.add("afterSubscribe");
    events.add("request(1)");
    subscription[0].request(1);
    return events;
  }

  static LinkedHashMap<String, Object> traceLazyFactories() {
    LinkedHashMap<String, Object> out = new LinkedHashMap<>();
    int[] value = new int[] { 1 };
    Flux<Integer> fluxJust = Flux.just(value[0]);
    Mono<Integer> monoJust = Mono.just(value[0]);
    value[0] = 2;

    AtomicInteger fluxDeferCounter = new AtomicInteger();
    Flux<Integer> fluxDefer = Flux.defer(() -> Flux.just(fluxDeferCounter.incrementAndGet()));
    AtomicInteger monoDeferCounter = new AtomicInteger();
    Mono<Integer> monoDefer = Mono.defer(() -> Mono.just(monoDeferCounter.incrementAndGet()));
    AtomicInteger supplierCounter = new AtomicInteger();
    Mono<Integer> monoSupplier = Mono.fromSupplier(() -> supplierCounter.incrementAndGet());

    ArrayList<String> createEvents = new ArrayList<>();
    createEvents.add("assembled");
    Flux<Integer> created = Flux.create(sink -> {
      createEvents.add("callback");
      sink.next(1);
      sink.complete();
    });
    List<Integer> createValues = created.collectList().block();

    out.put("fluxJust", Arrays.asList(fluxJust.collectList().block(), fluxJust.collectList().block()));
    out.put("monoJust", Arrays.asList(monoJust.block(), monoJust.block()));
    out.put("fluxDefer", Arrays.asList(fluxDefer.collectList().block(), fluxDefer.collectList().block()));
    out.put("monoDefer", Arrays.asList(monoDefer.block(), monoDefer.block()));
    out.put("monoFromSupplier", Arrays.asList(monoSupplier.block(), monoSupplier.block()));
    LinkedHashMap<String, Object> create = new LinkedHashMap<>();
    create.put("events", createEvents);
    create.put("values", createValues);
    out.put("create", create);
    return out;
  }

  static LinkedHashMap<String, Object> traceContext() {
    LinkedHashMap<String, Object> out = new LinkedHashMap<>();
    out.put("basic", Mono.deferContextual(context -> Mono.just("mono:" + context.get("key")))
      .contextWrite(context -> context.put("key", "root"))
      .block());
    out.put("overrideOrder", Mono.deferContextual(context -> Mono.just("override:" + context.get("key")))
      .contextWrite(context -> context.put("key", "inner"))
      .contextWrite(context -> context.put("key", "outer"))
      .block());
    out.put("flatMap", Flux.just(1)
      .flatMap(value -> Mono.deferContextual(context -> Mono.just(value + ":" + context.get("key"))), 2)
      .contextWrite(Context.of("key", "ctx"))
      .collectList()
      .block());
    out.put("nestedFlatMap", Flux.just(1)
      .flatMap(value -> Flux.just(value + 1)
        .flatMap(inner -> Mono.deferContextual(context -> Mono.just("deep:" + context.get("key") + ":" + inner)), 2), 2)
      .contextWrite(Context.of("key", "ctx"))
      .collectList()
      .block());
    out.put("scheduler", Mono.deferContextual(context -> Mono.just("sched:" + context.get("key")))
      .subscribeOn(Schedulers.immediate())
      .publishOn(Schedulers.immediate())
      .contextWrite(Context.of("key", "ctx"))
      .block());
    out.put("monoFluxTransition", Mono.deferContextual(context -> Mono.just("mono:" + context.get("key")))
      .flux()
      .flatMap(value -> Mono.deferContextual(context -> Mono.just(value + ":flux:" + context.get("key"))), 2)
      .contextWrite(Context.of("key", "ctx"))
      .collectList()
      .block());
    return out;
  }

  static LinkedHashMap<String, Object> traceContextMatrix() {
    LinkedHashMap<String, Object> out = new LinkedHashMap<>();
    out.put("concatMap", Flux.just(1, 2)
      .concatMap(value -> Mono.deferContextual(context -> Mono.just("concat:" + value + ":" + context.get("key"))))
      .contextWrite(context -> context.put("key", "ctx"))
      .collectList()
      .block());
    out.put("switchMap", Flux.just(1)
      .switchMap(value -> Mono.deferContextual(context -> Mono.just("switch:" + value + ":" + context.get("key"))))
      .contextWrite(context -> context.put("key", "ctx"))
      .collectList()
      .block());
    out.put("switchIfEmpty", Mono.<String>empty()
      .switchIfEmpty(Mono.deferContextual(context -> Mono.just("fallback:" + context.get("key"))))
      .contextWrite(context -> context.put("key", "ctx"))
      .block());
    out.put("innerOverride", Flux.just(1)
      .flatMap(value -> Mono.deferContextual(context -> Mono.just("inner:" + value + ":" + context.get("key")))
        .contextWrite(context -> context.put("key", "inner")), 1)
      .contextWrite(context -> context.put("key", "outer"))
      .collectList()
      .block());
    out.put("thenMany", Mono.just(0)
      .thenMany(Flux.deferContextual(context -> Flux.just("thenMany:" + context.get("key"))))
      .contextWrite(context -> context.put("key", "ctx"))
      .collectList()
      .block());
    out.put("fluxNext", Flux.just(1, 2)
      .next()
      .flatMap(value -> Mono.deferContextual(context -> Mono.just("next:" + value + ":" + context.get("key"))))
      .contextWrite(context -> context.put("key", "ctx"))
      .block());
    out.put("nestedScheduler", Mono.deferContextual(context -> Mono.just("outer:" + context.get("key")))
      .flatMap(value -> Mono.deferContextual(context -> Mono.just(value + ":inner:" + context.get("key")))
        .publishOn(Schedulers.immediate())
        .subscribeOn(Schedulers.immediate()))
      .contextWrite(context -> context.put("key", "ctx"))
      .block());
    return out;
  }

  static LinkedHashMap<String, Object> traceNestedFlatMapSchedulers() {
    ArrayList<String> events = new ArrayList<>();
    List<Integer> values = Flux.just(1, 2)
      .doFirst(() -> events.add("outer.doFirst"))
      .subscribeOn(Schedulers.immediate())
      .publishOn(Schedulers.immediate())
      .flatMap(value -> Mono.just(value)
        .doFirst(() -> events.add("inner" + value + ".doFirst"))
        .subscribeOn(Schedulers.immediate())
        .publishOn(Schedulers.immediate())
        .doOnNext(next -> events.add("inner" + next + ".doOnNext"))
        .doFinally(signal -> events.add("inner" + value + ".doFinally(" + signalName(signal) + ")")), 1)
      .doOnNext(value -> events.add("outer.doOnNext(" + value + ")"))
      .doFinally(signal -> events.add("outer.doFinally(" + signalName(signal) + ")"))
      .collectList()
      .block();
    LinkedHashMap<String, Object> out = new LinkedHashMap<>();
    out.put("events", events);
    out.put("values", values);
    return out;
  }

  static LinkedHashMap<String, Object> traceFlatMapNesting() {
    LinkedHashMap<String, Object> out = new LinkedHashMap<>();
    ArrayList<String> nestedEvents = new ArrayList<>();
    List<Integer> nestedValues = Flux.just(1, 2)
      .flatMap(value -> Mono.just(value)
        .doFirst(() -> nestedEvents.add("outerInner" + value + ".doFirst"))
        .flatMapMany(inner -> Flux.just(inner, inner + 10)
          .publishOn(Schedulers.immediate())
          .doOnNext(next -> nestedEvents.add("deep" + value + ".doOnNext(" + next + ")")))
        .doFinally(signal -> nestedEvents.add("outerInner" + value + ".doFinally(" + signalName(signal) + ")")), 1)
      .doOnNext(value -> nestedEvents.add("outer.doOnNext(" + value + ")"))
      .collectList()
      .block();

    out.put("threeLevelConcatMap", Flux.just(1, 2)
      .concatMap(value -> Flux.just(value, value + 10)
        .concatMap(inner -> Mono.just(inner + 100)))
      .collectList()
      .block());
    out.put("flatMapConcurrencyOne", Flux.just(1, 2)
      .flatMap(value -> Flux.just(value, value + 10), 1)
      .collectList()
      .block());
    out.put("flatMapInnerErrorResume", Flux.just(1, 2)
      .flatMap(value -> value == 1
        ? Flux.<Integer>error(new RuntimeException("boom")).onErrorResume(error -> Flux.just(10))
        : Flux.just(20), 1)
      .collectList()
      .block());
    out.put("switchMapSingleOuter", Flux.just(1)
      .switchMap(value -> Flux.just(value, value + 10))
      .collectList()
      .block());
    LinkedHashMap<String, Object> nestedLifecycle = new LinkedHashMap<>();
    nestedLifecycle.put("events", nestedEvents);
    nestedLifecycle.put("values", nestedValues);
    out.put("nestedLifecycle", nestedLifecycle);
    return out;
  }

  static LinkedHashMap<String, Object> traceLifecyclePositions() {
    ArrayList<String> events = new ArrayList<>();
    List<Integer> values = Flux.just(1)
      .doFirst(() -> events.add("firstA"))
      .doFirst(() -> events.add("firstB"))
      .doOnNext(value -> events.add("nextBeforeMap(" + value + ")"))
      .map(value -> {
        events.add("map(" + value + ")");
        return value + 1;
      })
      .doOnNext(value -> events.add("nextAfterMap(" + value + ")"))
      .doFinally(signal -> events.add("finallyAfterMap(" + signalName(signal) + ")"))
      .collectList()
      .block();
    LinkedHashMap<String, Object> out = new LinkedHashMap<>();
    out.put("events", events);
    out.put("values", values);
    return out;
  }

  static LinkedHashMap<String, Object> traceLifecycleVariations() {
    LinkedHashMap<String, Object> out = new LinkedHashMap<>();
    ArrayList<String> completeEvents = new ArrayList<>();
    List<Integer> completeValues = Flux.just(1)
      .doFirst(() -> completeEvents.add("firstA"))
      .doFirst(() -> completeEvents.add("firstB"))
      .doOnNext(value -> completeEvents.add("nextBeforeMap(" + value + ")"))
      .map(value -> {
        completeEvents.add("map(" + value + ")");
        return value + 1;
      })
      .doOnNext(value -> completeEvents.add("nextAfterMap(" + value + ")"))
      .doOnComplete(() -> completeEvents.add("complete"))
      .doFinally(signal -> completeEvents.add("finally(" + signalName(signal) + ")"))
      .collectList()
      .block();

    ArrayList<String> afterTerminateEvents = new ArrayList<>();
    List<Integer> afterTerminateValues = Flux.just(1)
      .doOnComplete(() -> afterTerminateEvents.add("complete"))
      .doAfterTerminate(() -> afterTerminateEvents.add("afterTerminate"))
      .collectList()
      .block();

    ArrayList<String> errorEvents = new ArrayList<>();
    List<Integer> errorValues = Flux.concat(Flux.just(1), Flux.<Integer>error(new RuntimeException("boom")))
      .doOnNext(value -> errorEvents.add("next(" + value + ")"))
      .doOnError(error -> errorEvents.add("error"))
      .doFinally(signal -> errorEvents.add("finally(" + signalName(signal) + ")"))
      .onErrorReturn(9)
      .collectList()
      .block();

    ArrayList<String> cancelEvents = new ArrayList<>();
    List<Integer> cancelValues = Flux.range(1, 3)
      .doOnCancel(() -> cancelEvents.add("cancel"))
      .doFinally(signal -> cancelEvents.add("upstreamFinally(" + signalName(signal) + ")"))
      .take(1)
      .doFinally(signal -> cancelEvents.add("downstreamFinally(" + signalName(signal) + ")"))
      .collectList()
      .block();

    LinkedHashMap<String, Object> complete = new LinkedHashMap<>();
    complete.put("events", completeEvents);
    complete.put("values", completeValues);
    out.put("complete", complete);
    LinkedHashMap<String, Object> afterTerminate = new LinkedHashMap<>();
    afterTerminate.put("events", afterTerminateEvents);
    afterTerminate.put("values", afterTerminateValues);
    out.put("afterTerminate", afterTerminate);
    LinkedHashMap<String, Object> errorResume = new LinkedHashMap<>();
    errorResume.put("events", errorEvents);
    errorResume.put("values", errorValues);
    out.put("errorResume", errorResume);
    LinkedHashMap<String, Object> cancelTake = new LinkedHashMap<>();
    cancelTake.put("events", cancelEvents);
    cancelTake.put("values", cancelValues);
    out.put("cancelTake", cancelTake);
    return out;
  }

  static LinkedHashMap<String, Object> traceDoFinallyCancel() {
    ArrayList<String> events = new ArrayList<>();
    List<Integer> values = Flux.range(1, 3)
      .doFinally(signal -> events.add("upstreamFinally(" + signalName(signal) + ")"))
      .take(1)
      .doFinally(signal -> events.add("downstreamFinally(" + signalName(signal) + ")"))
      .collectList()
      .block();
    LinkedHashMap<String, Object> out = new LinkedHashMap<>();
    out.put("events", events);
    out.put("values", values);
    return out;
  }

  static LinkedHashMap<String, Object> traceSchedulerBoundaries() {
    LinkedHashMap<String, Object> out = new LinkedHashMap<>();
    ArrayList<String> publishEvents = new ArrayList<>();
    List<Integer> publishValues = Flux.just(1, 2)
      .doOnNext(value -> publishEvents.add("beforePublishOn(" + value + ")"))
      .publishOn(Schedulers.immediate())
      .doOnNext(value -> publishEvents.add("afterPublishOn(" + value + ")"))
      .collectList()
      .block();

    ArrayList<String> nestedEvents = new ArrayList<>();
    Integer nestedValue = Mono.just(1)
      .doFirst(() -> nestedEvents.add("outer.doFirst"))
      .subscribeOn(Schedulers.immediate())
      .flatMap(value -> Mono.just(value + 1)
        .doFirst(() -> nestedEvents.add("inner.doFirst"))
        .publishOn(Schedulers.immediate())
        .subscribeOn(Schedulers.immediate())
        .doOnNext(next -> nestedEvents.add("inner.doOnNext(" + next + ")")))
      .doOnNext(value -> nestedEvents.add("outer.doOnNext(" + value + ")"))
      .block();

    LinkedHashMap<String, Object> publishOnOrder = new LinkedHashMap<>();
    publishOnOrder.put("events", publishEvents);
    publishOnOrder.put("values", publishValues);
    out.put("publishOnOrder", publishOnOrder);
    LinkedHashMap<String, Object> nestedSubscribeOnPublishOn = new LinkedHashMap<>();
    nestedSubscribeOnPublishOn.put("events", nestedEvents);
    nestedSubscribeOnPublishOn.put("value", nestedValue);
    out.put("nestedSubscribeOnPublishOn", nestedSubscribeOnPublishOn);
    return out;
  }

  static LinkedHashMap<String, Object> traceMonoFluxTransitions() {
    LinkedHashMap<String, Object> out = new LinkedHashMap<>();
    out.put("monoFluxMono", Mono.deferContextual(context -> Mono.just("mono:" + context.get("key")))
      .flux()
      .next()
      .flatMap(value -> Mono.deferContextual(context -> Mono.just(value + ":mono:" + context.get("key"))))
      .contextWrite(context -> context.put("key", "ctx"))
      .block());
    out.put("monoFlatMapMany", Mono.just(1)
      .flatMapMany(value -> Flux.deferContextual(context -> Flux.just("many:" + value + ":" + context.get("key"))))
      .contextWrite(context -> context.put("key", "ctx"))
      .collectList()
      .block());
    out.put("fluxNextFlatMap", Flux.just(1, 2)
      .next()
      .publishOn(Schedulers.immediate())
      .flatMap(value -> Mono.deferContextual(context -> Mono.just("flat:" + value + ":" + context.get("key"))))
      .contextWrite(context -> context.put("key", "ctx"))
      .block());
    out.put("thenManyRoundTrip", Mono.just(1)
      .thenMany(Flux.just(2, 3))
      .next()
      .flatMap(value -> Mono.just(value + 10))
      .block());
    return out;
  }

  static LinkedHashMap<String, Object> traceLongLivedStreams() {
    LinkedHashMap<String, Object> out = new LinkedHashMap<>();
    out.put("delayedCollectList", delayedFlux(1, 2).collectList().block());
    out.put("delayedConcatWith", delayedFlux(1, 2).concatWith(delayedFlux(9)).collectList().block());
    out.put("delayedMergeWithSorted", sortedNumbers(delayedFlux(2).mergeWith(delayedFlux(1))).block());
    out.put("intervalTakeCollectList", Flux.interval(Duration.ofMillis(1), Schedulers.boundedElastic())
      .take(3)
      .map(Long::intValue)
      .collectList()
      .block());
    out.put("neverTimeoutFallback", Flux.<Integer>never()
      .timeout(Duration.ofMillis(1), Flux.just(7), Schedulers.boundedElastic())
      .collectList()
      .block());
    out.put("nestedDelayedConcatMap", delayedFlux(1, 2)
      .concatMap(value -> delayedFlux(value, value + 10))
      .collectList()
      .block());
    out.put("nestedDelayedFlatMapSorted", sortedNumbers(delayedFlux(1)
      .mergeWith(delayedFlux(2).flatMap(value -> delayedFlux(value, value + 10), 1))).block());
    out.put("monoThenManyDelayed", Mono.delay(Duration.ofMillis(1), Schedulers.boundedElastic())
      .thenMany(delayedFlux(3, 4))
      .collectList()
      .block());
    return out;
  }

  static Flux<Integer> delayedFlux(Integer... values) {
    return Flux.just(values).delayElements(Duration.ofMillis(1), Schedulers.boundedElastic());
  }

  static Mono<List<Integer>> sortedNumbers(Flux<Integer> source) {
    return source.collectList().map(values -> {
      Collections.sort(values);
      return values;
    });
  }

  static String signalName(SignalType signal) {
    if (signal == SignalType.ON_COMPLETE) return "complete";
    if (signal == SignalType.ON_ERROR) return "error";
    if (signal == SignalType.CANCEL) return "cancel";
    return String.valueOf(signal).toLowerCase(Locale.ROOT);
  }

  static ArrayList<Case<Function<Flux<Integer>, Object>>> singleFluxCollector() {
    ArrayList<Case<Function<Flux<Integer>, Object>>> cases = new ArrayList<>();
    cases.add(new Case<>("collect", source -> source));
    return cases;
  }

  static ArrayList<Case<Function<Mono<Integer>, Object>>> singleMonoCollector() {
    ArrayList<Case<Function<Mono<Integer>, Object>>> cases = new ArrayList<>();
    cases.add(new Case<>("block", source -> source));
    return cases;
  }

  static Flux<Integer> generatedFlux() {
    boolean[] emitted = new boolean[] { false };
    return Flux.<Integer>generate(sink -> {
      if (emitted[0]) {
        sink.complete();
      } else {
        emitted[0] = true;
        sink.next(1);
      }
    });
  }

  static Supplier<Flux<Integer>> counterFlux() {
    AtomicInteger counter = new AtomicInteger();
    return () -> Flux.defer(() -> Flux.just(counter.incrementAndGet()));
  }

  static Supplier<Mono<Integer>> counterMono() {
    AtomicInteger counter = new AtomicInteger();
    return () -> Mono.defer(() -> Mono.just(counter.incrementAndGet()));
  }

  static Supplier<Mono<Integer>> counterSupplierMono() {
    AtomicInteger counter = new AtomicInteger();
    return () -> Mono.fromSupplier(() -> counter.incrementAndGet());
  }

  static String doubleCollect(Supplier<?> factory) {
    return json(collectOracle(factory.get())) + "|" + json(collectOracle(factory.get()));
  }

  static <L, R> LinkedHashMap<String, Object> summarizeMatrix(
    List<Case<L>> leftCases,
    List<Case<R>> rightCases,
    MatrixApply<L, R> apply
  ) {
    ArrayList<String> entries = new ArrayList<>();
    for (Case<L> left : leftCases) {
      for (Case<R> right : rightCases) {
        String name = left.name + "->" + right.name;
        entries.add(name + "=" + json(collectOracle(apply.apply(left.value, right.value))));
      }
    }
    LinkedHashMap<String, Object> out = new LinkedHashMap<>();
    out.put("count", entries.size());
    out.put("hash", fnv1a(String.join("\n", entries)));
    out.put("first", entries.subList(0, Math.min(5, entries.size())));
    out.put("last", entries.subList(Math.max(0, entries.size() - 5), entries.size()));
    out.put("entries", entries);
    return out;
  }

  static Object collectOracle(Object value) {
    try {
      if (value instanceof Flux<?> flux) {
        return normalize(flux.take(8).collectList().block(Duration.ofMillis(500)));
      }
      if (value instanceof Mono<?> mono) {
        return normalize(mono.block(Duration.ofMillis(500)));
      }
      return normalize(value);
    } catch (Throwable error) {
      return "ERROR";
    }
  }

  static Flux<Integer> toFluxOracle(Object value) {
    if (value instanceof Flux<?> flux) {
      return ((Flux<?>) flux).cast(Integer.class);
    }
    if (value instanceof Mono<?> mono) {
      return ((Mono<?>) mono).cast(Integer.class).flux();
    }
    return Flux.just(1, 2);
  }

  static Mono<Integer> toMonoOracle(Object value) {
    if (value instanceof Mono<?> mono) {
      return ((Mono<?>) mono).cast(Integer.class);
    }
    if (value instanceof Flux<?> flux) {
      return ((Flux<?>) flux).cast(Integer.class).next();
    }
    return Mono.just(1);
  }

  static Object normalize(Object value) {
    if (value == null) return null;
    if (value instanceof Flux<?> || value instanceof Mono<?>) return collectOracle(value);
    if (value instanceof Iterable<?> iterable) {
      ArrayList<Object> result = new ArrayList<>();
      for (Object next : iterable) {
        result.add(normalize(next));
      }
      return result;
    }
    if (value instanceof Number || value instanceof Boolean || value instanceof String) return value;
    return String.valueOf(value);
  }

  static String fnv1a(String input) {
    long hash = 0x811c9dc5L;
    for (int i = 0; i < input.length(); i++) {
      hash ^= input.charAt(i);
      hash = (hash * 0x01000193L) & 0xffffffffL;
    }
    return String.format("%08x", hash);
  }

  static final class Case<T> {
    final String name;
    final T value;

    Case(String name, T value) {
      this.name = name;
      this.value = value;
    }
  }

  interface MatrixApply<L, R> {
    Object apply(L left, R right);
  }

  static void sinkScenarios(LinkedHashMap<String, Object> out) {
    mark("sinks-one-value-late");
    Sinks.One<Integer> oneValue = Sinks.one();
    out.put("sinks-one-value-late", Arrays.asList(oneValue.tryEmitValue(1), oneValue.tryEmitValue(2), blockMono(oneValue.asMono())));

    mark("sinks-one-empty-late");
    Sinks.One<Integer> oneEmpty = Sinks.one();
    out.put("sinks-one-empty-late", Arrays.asList(oneEmpty.tryEmitEmpty(), oneEmpty.tryEmitValue(1), blockMono(oneEmpty.asMono().defaultIfEmpty(-1))));

    mark("sinks-one-error-late");
    Sinks.One<Integer> oneError = Sinks.one();
    out.put("sinks-one-error-late", Arrays.asList(oneError.tryEmitError(new RuntimeException("boom")), oneError.tryEmitValue(1), blockMono(oneError.asMono().onErrorReturn(-1))));

    mark("sinks-empty-complete-late");
    Sinks.Empty<Integer> emptyComplete = Sinks.empty();
    out.put("sinks-empty-complete-late", Arrays.asList(emptyComplete.tryEmitEmpty(), emptyComplete.tryEmitError(new RuntimeException("boom")), blockMono(emptyComplete.asMono().defaultIfEmpty(-1))));

    mark("sinks-emit-after-terminated");
    Sinks.One<Integer> emitAfterTerminated = Sinks.one();
    out.put("sinks-emit-after-terminated", emissionReason(() -> {
      emitAfterTerminated.emitValue(1, Sinks.EmitFailureHandler.FAIL_FAST);
      emitAfterTerminated.emitValue(2, Sinks.EmitFailureHandler.FAIL_FAST);
    }));

    mark("sinks-unicast-buffer-late");
    Sinks.Many<Integer> unicastBuffer = Sinks.many().unicast().onBackpressureBuffer();
    out.put("sinks-unicast-buffer-late", Arrays.asList(unicastBuffer.tryEmitNext(1), unicastBuffer.tryEmitNext(2), unicastBuffer.tryEmitComplete(), blockFlux(unicastBuffer.asFlux())));

    mark("sinks-unicast-error-zero-subscriber");
    Sinks.Many<Integer> unicastError = Sinks.many().unicast().onBackpressureError();
    out.put("sinks-unicast-error-zero-subscriber", Arrays.asList(unicastError.tryEmitNext(1), unicastError.tryEmitComplete(), blockFlux(unicastError.asFlux())));

    mark("sinks-multicast-buffer-warmup");
    Sinks.Many<Integer> multicastBuffer = Sinks.many().multicast().onBackpressureBuffer();
    out.put("sinks-multicast-buffer-warmup", Arrays.asList(multicastBuffer.tryEmitNext(1), multicastBuffer.tryEmitNext(2), multicastBuffer.tryEmitComplete(), blockFlux(multicastBuffer.asFlux())));

    mark("sinks-multicast-direct-zero-subscriber");
    Sinks.Many<Integer> directAllOrNothing = Sinks.many().multicast().directAllOrNothing();
    Sinks.Many<Integer> directBestEffort = Sinks.many().multicast().directBestEffort();
    out.put("sinks-multicast-direct-zero-subscriber", Arrays.asList(directAllOrNothing.tryEmitNext(1), directBestEffort.tryEmitNext(1)));

    mark("sinks-replay-all-late");
    Sinks.Many<Integer> replayAll = Sinks.many().replay().all();
    out.put("sinks-replay-all-late", Arrays.asList(replayAll.tryEmitNext(1), replayAll.tryEmitNext(2), replayAll.tryEmitComplete(), blockFlux(replayAll.asFlux())));

    mark("sinks-replay-limit-late");
    Sinks.Many<Integer> replayLimit = Sinks.many().replay().limit(2);
    out.put("sinks-replay-limit-late", Arrays.asList(replayLimit.tryEmitNext(1), replayLimit.tryEmitNext(2), replayLimit.tryEmitNext(3), replayLimit.tryEmitComplete(), blockFlux(replayLimit.asFlux())));

    mark("sinks-replay-latest-late");
    Sinks.Many<Integer> replayLatest = Sinks.many().replay().latest();
    out.put("sinks-replay-latest-late", Arrays.asList(replayLatest.tryEmitNext(1), replayLatest.tryEmitNext(2), replayLatest.tryEmitComplete(), blockFlux(replayLatest.asFlux())));

    mark("sinks-replay-latest-or-default");
    Sinks.Many<Integer> replayLatestDefault = Sinks.many().replay().latestOrDefault(9);
    out.put("sinks-replay-latest-or-default", blockFlux(replayLatestDefault.asFlux().take(1)));

    mark("sinks-many-with-upstream");
    Sinks.ManyWithUpstream<Integer> manyWithUpstream = Sinks.unsafe().manyWithUpstream().multicastOnBackpressureBuffer();
    out.put("sinks-many-with-upstream", Arrays.asList("SUBSCRIBED", subscribeToAndCollect(manyWithUpstream)));

    mark("sinks-one-subscriber-count");
    Sinks.One<Integer> oneSubscriberCount = Sinks.one();
    Disposable oneSubscription = oneSubscriberCount.asMono().subscribe();
    int oneCountBeforeDispose = oneSubscriberCount.currentSubscriberCount();
    oneSubscription.dispose();
    out.put("sinks-one-subscriber-count", Arrays.asList(oneCountBeforeDispose, oneSubscriberCount.currentSubscriberCount()));

    mark("sinks-many-subscriber-count");
    Sinks.Many<Integer> manySubscriberCount = Sinks.many().multicast().onBackpressureBuffer();
    Disposable manySubscription = manySubscriberCount.asFlux().subscribe();
    int manyCountBeforeDispose = manySubscriberCount.currentSubscriberCount();
    manySubscription.dispose();
    out.put("sinks-many-subscriber-count", Arrays.asList(manyCountBeforeDispose, manySubscriberCount.currentSubscriberCount()));
  }

  static void mark(String scenario) {
    System.err.println("[java-oracle] " + scenario);
  }

  static Object subscribeToAndCollect(Sinks.ManyWithUpstream<Integer> sink) {
    sink.subscribeTo(Flux.just(1, 2));
    return blockFlux(sink.asFlux());
  }

  static Object blockMono(Mono<Integer> mono) {
    try {
      return mono.block(Duration.ofMillis(100));
    } catch (Throwable error) {
      return "TIMEOUT_OR_ERROR:" + error.getClass().getSimpleName();
    }
  }

  static Object blockFlux(Flux<Integer> flux) {
    try {
      return flux.collectList().block(Duration.ofMillis(100));
    } catch (Throwable error) {
      return "TIMEOUT_OR_ERROR:" + error.getClass().getSimpleName();
    }
  }

  static String emissionReason(Runnable runnable) {
    try {
      runnable.run();
      return "NO_ERROR";
    } catch (Sinks.EmissionException error) {
      return String.valueOf(error.getReason());
    }
  }

  static String json(Object value) {
    if (value == null) return "null";
    if (value instanceof Number || value instanceof Boolean) return String.valueOf(value);
    if (value instanceof String) return "\"" + ((String)value).replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    if (value instanceof Map<?, ?> map) {
      StringBuilder builder = new StringBuilder("{");
      boolean first = true;
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        if (!first) builder.append(",");
        first = false;
        builder.append(json(String.valueOf(entry.getKey()))).append(":").append(json(entry.getValue()));
      }
      return builder.append("}").toString();
    }
    if (value instanceof Iterable<?> iterable) {
      StringBuilder builder = new StringBuilder("[");
      boolean first = true;
      for (Object next : iterable) {
        if (!first) builder.append(",");
        first = false;
        builder.append(json(next));
      }
      return builder.append("]").toString();
    }
    return json(String.valueOf(value));
  }
}
`;
}
