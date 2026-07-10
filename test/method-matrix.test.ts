import fs from "node:fs";
import path from "node:path";
import { beforeAll, describe, expect, it, vi } from "vitest";
import { Flux, Mono, Schedulers } from "@/index.js";
import { Signal } from "@/signal/signal.js";

type AnyPublisher = Flux<unknown> | Mono<unknown>;
type FluxCase = {
  name: string;
  apply(source: Flux<unknown>): unknown;
};
type MonoCase = {
  name: string;
  apply(source: Mono<unknown>): unknown;
};
type MethodFixture = {
  readonly instance: readonly string[];
  readonly static: readonly string[];
};

const matrixLimit = Number(process.env.REACTOR_MATRIX_LIMIT ?? "0");
const maxPairsPerSection = matrixLimit > 0 ? matrixLimit : Number.POSITIVE_INFINITY;
const finiteProjectionTimeoutMs = 1_500;
const fixtureFlux = fixtureMethods("Flux");
const fixtureMono = fixtureMethods("Mono");

describe("Project Reactor method matrix", () => {
  beforeAll(() => {
    vi.spyOn(console, "debug").mockImplementation(() => undefined);
  });

  it("logs the full pairwise matrix dimensions", () => {
    const sections = [
      ["Flux before/after", fluxCases.length, fluxCases.length],
      ["Mono before/after", monoCases.length, monoCases.length],
      ["Flux to Mono", fluxCases.length, monoCases.length],
      ["Mono to Flux", monoCases.length, fluxCases.length],
      ["nested publisher input", fluxCases.length, fluxCases.length],
      ["Flux static to Flux", fluxStaticCases.length, fluxCases.length],
      ["Mono static to Mono", monoStaticCases.length, monoCases.length],
      ["Flux static to Mono", fluxStaticCases.length, monoCases.length],
      ["Mono static to Flux", monoStaticCases.length, fluxCases.length],
      ["Flux live source variants", fluxSourceVariants().length, fluxCases.length],
      ["Mono live source variants", monoSourceVariants().length, monoCases.length],
      ["nested live publisher input", nestedPublisherVariants().length, fluxCases.length]
    ] as const;
    const fullPairs = sections.reduce((total, [, left, right]) => total + left * right, 0);
    const totalPairs = sections.reduce((total, [, left, right]) => total + Math.min(maxPairsPerSection, left * right), 0);
    for (const [label, left, right] of sections) {
      console.log(`[method-matrix] ${label}: ${left} x ${right} = ${left * right}`);
    }
    console.log(`[method-matrix] pairwise checks scheduled: ${totalPairs} of ${fullPairs}`);
    expect(fullPairs).toBeGreaterThan(95_000);
  });

  it("keeps Java oracle trace coverage for request, lazy, context and lifecycle behavior", () => {
    const traces = javaOracleTraces();
    const traceNames = Object.keys(traces);
    console.log(`[method-matrix] Java oracle trace sections: ${traceNames.join(", ")}`);

    expect(traceNames).toEqual([
      "requestOneByOne",
      "noRequest",
      "lazyDefer",
      "context",
      "nestedFlatMapSchedulers",
      "lifecyclePositions",
      "doFinallyCancel",
      "requestPatterns",
      "lazyFactories",
      "contextMatrix",
      "flatMapNesting",
      "lifecycleVariations",
      "schedulerBoundaries",
      "monoFluxTransitions",
      "longLivedStreams"
    ]);
    expect(traces.requestOneByOne).toEqual([
      "onSubscribe",
      "request(1)",
      "onNext(1)",
      "request(1)",
      "onNext(2)",
      "request(1)",
      "onNext(3)",
      "onComplete"
    ]);
    expect(traces.noRequest).toEqual(["onSubscribe"]);
    expect(traces.lazyDefer).toEqual([
      "assembled",
      "factory",
      "innerOnSubscribe",
      "onSubscribe",
      "afterSubscribe",
      "request(1)",
      "onNext(1)",
      "onComplete"
    ]);
    expect(traces.context).toMatchObject({
      basic: "mono:root",
      overrideOrder: "override:inner",
      flatMap: ["1:ctx"],
      nestedFlatMap: ["deep:ctx:2"],
      scheduler: "sched:ctx",
      monoFluxTransition: ["mono:ctx:flux:ctx"]
    });
    expect(traces.doFinallyCancel).toMatchObject({
      events: ["upstreamFinally(cancel)", "downstreamFinally(complete)"],
      values: [1]
    });
    expect(traces.requestPatterns).toMatchObject({
      noRequest: ["onSubscribe"],
      batchTwo: ["onSubscribe", "request(2)", "onNext(1)", "onNext(2)", "request(2)", "onNext(3)", "onNext(4)", "onComplete"],
      cancelAfterFirst: ["onSubscribe", "request(1)", "onNext(1)", "cancel"]
    });
    expect(traces.lazyFactories).toMatchObject({
      fluxJust: [[1], [1]],
      monoJust: [1, 1],
      fluxDefer: [[1], [2]],
      monoDefer: [1, 2],
      monoFromSupplier: [1, 2],
      create: { events: ["assembled", "callback"], values: [1] }
    });
    expect(traces.contextMatrix).toMatchObject({
      concatMap: ["concat:1:ctx", "concat:2:ctx"],
      switchMap: ["switch:1:ctx"],
      switchIfEmpty: "fallback:ctx",
      innerOverride: ["inner:1:inner"],
      thenMany: ["thenMany:ctx"],
      fluxNext: "next:1:ctx",
      nestedScheduler: "outer:ctx:inner:ctx"
    });
    expect(traces.flatMapNesting).toMatchObject({
      threeLevelConcatMap: [101, 111, 102, 112],
      flatMapConcurrencyOne: [1, 11, 2, 12],
      flatMapInnerErrorResume: [10, 20],
      switchMapSingleOuter: [1, 11],
      nestedLifecycle: {
        values: [1, 11, 2, 12]
      }
    });
    expect(traces.lifecycleVariations).toMatchObject({
      complete: {
        events: ["firstB", "firstA", "nextBeforeMap(1)", "map(1)", "nextAfterMap(2)", "complete", "finally(complete)"],
        values: [2]
      },
      afterTerminate: {
        events: ["complete", "afterTerminate"],
        values: [1]
      },
      errorResume: {
        events: ["next(1)", "error", "finally(error)"],
        values: [1, 9]
      },
      cancelTake: {
        events: ["cancel", "upstreamFinally(cancel)", "downstreamFinally(complete)"],
        values: [1]
      }
    });
    expect(traces.schedulerBoundaries).toMatchObject({
      publishOnOrder: {
        events: ["beforePublishOn(1)", "afterPublishOn(1)", "beforePublishOn(2)", "afterPublishOn(2)"],
        values: [1, 2]
      },
      nestedSubscribeOnPublishOn: {
        events: ["outer.doFirst", "inner.doFirst", "inner.doOnNext(2)", "outer.doOnNext(2)"],
        value: 2
      }
    });
    expect(traces.monoFluxTransitions).toMatchObject({
      monoFluxMono: "mono:ctx:mono:ctx",
      monoFlatMapMany: ["many:1:ctx"],
      fluxNextFlatMap: "flat:1:ctx",
      thenManyRoundTrip: 12
    });
    expect(traces.longLivedStreams).toMatchObject({
      delayedCollectList: [1, 2],
      delayedConcatWith: [1, 2, 9],
      delayedMergeWithSorted: [1, 2],
      intervalTakeCollectList: [0, 1, 2],
      neverTimeoutFallback: [7],
      nestedDelayedConcatMap: [1, 11, 2, 12],
      nestedDelayedFlatMapSorted: [1, 2, 12],
      monoThenManyDelayed: [3, 4]
    });
  });

  it("has a matrix case for every fixture Flux method", () => {
    expect(missing(new Set(fixtureFlux.static), new Set(fluxStaticCases.map(testCase => testCase.name)))).toEqual([]);
    expect(missing(new Set(fixtureFlux.instance), new Set(fluxCases.map(testCase => testCase.name)))).toEqual([]);
  });

  it("has a matrix case for every fixture Mono method", () => {
    expect(missing(new Set(fixtureMono.static), new Set(monoStaticCases.map(testCase => testCase.name)))).toEqual([]);
    expect(missing(new Set(fixtureMono.instance), new Set(monoCases.map(testCase => testCase.name)))).toEqual([]);
  });

  it("executes every Flux method as a standalone finite projection", async () => {
    for (const testCase of fluxCases) {
      await expectFinite(`Flux.${testCase.name}`, toFlux(testCase.apply(baseFlux()), baseFlux()));
    }
  });

  it("executes every Mono method as a standalone finite projection", async () => {
    for (const testCase of monoCases) {
      await expectFinite(`Mono.${testCase.name}`, toFlux(testCase.apply(baseMono()), baseFlux()));
    }
  });

  it("executes every static Flux and Mono factory as a finite projection", async () => {
    for (const testCase of [...fluxStaticCases, ...monoStaticCases]) {
      await expectFinite(`static.${testCase.name}`, toFlux(testCase.apply(), baseFlux()));
    }
  });

  it("composes every static Flux factory with every Flux method", async () => {
    await runPairMatrix("Flux static to Flux", fluxStaticCases, fluxCases, (before, after) => {
      const first = safeFlux(toFlux(before.apply(), baseFlux()));
      return toFlux(after.apply(first), first);
    });
  });

  it("composes every static Mono factory with every Mono method", async () => {
    await runPairMatrix("Mono static to Mono", monoStaticCases, monoCases, (before, after) => {
      const first = safeMono(toMono(before.apply(), baseMono()));
      return toFlux(after.apply(first), baseFlux());
    });
  });

  it("composes every static Flux factory through Flux to Mono transitions", async () => {
    await runPairMatrix("Flux static to Mono", fluxStaticCases, monoCases, (before, after) => {
      const mono = safeFlux(toFlux(before.apply(), baseFlux())).next();
      return toFlux(after.apply(mono), baseFlux());
    });
  });

  it("composes every static Mono factory through Mono to Flux transitions", async () => {
    await runPairMatrix("Mono static to Flux", monoStaticCases, fluxCases, (before, after) => {
      const flux = safeMono(toMono(before.apply(), baseMono())).flux();
      return toFlux(after.apply(flux), baseFlux());
    });
  });

  it("builds and executes before/after Flux pair chains", async () => {
    await runPairMatrix("Flux before/after", fluxCases, fluxCases, (before, after) => {
      const first = safeFlux(toFlux(before.apply(baseFlux()), baseFlux()));
      return toFlux(after.apply(first), first);
    });
  });

  it("builds and executes before/after Mono pair chains", async () => {
    await runPairMatrix("Mono before/after", monoCases, monoCases, (before, after) => {
      const first = safeMono(toMono(before.apply(baseMono()), baseMono()));
      return toFlux(after.apply(first), baseFlux());
    });
  });

  it("executes Flux to Mono transition chains", async () => {
    await runPairMatrix("Flux to Mono", fluxCases, monoCases, (before, after) => {
      const mono = safeFlux(toFlux(before.apply(baseFlux()), baseFlux())).next();
      return toFlux(after.apply(mono), baseFlux());
    });
  });

  it("executes Mono to Flux transition chains", async () => {
    await runPairMatrix("Mono to Flux", monoCases, fluxCases, (before, after) => {
      const flux = safeMono(toMono(before.apply(baseMono()), baseMono())).flux();
      return toFlux(after.apply(flux), baseFlux());
    });
  });

  it("executes nested publisher-input usage for every method pair", async () => {
    await runPairMatrix("nested publisher input", fluxCases, fluxCases, (outer, inner) => {
      const nested = safeFlux(toFlux(inner.apply(Flux.just(10)), Flux.just(10)));
      return toFlux(applyNestedFlux(outer.name, baseFlux(), nested), baseFlux());
    });
  });

  it("executes every Flux method against delayed, interval and timeout-backed sources", async () => {
    let executed = 0;
    for (const variant of fluxSourceVariants()) {
      for (const testCase of fluxCases) {
        executed += 1;
        await expectFinite(
          `Flux.${testCase.name} on ${variant.name}`,
          toFlux(testCase.apply(variant.flux()), variant.fallback())
        );
      }
    }
    console.log(`[method-matrix] Flux live source variants: executed ${executed} chains`);
    expect(executed).toBe(fluxSourceVariants().length * fluxCases.length);
  }, 60_000);

  it("executes every Mono method against delayed and timeout-backed sources", async () => {
    let executed = 0;
    for (const variant of monoSourceVariants()) {
      for (const testCase of monoCases) {
        executed += 1;
        await expectFinite(
          `Mono.${testCase.name} on ${variant.name}`,
          toFlux(testCase.apply(variant.mono()), variant.fallback())
        );
      }
    }
    console.log(`[method-matrix] Mono live source variants: executed ${executed} chains`);
    expect(executed).toBe(monoSourceVariants().length * monoCases.length);
  });

  it("executes every Flux method with delayed and live nested publisher inputs", async () => {
    let executed = 0;
    for (const nested of nestedPublisherVariants()) {
      for (const testCase of fluxCases) {
        executed += 1;
        await expectFinite(
          `nested ${testCase.name} with ${nested.name}`,
          toFlux(applyNestedFlux(testCase.name, delayedFlux(1, 2), nested.flux()), baseFlux())
        );
      }
    }
    console.log(`[method-matrix] nested live publisher inputs: executed ${executed} chains`);
    expect(executed).toBe(nestedPublisherVariants().length * fluxCases.length);
  }, 60_000);

  it("checks focused delayed and live merge, concat and collectList compositions", async () => {
    await expect(withTimeout(delayedFlux(1, 2).concatWith(delayedFlux(9)).toArray(), 500))
      .resolves.toEqual([1, 2, 9]);
    await expect(withTimeout(delayedFlux(1, 2).collectList().block(), 500))
      .resolves.toEqual([1, 2]);
    await expect(withTimeout(sortedNumbers(delayedFlux(2).mergeWith(delayedFlux(1))).block(), 500))
      .resolves.toEqual([1, 2]);
    await expect(withTimeout(Flux.interval(0, Schedulers.timeout()).take(3).collectList().block(), 500))
      .resolves.toEqual([0, 1, 2]);
    await expect(withTimeout(Flux.never<number>().timeout(1, Flux.just(7), Schedulers.timeout()).collectList().block(), 500))
      .resolves.toEqual([7]);
    await expect(withTimeout(delayedFlux(1, 2).concatMap(value => delayedFlux(value, Number(value) + 10)).collectList().block(), 500))
      .resolves.toEqual([1, 11, 2, 12]);
    await expect(withTimeout(sortedNumbers(delayedFlux(1).mergeWith(delayedFlux(2).flatMap(value => delayedFlux(value, Number(value) + 10), 1))).block(), 500))
      .resolves.toEqual([1, 2, 12]);
  });

  it("executes scheduler variants for every scheduler-aware method", async () => {
    const schedulers = [
      Schedulers.immediate(),
      Schedulers.microtask(),
      Schedulers.timeout(),
      Schedulers.newSingle("matrix"),
      Schedulers.newBoundedElastic("matrix")
    ];

    for (const scheduler of schedulers) {
      await expectFinite(`Flux.publishOn.${scheduler.name}`, Flux.just(1).publishOn(scheduler));
      await expectFinite(`Flux.subscribeOn.${scheduler.name}`, Flux.just(1).subscribeOn(scheduler));
      await expectFinite(`Flux.delayElements.${scheduler.name}`, Flux.just(1).delayElements(0, scheduler));
      await expectFinite(`Flux.timeout.${scheduler.name}`, Flux.just(1).timeout(1, Flux.just(9), scheduler));
      await expectFinite(`Flux.sampleFirst.${scheduler.name}`, Flux.just(1).sampleFirst(0, scheduler));
      await expectFinite(`Mono.delay.${scheduler.name}`, Mono.delay(0, scheduler));
      await expectFinite(`Mono.publishOn.${scheduler.name}`, Mono.just(1).publishOn(scheduler));
      await expectFinite(`Mono.subscribeOn.${scheduler.name}`, Mono.just(1).subscribeOn(scheduler));
      await expectFinite(`Mono.delayElement.${scheduler.name}`, Mono.just(1).delayElement(0, scheduler));
    }
  });
});

const fluxStaticCases: Array<{ name: string; apply(): unknown }> = [
  { name: "combineLatest", apply: () => Flux.combineLatest([Flux.just(1), Flux.just(2)]) },
  { name: "concat", apply: () => Flux.concat(Flux.just(1), Flux.just(2)) },
  { name: "concatDelayError", apply: () => Flux.concatDelayError(Flux.just(1), Flux.just(2)) },
  { name: "create", apply: () => Flux.create<number>(sink => { sink.next(1); sink.complete(); }) },
  { name: "defer", apply: () => Flux.defer(() => Flux.just(1)) },
  { name: "deferContextual", apply: () => Flux.deferContextual(() => Flux.just(1)) },
  { name: "empty", apply: () => Flux.empty() },
  { name: "error", apply: () => Flux.error<number>(new Error("matrix")).onErrorReturn(1) },
  { name: "first", apply: () => Flux.first(Flux.empty(), Flux.just(1)) },
  { name: "firstWithSignal", apply: () => Flux.firstWithSignal(Flux.just(1), Flux.just(2)) },
  { name: "firstWithValue", apply: () => Flux.firstWithValue(Flux.empty(), Flux.just(1)) },
  { name: "from", apply: () => Flux.from([1]) },
  { name: "fromArray", apply: () => Flux.fromArray([1]) },
  { name: "fromIterable", apply: () => Flux.fromIterable([1]) },
  { name: "fromStream", apply: () => Flux.fromStream(() => [1]) },
  { name: "generate", apply: () => Flux.generate<number>(sink => { sink.next(1); sink.complete(); }) },
  { name: "interval", apply: () => Flux.interval(0, Schedulers.immediate()).take(1) },
  { name: "just", apply: () => Flux.just(1) },
  { name: "merge", apply: () => Flux.merge(Flux.just(1), Flux.just(2)) },
  { name: "mergeComparing", apply: () => Flux.mergeComparing(numberComparator, Flux.just(2), Flux.just(1)) },
  { name: "mergeComparingDelayError", apply: () => Flux.mergeComparingDelayError(numberComparator, Flux.just(2), Flux.just(1)) },
  { name: "mergeDelayError", apply: () => Flux.mergeDelayError(Flux.just(1), Flux.just(2)) },
  { name: "mergeOrdered", apply: () => Flux.mergeOrdered(numberComparator, Flux.just(2), Flux.just(1)) },
  { name: "mergePriority", apply: () => Flux.mergePriority(numberComparator, Flux.just(2), Flux.just(1)) },
  { name: "mergePriorityDelayError", apply: () => Flux.mergePriorityDelayError(numberComparator, Flux.just(2), Flux.just(1)) },
  { name: "mergeSequential", apply: () => Flux.mergeSequential(Flux.just(1), Flux.just(2)) },
  { name: "mergeSequentialDelayError", apply: () => Flux.mergeSequentialDelayError(Flux.just(1), Flux.just(2)) },
  { name: "never", apply: () => Flux.never().timeout(1, Flux.just(1), Schedulers.timeout()) },
  { name: "push", apply: () => Flux.push<number>(sink => { sink.next(1); sink.complete(); }) },
  { name: "range", apply: () => Flux.range(1, 1) },
  { name: "switchOnNext", apply: () => Flux.switchOnNext(Flux.just(Flux.just(1))) },
  { name: "using", apply: () => Flux.using(() => ({ closed: false }), () => Flux.just(1), resource => { resource.closed = true; }) },
  { name: "usingWhen", apply: () => Flux.usingWhen(Flux.just({ closed: false }), () => Flux.just(1), () => Flux.empty()) },
  { name: "zip", apply: () => Flux.zip([Flux.just(1), Flux.just(2)]) }
];

const monoStaticCases: Array<{ name: string; apply(): unknown }> = [
  { name: "create", apply: () => Mono.create<number>(sink => sink.success(1)) },
  { name: "defer", apply: () => Mono.defer(() => Mono.just(1)) },
  { name: "deferContextual", apply: () => Mono.deferContextual(() => Mono.just(1)) },
  { name: "delay", apply: () => Mono.delay(0, Schedulers.immediate()) },
  { name: "empty", apply: () => Mono.empty() },
  { name: "error", apply: () => Mono.error<number>(new Error("matrix")).onErrorReturn(1) },
  { name: "first", apply: () => Mono.first(Mono.empty(), Mono.just(1)) },
  { name: "firstWithSignal", apply: () => Mono.firstWithSignal(Mono.just(1), Mono.just(2)) },
  { name: "firstWithValue", apply: () => Mono.firstWithValue(Mono.empty(), Mono.just(1)) },
  { name: "from", apply: () => Mono.from(Flux.just(1)) },
  { name: "fromCallable", apply: () => Mono.fromCallable(() => 1) },
  { name: "fromCompletionStage", apply: () => Mono.fromCompletionStage(Promise.resolve(1)) },
  { name: "fromDirect", apply: () => Mono.fromDirect(Flux.just(1, 2)) },
  { name: "fromFuture", apply: () => Mono.fromFuture(Promise.resolve(1)) },
  { name: "fromRunnable", apply: () => Mono.fromRunnable(() => undefined) },
  { name: "fromSupplier", apply: () => Mono.fromSupplier(() => 1) },
  { name: "ignoreElements", apply: () => Mono.ignoreElements(Flux.just(1)) },
  { name: "just", apply: () => Mono.just(1) },
  { name: "justOrEmpty", apply: () => Mono.justOrEmpty(1) },
  { name: "never", apply: () => Mono.never().timeout(1, Mono.just(1), Schedulers.timeout()) },
  { name: "sequenceEqual", apply: () => Mono.sequenceEqual(Flux.just(1), Flux.just(1)) },
  { name: "using", apply: () => Mono.using(() => ({ closed: false }), () => Mono.just(1), resource => { resource.closed = true; }) },
  { name: "usingWhen", apply: () => Mono.usingWhen(Mono.just({ closed: false }), () => Mono.just(1), () => Mono.empty()) },
  { name: "when", apply: () => Mono.when(Flux.just(1), Mono.just(2)) },
  { name: "whenDelayError", apply: () => Mono.whenDelayError(Flux.just(1), Mono.just(2)) },
  { name: "zip", apply: () => Mono.zip([Mono.just(1), Mono.just(2)]) },
  { name: "zipDelayError", apply: () => Mono.zipDelayError([Mono.just(1), Mono.just(2)]) }
];

const fluxCases: FluxCase[] = [
  { name: "all", apply: source => source.all(() => true) },
  { name: "any", apply: source => source.any(() => true) },
  { name: "as", apply: source => source.as(next => next) },
  { name: "blockFirst", apply: source => source.blockFirst() },
  { name: "blockLast", apply: source => source.blockLast() },
  { name: "buffer", apply: source => source.buffer(2) },
  { name: "bufferTimeout", apply: source => source.bufferTimeout(2, 0, Schedulers.immediate()) },
  { name: "bufferUntil", apply: source => source.bufferUntil(() => true) },
  { name: "bufferUntilChanged", apply: source => source.bufferUntilChanged(value => String(value)) },
  { name: "bufferWhen", apply: source => source.bufferWhen(Flux.just(0), () => Flux.just(0)) },
  { name: "bufferWhile", apply: source => source.bufferWhile(() => true) },
  { name: "cache", apply: source => source.cache() },
  { name: "cancelOn", apply: source => source.cancelOn(Schedulers.immediate()) },
  { name: "cast", apply: source => source.cast() },
  { name: "checkpoint", apply: source => source.checkpoint("matrix") },
  { name: "collect", apply: source => source.collect(() => [] as unknown[], (values, value) => { values.push(value); }) },
  { name: "collectList", apply: source => source.collectList() },
  { name: "collectMap", apply: source => source.collectMap(value => String(value)) },
  { name: "collectMultimap", apply: source => source.collectMultimap(value => String(value)) },
  { name: "collectSortedList", apply: source => source.collectSortedList(stringComparator) },
  { name: "concatMap", apply: source => source.concatMap(value => Flux.just(value)) },
  { name: "concatMapDelayError", apply: source => source.concatMapDelayError(value => Flux.just(value)) },
  { name: "concatMapIterable", apply: source => source.concatMapIterable(value => [value]) },
  { name: "concatWith", apply: source => source.concatWith(Flux.just(9)) },
  { name: "concatWithValues", apply: source => source.concatWithValues(9) },
  { name: "contextCapture", apply: source => source.contextCapture() },
  { name: "contextWrite", apply: source => source.contextWrite(context => context.put("matrix", true)) },
  { name: "count", apply: source => source.count() },
  { name: "defaultIfEmpty", apply: source => source.defaultIfEmpty(0) },
  { name: "delayElements", apply: source => source.delayElements(0, Schedulers.immediate()) },
  { name: "delaySequence", apply: source => source.delaySequence(0, Schedulers.immediate()) },
  { name: "delaySubscription", apply: source => source.delaySubscription(0, Schedulers.immediate()) },
  { name: "delayUntil", apply: source => source.delayUntil(() => Flux.empty()) },
  { name: "dematerialize", apply: () => Flux.just(Signal.next(1), Signal.complete()).dematerialize() },
  { name: "distinct", apply: source => source.distinct(value => String(value)) },
  { name: "distinctUntilChanged", apply: source => source.distinctUntilChanged(value => String(value)) },
  { name: "doAfterTerminate", apply: source => source.doAfterTerminate(() => undefined) },
  { name: "doFinally", apply: source => source.doFinally(() => undefined) },
  { name: "doFirst", apply: source => source.doFirst(() => undefined) },
  { name: "doOnCancel", apply: source => source.doOnCancel(() => undefined) },
  { name: "doOnComplete", apply: source => source.doOnComplete(() => undefined) },
  { name: "doOnDiscard", apply: source => source.doOnDiscard(Object, () => undefined) },
  { name: "doOnEach", apply: source => source.doOnEach(() => undefined) },
  { name: "doOnError", apply: source => source.doOnError(() => undefined) },
  { name: "doOnNext", apply: source => source.doOnNext(() => undefined) },
  { name: "doOnRequest", apply: source => source.doOnRequest(() => undefined) },
  { name: "doOnSubscribe", apply: source => source.doOnSubscribe(() => undefined) },
  { name: "doOnTerminate", apply: source => source.doOnTerminate(() => undefined) },
  { name: "elementAt", apply: source => source.elementAt(0, 0) },
  { name: "elapsed", apply: source => source.elapsed(Schedulers.immediate()) },
  { name: "expand", apply: source => source.expand(() => Flux.empty()) },
  { name: "expandDeep", apply: source => source.expandDeep(() => Flux.empty()) },
  { name: "filter", apply: source => source.filter(() => true) },
  { name: "filterWhen", apply: source => source.filterWhen(() => Mono.just(true)) },
  { name: "flatMap", apply: source => source.flatMap(value => Flux.just(value), 1) },
  { name: "flatMapDelayError", apply: source => source.flatMapDelayError(value => Flux.just(value), 1) },
  { name: "flatMapIterable", apply: source => source.flatMapIterable(value => [value]) },
  { name: "flatMapSequential", apply: source => source.flatMapSequential(value => Flux.just(value), 1) },
  { name: "flatMapSequentialDelayError", apply: source => source.flatMapSequentialDelayError(value => Flux.just(value), 1) },
  { name: "groupBy", apply: source => source.groupBy(value => String(value)) },
  { name: "groupJoin", apply: source => source.groupJoin(Flux.just(9), () => Flux.empty(), () => Flux.empty(), (left, right) => [left, right]) },
  { name: "handle", apply: source => source.handle((value, sink) => sink.next(value)) },
  { name: "hasElement", apply: source => source.hasElement(1) },
  { name: "hasElements", apply: source => source.hasElements() },
  { name: "hide", apply: source => source.hide() },
  { name: "ignoreElements", apply: source => source.ignoreElements() },
  { name: "index", apply: source => source.index() },
  { name: "join", apply: source => source.join(Flux.just(9), () => Flux.empty(), () => Flux.empty(), (left, right) => [left, right]) },
  { name: "last", apply: source => source.last(0) },
  { name: "limitRate", apply: source => source.limitRate(2) },
  { name: "limitRequest", apply: source => source.limitRequest(1) },
  { name: "log", apply: source => source.log("matrix") },
  { name: "map", apply: source => source.map(value => value) },
  { name: "mapNotNull", apply: source => source.mapNotNull(value => value) },
  { name: "materialize", apply: source => source.materialize() },
  { name: "mergeComparingWith", apply: source => source.mergeComparingWith(stringComparator, Flux.just(9)) },
  { name: "mergeOrderedWith", apply: source => source.mergeOrderedWith(stringComparator, Flux.just(9)) },
  { name: "mergeWith", apply: source => source.mergeWith(Flux.just(9)) },
  { name: "metrics", apply: source => source.metrics() },
  { name: "name", apply: source => source.name("matrix") },
  { name: "next", apply: source => source.next() },
  { name: "ofType", apply: source => source.ofType(Object) },
  { name: "onBackpressureBuffer", apply: source => source.onBackpressureBuffer() },
  { name: "onBackpressureDrop", apply: source => source.onBackpressureDrop() },
  { name: "onBackpressureError", apply: source => source.onBackpressureError() },
  { name: "onBackpressureLatest", apply: source => source.onBackpressureLatest() },
  { name: "onErrorComplete", apply: source => source.onErrorComplete() },
  { name: "onErrorContinue", apply: source => source.onErrorContinue(() => undefined) },
  { name: "onErrorMap", apply: source => source.onErrorMap(error => error) },
  { name: "onErrorResume", apply: source => source.onErrorResume(() => Flux.just(1)) },
  { name: "onErrorReturn", apply: source => source.onErrorReturn(1) },
  { name: "onErrorStop", apply: source => source.onErrorStop() },
  { name: "onTerminateDetach", apply: source => source.onTerminateDetach() },
  { name: "or", apply: source => source.or(Flux.just(9)) },
  { name: "parallel", apply: source => source.parallel() },
  { name: "publish", apply: source => source.publish(next => next) },
  { name: "publishNext", apply: source => source.publishNext() },
  { name: "publishOn", apply: source => source.publishOn(Schedulers.immediate()) },
  { name: "reduce", apply: source => source.reduce((left, right) => right ?? left) },
  { name: "reduceWith", apply: source => source.reduceWith(() => 0, () => 1) },
  { name: "repeat", apply: source => source.repeat(1) },
  { name: "repeatWhen", apply: source => source.repeatWhen(() => Flux.empty()) },
  { name: "replay", apply: source => source.replay() },
  { name: "retry", apply: source => source.retry(1) },
  { name: "retryWhen", apply: source => source.retryWhen(() => Flux.empty()) },
  { name: "sample", apply: source => source.sample(Flux.just(0)) },
  { name: "sampleFirst", apply: source => source.sampleFirst(0, Schedulers.immediate()) },
  { name: "sampleTimeout", apply: source => source.sampleTimeout(() => Flux.empty()) },
  { name: "scan", apply: source => source.scan((left, right) => right ?? left) },
  { name: "scanWith", apply: source => source.scanWith(() => 0, () => 1) },
  { name: "share", apply: source => source.share() },
  { name: "shareNext", apply: source => source.shareNext() },
  { name: "single", apply: source => source.take(1).single(0) },
  { name: "singleOrEmpty", apply: source => source.take(1).singleOrEmpty() },
  { name: "skip", apply: source => source.skip(0) },
  { name: "skipLast", apply: source => source.skipLast(0) },
  { name: "skipUntil", apply: source => source.skipUntil(() => true) },
  { name: "skipUntilOther", apply: source => source.skipUntilOther(Flux.just(0)) },
  { name: "skipWhile", apply: source => source.skipWhile(() => false) },
  { name: "sort", apply: source => source.sort(stringComparator) },
  { name: "startWith", apply: source => source.startWith(0) },
  { name: "subscribe", apply: source => { const disposable = source.subscribe(() => undefined); disposable?.dispose(); return source; } },
  { name: "subscribeOn", apply: source => source.subscribeOn(Schedulers.immediate()) },
  { name: "subscribeWith", apply: source => source.subscribeWith(noopSubscriber()) },
  { name: "switchIfEmpty", apply: source => source.switchIfEmpty(Flux.just(1)) },
  { name: "switchMap", apply: source => source.switchMap(value => Flux.just(value)) },
  { name: "switchOnFirst", apply: source => source.switchOnFirst((_signal, next) => next) },
  { name: "tag", apply: source => source.tag("matrix", "true") },
  { name: "take", apply: source => source.take(1) },
  { name: "takeLast", apply: source => source.takeLast(1) },
  { name: "takeUntil", apply: source => source.takeUntil(() => true) },
  { name: "takeUntilOther", apply: source => source.takeUntilOther(Flux.empty()) },
  { name: "takeWhile", apply: source => source.takeWhile(() => true) },
  { name: "tap", apply: source => source.tap({ onNext: () => undefined }) },
  { name: "then", apply: source => source.then() },
  { name: "thenEmpty", apply: source => source.thenEmpty(Flux.empty()) },
  { name: "thenMany", apply: source => source.thenMany(Flux.just(1)) },
  { name: "thenReturn", apply: source => source.thenReturn(1) },
  { name: "timed", apply: source => source.timed(Schedulers.immediate()) },
  { name: "timeout", apply: source => source.timeout(1, Flux.just(1), Schedulers.timeout()) },
  { name: "timestamp", apply: source => source.timestamp(Schedulers.immediate()) },
  { name: "toStream", apply: source => source.toStream() },
  { name: "toIterable", apply: source => source.toIterable() },
  { name: "transform", apply: source => source.transform(next => next) },
  { name: "transformDeferred", apply: source => source.transformDeferred(next => next) },
  { name: "transformDeferredContextual", apply: source => source.transformDeferredContextual(next => next) },
  { name: "window", apply: source => source.window(2) },
  { name: "windowTimeout", apply: source => source.windowTimeout(2, 0, Schedulers.immediate()) },
  { name: "windowUntil", apply: source => source.windowUntil(() => true) },
  { name: "windowUntilChanged", apply: source => source.windowUntilChanged(value => String(value)) },
  { name: "windowWhen", apply: source => source.windowWhen(Flux.just(0), () => Flux.just(0)) },
  { name: "windowWhile", apply: source => source.windowWhile(() => true) },
  { name: "withLatestFrom", apply: source => source.withLatestFrom(Flux.just(9)) },
  { name: "zipWith", apply: source => source.zipWith(Flux.just(9)) },
  { name: "zipWithIterable", apply: source => source.zipWithIterable([9]) }
];

const monoCases: MonoCase[] = [
  { name: "and", apply: source => source.and(Mono.empty()) },
  { name: "as", apply: source => source.as(next => next) },
  { name: "block", apply: source => source.block() },
  { name: "cache", apply: source => source.cache() },
  { name: "cacheInvalidateIf", apply: source => source.cacheInvalidateIf(() => false) },
  { name: "cacheInvalidateWhen", apply: source => source.cacheInvalidateWhen(() => Mono.empty()) },
  { name: "cancelOn", apply: source => source.cancelOn(Schedulers.immediate()) },
  { name: "cast", apply: source => source.cast() },
  { name: "checkpoint", apply: source => source.checkpoint("matrix") },
  { name: "concatWith", apply: source => source.concatWith(Mono.just(9)) },
  { name: "contextCapture", apply: source => source.contextCapture() },
  { name: "contextWrite", apply: source => source.contextWrite(context => context.put("matrix", true)) },
  { name: "defaultIfEmpty", apply: source => source.defaultIfEmpty(0) },
  { name: "delayElement", apply: source => source.delayElement(0, Schedulers.immediate()) },
  { name: "delaySubscription", apply: source => source.delaySubscription(0, Schedulers.immediate()) },
  { name: "delayUntil", apply: source => source.delayUntil(() => Mono.empty()) },
  { name: "dematerialize", apply: () => Mono.just(Signal.next(1)).dematerialize() },
  { name: "doAfterTerminate", apply: source => source.doAfterTerminate(() => undefined) },
  { name: "doFinally", apply: source => source.doFinally(() => undefined) },
  { name: "doFirst", apply: source => source.doFirst(() => undefined) },
  { name: "doOnCancel", apply: source => source.doOnCancel(() => undefined) },
  { name: "doOnDiscard", apply: source => source.doOnDiscard(Object, () => undefined) },
  { name: "doOnEach", apply: source => source.doOnEach(() => undefined) },
  { name: "doOnError", apply: source => source.doOnError(() => undefined) },
  { name: "doOnNext", apply: source => source.doOnNext(() => undefined) },
  { name: "doOnRequest", apply: source => source.doOnRequest(() => undefined) },
  { name: "doOnSubscribe", apply: source => source.doOnSubscribe(() => undefined) },
  { name: "doOnSuccess", apply: source => source.doOnSuccess(() => undefined) },
  { name: "doOnTerminate", apply: source => source.doOnTerminate(() => undefined) },
  { name: "elapsed", apply: source => source.elapsed(Schedulers.immediate()) },
  { name: "expand", apply: source => source.expand(() => Mono.empty()) },
  { name: "expandDeep", apply: source => source.expandDeep(() => Mono.empty()) },
  { name: "filter", apply: source => source.filter(() => true) },
  { name: "filterWhen", apply: source => source.filterWhen(() => Mono.just(true)) },
  { name: "flatMap", apply: source => source.flatMap(value => Mono.just(value)) },
  { name: "flatMapIterable", apply: source => source.flatMapIterable(value => [value]) },
  { name: "flatMapMany", apply: source => source.flatMapMany(value => Flux.just(value)) },
  { name: "flux", apply: source => source.flux() },
  { name: "handle", apply: source => source.handle((value, sink) => sink.next(value)) },
  { name: "hasElement", apply: source => source.hasElement() },
  { name: "hide", apply: source => source.hide() },
  { name: "ignoreElement", apply: source => source.ignoreElement() },
  { name: "log", apply: source => source.log("matrix") },
  { name: "map", apply: source => source.map(value => value) },
  { name: "mapNotNull", apply: source => source.mapNotNull(value => value) },
  { name: "materialize", apply: source => source.materialize() },
  { name: "mergeWith", apply: source => source.mergeWith(Mono.just(9)) },
  { name: "metrics", apply: source => source.metrics() },
  { name: "name", apply: source => source.name("matrix") },
  { name: "ofType", apply: source => source.ofType(Object) },
  { name: "onErrorComplete", apply: source => source.onErrorComplete() },
  { name: "onErrorContinue", apply: source => source.onErrorContinue(() => undefined) },
  { name: "onErrorMap", apply: source => source.onErrorMap(error => error) },
  { name: "onErrorResume", apply: source => source.onErrorResume(() => Mono.just(1)) },
  { name: "onErrorReturn", apply: source => source.onErrorReturn(1) },
  { name: "onErrorStop", apply: source => source.onErrorStop() },
  { name: "onTerminateDetach", apply: source => source.onTerminateDetach() },
  { name: "or", apply: source => source.or(Mono.just(9)) },
  { name: "publish", apply: source => source.publish(next => next) },
  { name: "publishOn", apply: source => source.publishOn(Schedulers.immediate()) },
  { name: "repeat", apply: source => source.repeat(1) },
  { name: "repeatWhen", apply: source => source.repeatWhen(() => Flux.empty()) },
  { name: "repeatWhenEmpty", apply: source => source.repeatWhenEmpty(() => Flux.empty()) },
  { name: "retry", apply: source => source.retry(1) },
  { name: "retryWhen", apply: source => source.retryWhen(() => Flux.empty()) },
  { name: "share", apply: source => source.share() },
  { name: "single", apply: source => source.single() },
  { name: "singleOptional", apply: source => source.singleOptional() },
  { name: "subscribe", apply: source => { const disposable = source.subscribe(() => undefined); disposable?.dispose(); return source; } },
  { name: "subscribeOn", apply: source => source.subscribeOn(Schedulers.immediate()) },
  { name: "subscribeWith", apply: source => source.subscribeWith(noopSubscriber()) },
  { name: "switchIfEmpty", apply: source => source.switchIfEmpty(Mono.just(1)) },
  { name: "tag", apply: source => source.tag("matrix", "true") },
  { name: "take", apply: source => source.take() },
  { name: "takeUntilOther", apply: source => source.takeUntilOther(Flux.empty()) },
  { name: "tap", apply: source => source.tap({ onNext: () => undefined }) },
  { name: "then", apply: source => source.then() },
  { name: "thenEmpty", apply: source => source.thenEmpty(Mono.empty()) },
  { name: "thenMany", apply: source => source.thenMany(Flux.just(1)) },
  { name: "thenReturn", apply: source => source.thenReturn(1) },
  { name: "timed", apply: source => source.timed(Schedulers.immediate()) },
  { name: "timeout", apply: source => source.timeout(1, Mono.just(1), Schedulers.timeout()) },
  { name: "timestamp", apply: source => source.timestamp(Schedulers.immediate()) },
  { name: "toFuture", apply: source => source.toFuture() },
  { name: "transform", apply: source => source.transform(next => next) },
  { name: "transformDeferred", apply: source => source.transformDeferred(next => next) },
  { name: "transformDeferredContextual", apply: source => source.transformDeferredContextual(next => next) },
  { name: "zipWhen", apply: source => source.zipWhen(value => Mono.just(value)) },
  { name: "zipWith", apply: source => source.zipWith(Mono.just(9)) }
];

async function runPairMatrix<A extends { name: string }, B extends { name: string }>(
  label: string,
  beforeCases: readonly A[],
  afterCases: readonly B[],
  run: (before: A, after: B) => unknown
): Promise<void> {
  let executed = 0;
  for (const before of beforeCases) {
    for (const after of afterCases) {
      if (executed >= maxPairsPerSection) {
        return;
      }
      executed += 1;
      await expectFinite(`${label}: ${before.name} -> ${after.name}`, toFlux(run(before, after), baseFlux()));
    }
  }
  console.log(`[method-matrix] ${label}: executed ${executed} pairwise chains`);
  expect(executed).toBe(beforeCases.length * afterCases.length);
}

async function expectFinite(label: string, publisher: Flux<unknown>): Promise<void> {
  try {
    const values = await withTimeout(safeFlux(publisher).take(4).toArray(), finiteProjectionTimeoutMs);
    expect(Array.isArray(values), label).toBe(true);
  } catch (error) {
    throw new Error(`${label} failed: ${String(error)}`);
  }
}

function applyNestedFlux(name: string, source: Flux<unknown>, nested: Flux<unknown>): unknown {
  switch (name) {
    case "concatWith":
      return source.concatWith(nested);
    case "delaySubscription":
      return source.delaySubscription(nested);
    case "mergeWith":
      return source.mergeWith(nested);
    case "or":
      return source.or(nested);
    case "sample":
      return source.sample(nested);
    case "skipUntilOther":
      return source.skipUntilOther(nested);
    case "switchIfEmpty":
      return source.switchIfEmpty(nested);
    case "takeUntilOther":
      return source.takeUntilOther(nested);
    case "thenEmpty":
      return source.thenEmpty(nested);
    case "thenMany":
      return source.thenMany(nested);
    case "timeout":
      return source.timeout(1, nested, Schedulers.timeout());
    case "withLatestFrom":
      return source.withLatestFrom(nested);
    case "zipWith":
      return source.zipWith(nested);
    default:
      return fluxCases.find(testCase => testCase.name === name)?.apply(source) ?? source;
  }
}

function toFlux(value: unknown, fallback: Flux<unknown>): Flux<unknown> {
  if (value instanceof Flux) {
    return value;
  }
  if (value instanceof Mono) {
    return value.flux();
  }
  if (isAsyncIterable(value) || isIterable(value)) {
    return Flux.from(value as AsyncIterable<unknown> | Iterable<unknown>);
  }
  if (isPromiseLike(value)) {
    return Mono.fromPromise(value).flux();
  }
  return fallback;
}

function toMono(value: unknown, fallback: Mono<unknown>): Mono<unknown> {
  if (value instanceof Mono) {
    return value;
  }
  if (value instanceof Flux) {
    return value.next();
  }
  if (isPromiseLike(value)) {
    return Mono.fromPromise(value);
  }
  return fallback;
}

function safeFlux(source: Flux<unknown>): Flux<unknown> {
  return source.map(safeValue).defaultIfEmpty(0);
}

function safeMono(source: Mono<unknown>): Mono<unknown> {
  return source.map(safeValue).defaultIfEmpty(0);
}

function safeValue(value: unknown): unknown {
  if (value === undefined || value === null) {
    return 0;
  }
  if (value instanceof Flux || value instanceof Mono) {
    return "[publisher]";
  }
  return value;
}

function baseFlux(): Flux<unknown> {
  return Flux.just(1);
}

function baseMono(): Mono<unknown> {
  return Mono.just(1);
}

function fluxSourceVariants(): Array<{ name: string; flux(): Flux<unknown>; fallback(): Flux<unknown> }> {
  return [
    {
      name: "delayed-async-iterable",
      flux: () => delayedFlux(1, 2),
      fallback: () => delayedFlux(1)
    },
    {
      name: "interval-take",
      flux: () => Flux.interval(0, Schedulers.timeout()).take(2).map(value => value + 1),
      fallback: () => Flux.just(1)
    },
    {
      name: "never-timeout-fallback",
      flux: () => Flux.never<unknown>().timeout(1, Flux.just(1), Schedulers.timeout()),
      fallback: () => Flux.just(1)
    },
    {
      name: "nested-delayed-flat-map",
      flux: () => delayedFlux(1, 2).flatMap(value => delayedFlux(value, Number(value) + 10), 1),
      fallback: () => Flux.just(1)
    }
  ];
}

function monoSourceVariants(): Array<{ name: string; mono(): Mono<unknown>; fallback(): Flux<unknown> }> {
  return [
    {
      name: "delayed-mono",
      mono: () => Mono.delay(0, Schedulers.timeout()).map(() => 1),
      fallback: () => Flux.just(1)
    },
    {
      name: "never-timeout-fallback",
      mono: () => Mono.never<unknown>().timeout(1, Mono.just(1), Schedulers.timeout()),
      fallback: () => Flux.just(1)
    },
    {
      name: "from-delayed-flux",
      mono: () => delayedFlux(1, 2).next(),
      fallback: () => Flux.just(1)
    }
  ];
}

function nestedPublisherVariants(): Array<{ name: string; flux(): Flux<unknown> }> {
  return [
    {
      name: "delayed",
      flux: () => delayedFlux(9, 10)
    },
    {
      name: "interval-take",
      flux: () => Flux.interval(0, Schedulers.timeout()).take(2).map(value => value + 9)
    },
    {
      name: "never-timeout-fallback",
      flux: () => Flux.never<unknown>().timeout(1, Flux.just(9), Schedulers.timeout())
    }
  ];
}

function delayedFlux(...values: readonly unknown[]): Flux<unknown> {
  return Flux.defer(() => Flux.from(delayedValues(values)));
}

async function* delayedValues(values: readonly unknown[]): AsyncIterable<unknown> {
  for (const value of values) {
    await timerTick();
    yield value;
  }
}

function timerTick(): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, 0));
}

function sortedNumbers(source: Flux<unknown>): Mono<number[]> {
  return source.collectList().map(values => values.map(Number).sort(numberComparator));
}

function noopSubscriber() {
  return {
    onSubscribe(subscription: { request(n: number): void }) {
      subscription.request(Number.POSITIVE_INFINITY);
    },
    onNext() {
      // no-op
    },
    onError(error: unknown) {
      throw error;
    },
    onComplete() {
      // no-op
    }
  };
}

function numberComparator(left: unknown, right: unknown): number {
  return Number(left) - Number(right);
}

function stringComparator(left: unknown, right: unknown): number {
  return String(left).localeCompare(String(right));
}

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return typeof (value as { then?: unknown })?.then === "function";
}

function isAsyncIterable(value: unknown): value is AsyncIterable<unknown> {
  return typeof (value as { [Symbol.asyncIterator]?: unknown })?.[Symbol.asyncIterator] === "function";
}

function isIterable(value: unknown): value is Iterable<unknown> {
  return typeof (value as { [Symbol.iterator]?: unknown })?.[Symbol.iterator] === "function";
}

function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error(`timed out after ${ms}ms`)), ms);
    promise.then(
      value => {
        clearTimeout(timeout);
        resolve(value);
      },
      error => {
        clearTimeout(timeout);
        reject(error);
      }
    );
  });
}

function javaOracleTraces(): Record<string, unknown> {
  return reactorFixture()["method-matrix"].traces;
}

function fixtureMethods(className: "Flux" | "Mono"): MethodFixture {
  const api = reactorFixture().api?.[className];
  if (!api) {
    throw new Error(`Missing ${className} API fixture. Run npm run fixtures:reactor to refresh test fixtures.`);
  }
  return api;
}

function reactorFixture(): {
  readonly api?: {
    readonly Flux?: MethodFixture;
    readonly Mono?: MethodFixture;
  };
  readonly "method-matrix": {
    readonly traces: Record<string, unknown>;
  };
} {
  const file = path.join(process.cwd(), "test", "fixtures", "reactor-oracle-matrix.json");
  return JSON.parse(fs.readFileSync(file, "utf8")) as {
    readonly api?: {
      readonly Flux?: MethodFixture;
      readonly Mono?: MethodFixture;
    };
    readonly "method-matrix": {
      readonly traces: Record<string, unknown>;
    };
  };
}

function missing(expected: Set<string>, actual: Set<string>): string[] {
  return [...expected].filter(name => !actual.has(name)).sort();
}
