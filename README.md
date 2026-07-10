# reactor-core-ts

Browser-first TypeScript implementation inspired by Project Reactor Core.

The runtime has no external dependencies and is built around standard Web APIs:
`AsyncIterable`, `AbortController`, `queueMicrotask`, `setTimeout` and
`requestAnimationFrame` when available.

## Install

```sh
npm install
npm run build
```

## Basic Usage

```ts
import { Flux, Mono, Sinks } from "reactor-core-ts";

const values = await Flux.range(1, 6)
  .filter(value => value % 2 === 0)
  .map(value => value * 10)
  .collectList()
  .block();

const result = await Mono.fromPromise(fetch("/api/user").then(r => r.json()))
  .timeout(3_000)
  .onErrorResume(() => Mono.just({ anonymous: true }))
  .block();

const sink = Sinks.many().replay().latest<number>();
sink.emitNext(1);
sink.emitNext(2);
sink.emitComplete();

console.log(await sink.asFlux().toArray()); // [2]
```

## Implemented Surface

- Reactive Streams primitives: `Publisher`, `Subscriber`, `Subscription`, `Disposable`.
- `Flux` factories: `just`, `from`, `fromArray`, `fromIterable`, `create`, `defer`,
  `deferContextual`, `range`, `interval`, `merge`, `concat`, `zip`, `combineLatest`,
  `empty`, `never`, `error`.
- `Flux` operators: `map`, `filter`, `handle`, `flatMap`, `concatMap`, `switchMap`,
  `mergeWith`, `concatWith`, `startWith`, `take`, `takeWhile`, `takeUntil`, `skip`,
  `skipWhile`, `distinct`, `distinctUntilChanged`, `buffer`, `window`, `scan`,
  `reduce`, `collectList`, `count`, `any`, `all`, `hasElement`, `next`, `single`,
  `singleOrEmpty`, `defaultIfEmpty`, `switchIfEmpty`, `doOnNext`, `doOnError`,
  `doOnComplete`, `doFinally`, `onErrorResume`, `onErrorReturn`, `onErrorMap`,
  `retry`, `repeat`, `delayElements`, `timeout`, `publishOn`, `subscribeOn`,
  `timestamp`, `elapsed`, `materialize`, `dematerialize`, `transform`,
  `transformDeferred`, `transformDeferredContextual`, `then`, `thenMany`,
  `thenReturn`, `zipWith`, `toArray`, `toPromise`.
- `Mono` factories and operators for zero-or-one sources, including `fromPromise`,
  `fromSupplier`, `fromCallable`, `fromRunnable`, `justOrEmpty`, `create`, `delay`,
  `when`, `firstWithSignal`, `firstWithValue`, `zip`, `flatMap`, `flatMapMany`.
- Browser schedulers: `immediate`, `microtask`, `timeout`, `animationFrame`,
  `single`, `parallel`, `boundedElastic`, `fromExecutor`.
- Immutable `Context`/`ContextView`.
- `Sinks.one`, `Sinks.empty`, `Sinks.many().unicast()`, `multicast()` and `replay()`.
- `Signal`, `SignalType`, `Hooks`, Reactor-style errors.

## Source Layout

- `src/publishers/*` contains the `Publisher` contract and Flux/Mono classes.
- `src/subscriptions/*` contains the `Subscriber`/`Subscription` contracts and subscription implementation.
- `src/index.ts` is the only file in the source root; public chunks live in folder `index.ts` files.
- `src/publishers/index.ts`, `src/sinks/index.ts`, `src/scheduler/index.ts` and other package folders expose their local public surface.
- `src/publishers/operators/*` contains focused operator packages.
- `src/publishers/operators/compat/*` contains Reactor API adapters expressed through existing primitives.
- `src/sinks/*` contains sink types, one/many implementations and specs separately.
- `scripts/check-api-parity.mjs` compares the current TS runtime against the local Java Reactor source.

## Public API And Chunking

The package root intentionally exports only:

```ts
Publisher;
Subscriber;
Subscription;
Flux;
Mono;
Sinks;
Schedulers;
```

Use focused subpaths when you want smaller chunks:

```ts
import { Flux } from "reactor-core-ts/flux";
import { Mono } from "reactor-core-ts/mono";
import { Sinks } from "reactor-core-ts/sinks";
import { Schedulers } from "reactor-core-ts/schedulers";
import type { Publisher, Subscriber, Subscription } from "reactor-core-ts/reactive-streams";
```

Operator packages are registered as side-effectful modules so bundlers do not
drop Flux/Mono prototype methods during tree shaking. `Sinks` uses the core
Flux/Mono classes directly, so importing `reactor-core-ts/sinks` does not pull
all Flux/Mono operator packages.

## Browser Semantics

This is not a byte-for-byte JVM port. It preserves the public programming model and
Reactive Streams contract where it maps to JavaScript, but uses `AsyncIterable` as the
internal execution model.

`Mono.block()` returns `Promise<T | undefined>` instead of blocking the current thread.
That is intentional: browsers cannot block the event loop safely.

Backpressure is represented by `Subscription.request(n)`. AsyncIterable sources may
prefetch one item to discover terminal completion without requiring an extra request.

## Verify

```sh
npm run typecheck
npm test
npm run build
npm run api:parity
```
