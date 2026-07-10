# reactor-core-ts

Browser-first TypeScript implementation inspired by Project Reactor Core.

`reactor-core-ts` gives you `Mono`, `Flux`, `Sinks`, `Schedulers` and immutable
`Context` for building lazy asynchronous pipelines in browser applications. The
runtime is intentionally small: no external runtime dependencies, no Node-only
APIs in the core path, and scheduling built on standard browser primitives such
as `queueMicrotask`, `setTimeout` and `requestAnimationFrame`.

The project is useful when you want a Reactor-like programming model in
TypeScript:

- `Mono<T>` is a lazy publisher that can complete with zero or one value.
- `Flux<T>` is a lazy publisher that can complete with zero, one or many values.
- `Sinks` let imperative code push values into a `Mono` or `Flux`.
- `Schedulers` control when work is executed.
- `Context` carries request-scoped metadata through a chain without adding more
  parameters to every function.

## Installation

For a consumer project:

```sh
npm install reactor-core-ts
```

For this repository:

```sh
npm install
npm run build
npm run ci
```

## Public Imports

Prefer the package root. Runtime exports are deliberately small, and Reactive
Streams contracts are exported as TypeScript types from the same root.

```ts
import { Context, Flux, Mono, Schedulers, Sinks } from "reactor-core-ts";
import type { Disposable, Publisher, Subscriber, Subscription } from "reactor-core-ts";
```

Use `import type` for `Publisher`, `Subscriber`, `Subscription` and
`Disposable`. They are compile-time contracts, not runtime objects.

## Quick Start

```ts
import { Flux, Mono, Sinks } from "reactor-core-ts";

const numbers = await Flux.range(1, 6)
  .filter(value => value % 2 === 0)
  .map(value => value * 10)
  .collectList()
  .block();

console.log(numbers); // [20, 40, 60]

const user = await Mono.fromPromise(fetch("/api/user").then(response => response.json()))
  .timeout(3_000)
  .onErrorResume(() => Mono.just({ anonymous: true }))
  .block();

const sink = Sinks.many().replay().latest<number>();
sink.emitNext(1);
sink.emitNext(2);
sink.emitComplete();

console.log(await sink.asFlux().toArray()); // [2]
```

Methods named `block`, `blockFirst` and `blockLast` do not block the JavaScript
thread. In this TypeScript/browser implementation they return `Promise`.

## What Reactive Streams Are

Reactive Streams are a simple contract for asynchronous data.

Think of a stream as a lazy pipeline:

1. A `Publisher` knows how to produce values.
2. A `Subscriber` receives values.
3. A `Subscription` connects them.
4. The subscriber calls `request(n)` to say how many values it is ready for.
5. The subscriber can call `cancel()` to stop the work.

The four basic signals are:

- `onSubscribe(subscription)`: the connection is ready.
- `onNext(value)`: one value arrived.
- `onError(error)`: the stream failed and is now finished.
- `onComplete()`: the stream finished successfully.

`Mono` and `Flux` are higher-level APIs built on this idea. You usually compose
operators such as `map`, `filter`, `flatMap`, `timeout`, `retry` and
`collectList` instead of writing a raw subscriber.

```ts
const result = await Flux.just("1", "2", "bad", "3")
  .flatMap(value => Mono.fromCallable(() => Number.parseInt(value, 10)))
  .filter(value => Number.isFinite(value))
  .collectList()
  .block();
```

### Why Use Reactive Streams

- Lazy by default: nothing runs until somebody subscribes or awaits a terminal
  operation.
- One model for sync values, promises, async iterables and custom publishers.
- Cancellation is part of the contract.
- Backpressure is part of the contract at the `Subscription` level.
- Errors are values in the pipeline, so fallback and retry logic stays local.
- `Context` can carry request metadata through nested async work.
- The same operators work for tiny one-value tasks and long-lived streams.

### Tradeoffs

- The mental model is more complex than `await fetch(...)`.
- Debugging long chains takes discipline. Use small functions and meaningful
  operator boundaries.
- It is not free. Every operator adds a small amount of work.
- Browser APIs cannot always be paused. Some external sources may still produce
  values faster than your code consumes them.
- Some Reactor compatibility operators are intentionally lightweight in the
  browser implementation. For example, `metrics`, `name`, `tag`, `parallel` and
  some backpressure operators mostly preserve API shape.
- For one tiny promise, plain `await` may be clearer.

## When To Use Mono, Flux, Sinks And Context

| Need | Use |
| --- | --- |
| One async result or no result | `Mono<T>` |
| Many values over time | `Flux<T>` |
| Adapt callback or event code into a stream | `Sinks` or `Flux.create` |
| Carry request/user/correlation metadata | `Context` |
| Change async execution timing | `Schedulers` |
| Implement a low-level stream bridge | `Publisher`, `Subscriber`, `Subscription` |

## Low-Level Reactive Streams Example

Most application code should use `Mono` and `Flux`. The low-level contracts are
there for adapters and integrations.

```ts
import { Flux } from "reactor-core-ts";
import type { Subscriber, Subscription } from "reactor-core-ts";

const subscriber: Subscriber<number> = {
  onSubscribe(subscription: Subscription) {
    subscription.request(2);
  },
  onNext(value) {
    console.log("value", value);
  },
  onError(error) {
    console.error(error);
  },
  onComplete() {
    console.log("done");
  }
};

Flux.range(1, 3).subscribe(subscriber);
```

## Mono Static Factories

Each row includes a small example. The examples are intentionally short so the
table can stay usable as a reference.

| Static method | What it does and why | Example |
| --- | --- | --- |
| `Mono.just(value)` | Create a `Mono` with one known non-null value. | `await Mono.just(1).block()` |
| `Mono.empty()` | Complete without a value. Useful for optional work. | `await Mono.empty<number>().defaultIfEmpty(0).block()` |
| `Mono.never()` | Never emit and never complete. Useful in timeout tests. | `Mono.never<number>().timeout(100, Mono.just(0))` |
| `Mono.error(error)` | Fail when subscribed. | `Mono.error(new Error("boom")).onErrorReturn(0)` |
| `Mono.defer(supplier)` | Build the real source per subscription. | `Mono.defer(() => Mono.just(Date.now()))` |
| `Mono.deferContextual(supplier)` | Build the source from `Context`. | `Mono.deferContextual(ctx => Mono.just(ctx.get("id")))` |
| `Mono.first(...sources)` | Use the first source that signals. | `Mono.first(Mono.delay(10), Mono.just(1))` |
| `Mono.from(input)` | Adapt a publisher, iterable, async iterable or promise and keep at most one value. | `Mono.from(Promise.resolve(1))` |
| `Mono.fromPromise(promise)` | Adapt a `PromiseLike<T>`. | `Mono.fromPromise(fetch("/api"))` |
| `Mono.fromCompletionStage(stage)` | Reactor-compatible promise adapter. | `Mono.fromCompletionStage(() => Promise.resolve(1))` |
| `Mono.fromFuture(future)` | Reactor-compatible future adapter. | `Mono.fromFuture(() => Promise.resolve(1))` |
| `Mono.fromDirect(input)` | Adapt a source directly and keep Mono cardinality. | `Mono.fromDirect(Flux.just(1, 2))` |
| `Mono.fromSupplier(supplier)` | Run a synchronous supplier on subscription. Nullish means empty. | `Mono.fromSupplier(() => localStorage.getItem("token"))` |
| `Mono.fromCallable(callable)` | Run a synchronous callable and capture thrown errors. | `Mono.fromCallable(() => JSON.parse(text))` |
| `Mono.fromRunnable(runnable)` | Run side-effect work and complete empty. | `Mono.fromRunnable(() => cache.clear())` |
| `Mono.justOrEmpty(value)` | Emit the value only when it is not `null` or `undefined`. | `Mono.justOrEmpty(maybeUser)` |
| `Mono.ignoreElements(source)` | Wait for a source to finish and drop its values. | `Mono.ignoreElements(Flux.just(1, 2))` |
| `Mono.sequenceEqual(left, right)` | Compare two publishers item by item. | `Mono.sequenceEqual(Flux.just(1), Flux.just(1))` |
| `Mono.create(callback)` | Bridge callback-style code into a `Mono`. | `Mono.create(sink => sink.success(1))` |
| `Mono.delay(duration, scheduler?)` | Emit `0` after a delay. | `Mono.delay({ seconds: 1 })` |
| `Mono.when(...sources)` | Complete when all sources complete, failing fast on error. | `Mono.when(saveA(), saveB())` |
| `Mono.whenDelayError(...sources)` | Wait for all sources, then report delayed errors. | `Mono.whenDelayError(taskA(), taskB())` |
| `Mono.firstWithSignal(...sources)` | Mirror the first source to signal value, error or completion. | `Mono.firstWithSignal(slow(), fast())` |
| `Mono.firstWithValue(...sources)` | Use the first source that emits a value. Empty sources are skipped. | `Mono.firstWithValue(Mono.empty(), Mono.just(1))` |
| `Mono.zip(sources, combinator?)` | Wait for all Monos and combine their values. | `Mono.zip([userMono, rolesMono], ([u, r]) => ({ u, r }))` |
| `Mono.zipDelayError(sources, combinator?)` | Zip values but delay errors until all sources finish. | `Mono.zipDelayError([a, b])` |
| `Mono.using(resource, source, cleanup)` | Create and clean up a synchronous resource. | `Mono.using(open, r => Mono.just(r.id), close)` |
| `Mono.usingWhen(resourceMono, source, cleanup...)` | Create and clean up an async resource. | `Mono.usingWhen(connect(), c => query(c), closeAsync)` |

## Flux Static Factories

| Static method | What it does and why | Example |
| --- | --- | --- |
| `Flux.just(...values)` | Emit known values and complete. | `Flux.just(1, 2, 3)` |
| `Flux.fromArray(values)` | Emit an array. | `Flux.fromArray([1, 2])` |
| `Flux.fromStream(stream)` | Emit an iterable or iterable supplier. | `Flux.fromStream(() => new Set([1, 2]))` |
| `Flux.fromIterable(values)` | Emit any iterable. | `Flux.fromIterable(new Map().keys())` |
| `Flux.from(input)` | Adapt publisher, iterable, async iterable or promise. | `Flux.from(fetch("/api").then(r => r.json()))` |
| `Flux.fromEvent(target, type, options?)` | Listen to a browser `EventTarget` and remove the listener on cancellation. | `Flux.fromEvent(button, "click").take(1)` |
| `Flux.fromWebSocket(socketOrUrl, options?)` | Listen to browser WebSocket messages and clean up listeners on cancellation. | `Flux.fromWebSocket<string>("wss://example.com/socket")` |
| `Flux.empty()` | Complete without values. | `Flux.empty<number>()` |
| `Flux.never()` | Never emit and never complete. | `Flux.never().timeout(100, Flux.just("fallback"))` |
| `Flux.error(error)` | Fail when subscribed. | `Flux.error(new Error("boom"))` |
| `Flux.defer(supplier)` | Create the real source for each subscription. | `Flux.defer(() => Flux.just(Date.now()))` |
| `Flux.deferContextual(supplier)` | Create the source from `Context`. | `Flux.deferContextual(ctx => Flux.just(ctx.get("id")))` |
| `Flux.create(callback)` | Bridge imperative multi-value code. | `Flux.create(s => { s.next(1); s.complete(); })` |
| `Flux.push(callback)` | Alias-style bridge for push sources. | `Flux.push(s => s.next(1))` |
| `Flux.generate(generator)` | Produce values synchronously through a sink. | `Flux.generate(s => { s.next(1); s.complete(); })` |
| `Flux.range(start, count)` | Emit consecutive numbers. | `Flux.range(10, 3)` |
| `Flux.interval(period, scheduler?)` | Emit increasing numbers on a timer. | `Flux.interval(100).take(3)` |
| `Flux.merge(...sources)` | Subscribe to sources and interleave values. | `Flux.merge(Flux.just(1), Flux.just(2))` |
| `Flux.mergeDelayError(...sources)` | Merge while delaying errors. | `Flux.mergeDelayError(a, b)` |
| `Flux.mergeSequential(...sources)` | Subscribe eagerly but emit in source order. | `Flux.mergeSequential(a, b)` |
| `Flux.mergeSequentialDelayError(...sources)` | Sequential merge with delayed errors. | `Flux.mergeSequentialDelayError(a, b)` |
| `Flux.mergeComparing(comparator, ...sources)` | Merge comparable sources by comparator. | `Flux.mergeComparing((a, b) => a - b, a, b)` |
| `Flux.mergeComparingDelayError(comparator, ...sources)` | Comparator merge with delayed errors. | `Flux.mergeComparingDelayError(compare, a, b)` |
| `Flux.mergeOrdered(comparator, ...sources)` | Ordered merge by comparator. | `Flux.mergeOrdered(compare, a, b)` |
| `Flux.mergePriority(comparator, ...sources)` | Priority-style merge by comparator. | `Flux.mergePriority(compare, a, b)` |
| `Flux.mergePriorityDelayError(comparator, ...sources)` | Priority merge with delayed errors. | `Flux.mergePriorityDelayError(compare, a, b)` |
| `Flux.concat(...sources)` | Run sources one after another. | `Flux.concat(Flux.just(1), Flux.just(2))` |
| `Flux.concatDelayError(...sources)` | Concat and delay errors. | `Flux.concatDelayError(a, b)` |
| `Flux.first(...sources)` | Mirror the first source that signals. | `Flux.first(slow, fast)` |
| `Flux.firstWithSignal(...sources)` | Same idea with explicit Reactor name. | `Flux.firstWithSignal(a, b)` |
| `Flux.firstWithValue(...sources)` | Use first source that emits a value. | `Flux.firstWithValue(Flux.empty(), Flux.just(1))` |
| `Flux.zip(sources, combinator?)` | Combine values position by position. | `Flux.zip([Flux.just(1), Flux.just(2)], ([a, b]) => a + b)` |
| `Flux.combineLatest(sources, combinator?)` | Combine latest values whenever a source updates. | `Flux.combineLatest([a, b])` |
| `Flux.switchOnNext(sources)` | Switch to the latest inner publisher. | `Flux.switchOnNext(Flux.just(Flux.just(1)))` |
| `Flux.using(resource, source, cleanup)` | Use and clean up a synchronous resource. | `Flux.using(open, r => Flux.just(r.id), close)` |
| `Flux.usingWhen(resource, source, cleanup...)` | Use and clean up an async resource. | `Flux.usingWhen(connect(), c => queryMany(c), closeAsync)` |

### Browser Source Examples

`Flux.fromEvent` is the small, direct bridge for DOM events. It adds one event
listener per subscription and removes it when the subscription is cancelled, for
example by `take(1)`, `timeout`, or manual cancellation.

```ts
const button = document.querySelector<HTMLButtonElement>("#save")!;

Flux.fromEvent(button, "click")
  .map(event => ({ x: event.clientX, y: event.clientY }))
  .take(1)
  .subscribe(point => console.log("clicked", point));
```

Use the generic overload when the target is only known as a plain
`EventTarget`.

```ts
Flux.fromEvent<InputEvent>(searchInput, "input")
  .map(event => (event.currentTarget as HTMLInputElement).value)
  .filter(value => value.length > 1)
  .subscribe(value => console.log("search", value));
```

`Flux.fromWebSocket` turns browser `message` events into stream values. Passing a
URL makes the Flux own the socket and close it on cancellation. Passing an
existing socket leaves it open unless `closeOnCancel: true` is set.

```ts
Flux.fromWebSocket<string>("wss://example.com/feed")
  .map(event => JSON.parse(event.data) as { type: string })
  .filter(message => message.type === "price")
  .take(10)
  .subscribe(message => console.log(message));
```

```ts
const socket = new WebSocket("wss://example.com/feed");

Flux.fromWebSocket<ArrayBuffer>(socket, {
  binaryType: "arraybuffer",
  closeOnCancel: true
})
  .map(event => event.data.byteLength)
  .timeout(5000)
  .subscribe(size => console.log("bytes", size));
```

## Mono Methods

`Mono` has at most one value. Many methods preserve that shape and return
`Mono`. Some methods intentionally return `Flux`, for example `repeat`,
`flatMapMany`, `concatWith` and `mergeWith`, because they can produce many
values.

Every row below includes a minimal example.

| Method | What it does and why | Example |
| --- | --- | --- |
| `subscribe` | Start the `Mono` with callbacks or a `Subscriber`. | `Mono.just(1).subscribe(console.log)` |
| `iterate` | Low-level iteration with `AbortSignal` and `Context`. | `Mono.just(1).iterate(signal, Context.empty())` |
| `toIterable` | Expose the Mono as an async iterable. | `for await (const v of Mono.just(1).toIterable()) {}` |
| `toArray` | Collect zero or one value into an array. | `await Mono.just(1).toArray()` |
| `toPromise` | Resolve to the value or `undefined`. | `await Mono.empty<number>().toPromise()` |
| `block` | Promise for the value or `undefined`. | `await Mono.just(1).block()` |
| `and` | Wait for this and another source, drop values. | `Mono.just(1).and(save())` |
| `as` | Pass the Mono to a custom transformer and return anything. | `Mono.just(1).as(source => source.block())` |
| `cast` | Type-cast the value. | `Mono.just("x").cast<string>()` |
| `concatWith` | Emit this value, then another publisher. Returns `Flux`. | `Mono.just(1).concatWith(Flux.just(2))` |
| `doFinally` | Run a callback after complete, error or cancel. | `Mono.just(1).doFinally(signal => console.log(signal))` |
| `doOnError` | Observe errors without handling them. | `Mono.error(error).doOnError(console.error)` |
| `doOnNext` | Observe the value without changing it. | `Mono.just(1).doOnNext(console.log)` |
| `doOnSuccess` | Observe success with value or `undefined`. | `Mono.empty().doOnSuccess(value => console.log(value))` |
| `doOnTerminate` | Run on complete or error. | `Mono.just(1).doOnTerminate(() => console.log("done"))` |
| `elapsed` | Pair the value with elapsed time since subscription. | `Mono.delay(10).elapsed()` |
| `flux` | Convert to `Flux`. | `Mono.just(1).flux()` |
| `handle` | Map, drop or error through a synchronous sink. | `Mono.just(1).handle((v, s) => s.next(v * 2))` |
| `hasElement` | Tell whether a value exists. | `Mono.empty().hasElement()` |
| `ignoreElement` | Drop the value and keep only completion or error. | `Mono.just(1).ignoreElement()` |
| `materialize` | Convert terminal/value signals into `Signal` objects. | `Mono.just(1).materialize()` |
| `mergeWith` | Merge this value with another source. Returns `Flux`. | `Mono.just(1).mergeWith(Flux.just(2))` |
| `onErrorMap` | Convert one error into another. | `Mono.error("x").onErrorMap(e => new Error(String(e)))` |
| `onErrorResume` | Recover with another publisher. | `Mono.error(error).onErrorResume(() => Mono.just(0))` |
| `onErrorReturn` | Recover with one fixed value. | `Mono.error(error).onErrorReturn(0)` |
| `publishOn` | Move downstream value delivery to a scheduler. | `Mono.just(1).publishOn(Schedulers.microtask())` |
| `repeat` | Re-subscribe and return a `Flux` of repeated values. | `Mono.just(1).repeat(2)` |
| `retry` | Retry after errors. | `unstableMono.retry(3)` |
| `single` | Assert that the Mono has one value. | `Mono.just(1).single()` |
| `subscribeOn` | Start subscription on a scheduler. | `Mono.fromCallable(load).subscribeOn(Schedulers.timeout())` |
| `subscribeWith` | Subscribe and return the same subscriber. | `Mono.just(1).subscribeWith(subscriber)` |
| `take` | Keep the value if present. | `Mono.just(1).take()` |
| `thenEmpty` | Wait for this, then wait for another empty source. | `Mono.just(1).thenEmpty(save())` |
| `thenMany` | Ignore this value, then run another publisher. | `Mono.just(1).thenMany(Flux.range(1, 3))` |
| `timeout` | Fail or switch to fallback if too slow. | `Mono.never().timeout(100, Mono.just(0))` |
| `timestamp` | Pair the value with scheduler time. | `Mono.just("a").timestamp()` |
| `transform` | Apply an assembly-time transformer. | `Mono.just(1).transform(m => m.map(v => v + 1))` |
| `transformDeferred` | Apply the transformer per subscription. | `Mono.just(1).transformDeferred(m => m.map(v => v + Date.now()))` |
| `transformDeferredContextual` | Transform per subscription with `Context`. | `Mono.just(1).transformDeferredContextual((m, ctx) => m.map(v => [v, ctx.get("id")]))` |
| `cache` | Reuse the result for later subscribers. | `expensiveMono.cache()` |
| `cacheInvalidateIf` | Cache with predicate-shaped invalidation support. | `expensiveMono.cacheInvalidateIf(value => value.stale)` |
| `cacheInvalidateWhen` | Cache with trigger-shaped invalidation support. | `expensiveMono.cacheInvalidateWhen(value => refreshSignal(value))` |
| `cancelOn` | Schedule cancellation work. | `Mono.never().cancelOn(Schedulers.timeout())` |
| `checkpoint` | Add a Reactor-style debugging boundary. | `Mono.just(1).checkpoint("load user")` |
| `contextCapture` | Reactor-compatible context capture hook. | `Mono.just(1).contextCapture()` |
| `contextWrite` | Add or change `Context` for upstream operators. | `Mono.deferContextual(ctx => Mono.just(ctx.get("id"))).contextWrite(Context.of("id", 1))` |
| `delayElement` | Delay the value. | `Mono.just(1).delayElement(100)` |
| `delaySubscription` | Delay subscription start. | `Mono.just(1).delaySubscription(100)` |
| `delayUntil` | Wait for a trigger derived from the value. | `Mono.just(user).delayUntil(user => audit(user))` |
| `dematerialize` | Convert `Signal` objects back into normal signals. | `Mono.just(signal).dematerialize()` |
| `doAfterTerminate` | Run after completion or error has been sent. | `Mono.just(1).doAfterTerminate(cleanup)` |
| `doFirst` | Run before subscription work starts. | `Mono.just(1).doFirst(() => console.log("first"))` |
| `doOnCancel` | Observe cancellation. | `Mono.never().doOnCancel(cleanup)` |
| `doOnDiscard` | Observe discarded values of a type. | `Mono.just(file).doOnDiscard(File, closeFile)` |
| `doOnEach` | Observe every `Signal`. | `Mono.just(1).doOnEach(signal => console.log(signal.type))` |
| `doOnRequest` | Observe downstream demand. | `Mono.just(1).doOnRequest(n => console.log(n))` |
| `doOnSubscribe` | Observe subscription creation. | `Mono.just(1).doOnSubscribe(s => s.request(1))` |
| `expand` | Breadth-first recursive expansion. Returns `Flux`. | `Mono.just(1).expand(v => v < 3 ? Mono.just(v + 1) : Mono.empty())` |
| `expandDeep` | Depth-first recursive expansion. Returns `Flux`. | `Mono.just(1).expandDeep(nextNode)` |
| `filterWhen` | Keep the value only if an async predicate emits true. | `Mono.just(user).filterWhen(user => canView(user))` |
| `flatMapIterable` | Turn one value into many iterable values. | `Mono.just([1, 2]).flatMapIterable(values => values)` |
| `hide` | Hide implementation identity. | `Mono.just(1).hide()` |
| `log` | Debug signals to `console.debug`. | `Mono.just(1).log("user")` |
| `mapNotNull` | Map and drop `null` or `undefined`. | `Mono.just(user).mapNotNull(user => user.name)` |
| `metrics` | Reactor-compatible metrics hook placeholder. | `Mono.just(1).metrics()` |
| `name` | Reactor-compatible name metadata hook. | `Mono.just(1).name("loadUser")` |
| `ofType` | Keep the value only if it is an instance of a class. | `Mono.just(value).ofType(User)` |
| `onErrorComplete` | Turn matching errors into empty completion. | `Mono.error(error).onErrorComplete()` |
| `onErrorContinue` | Observe an error and continue where supported. | `Mono.error(error).onErrorContinue((e, v) => console.log(e))` |
| `onErrorStop` | Restore stop-on-error behavior. | `Mono.just(1).onErrorStop()` |
| `onTerminateDetach` | Reactor-compatible detach hook. | `Mono.just(1).onTerminateDetach()` |
| `or` | Race with another publisher. | `Mono.never().or(Mono.just(1))` |
| `publish` | Share this Mono with a transformer. | `Mono.just(1).publish(shared => shared.map(v => v + 1))` |
| `repeatWhen` | Repeat under companion control. Returns `Flux`. | `Mono.just(1).repeatWhen(signals => signals.take(1))` |
| `repeatWhenEmpty` | Repeat when empty. | `Mono.empty<number>().repeatWhenEmpty(signals => signals.take(1))` |
| `retryWhen` | Retry under companion control. | `unstableMono.retryWhen(errors => errors.take(3))` |
| `share` | Share the Mono result shape. | `Mono.just(1).share()` |
| `singleOptional` | Emit the value or `undefined`. | `Mono.empty<number>().singleOptional()` |
| `tag` | Reactor-compatible metadata hook. | `Mono.just(1).tag("kind", "demo")` |
| `takeUntilOther` | Stop when another publisher signals. | `Mono.never().takeUntilOther(Mono.delay(100))` |
| `tap` | Observe lifecycle through a listener object. | `Mono.just(1).tap({ onNext: console.log })` |
| `timed` | Wrap the value in timing metadata. | `Mono.just(1).timed()` |
| `toFuture` | Promise alias for the value or `undefined`. | `await Mono.just(1).toFuture()` |
| `zipWhen` | Generate a second source from the first value and combine. | `Mono.just(user).zipWhen(user => loadRoles(user.id))` |
| `map` | Transform the value. | `Mono.just(1).map(v => v + 1)` |
| `flatMap` | Transform the value into another single-value source. | `Mono.just(1).flatMap(v => Mono.just(v + 1))` |
| `flatMapMany` | Transform the value into a many-value source. | `Mono.just(3).flatMapMany(n => Flux.range(1, n))` |
| `filter` | Keep the value only when the predicate is true. | `Mono.just(1).filter(v => v > 0)` |
| `defaultIfEmpty` | Replace empty completion with a value. | `Mono.empty<number>().defaultIfEmpty(0)` |
| `switchIfEmpty` | Replace empty completion with another source. | `Mono.empty<number>().switchIfEmpty(Mono.just(1))` |
| `then` | Ignore the value and complete empty. | `Mono.just(1).then()` |
| `thenReturn` | Ignore the value and emit a fixed value. | `Mono.just(1).thenReturn("done")` |
| `zipWith` | Combine this value with another publisher. | `Mono.just(1).zipWith(Mono.just(2), (a, b) => a + b)` |

## Flux Methods

`Flux` is for zero to many values. Every row includes a small example.

### Flux Core And Terminal Helpers

| Method | What it does and why | Example |
| --- | --- | --- |
| `subscribe` | Start the stream with callbacks or a `Subscriber`. | `Flux.just(1).subscribe(console.log)` |
| `iterate` | Low-level iteration with `AbortSignal` and `Context`. | `Flux.just(1).iterate(signal, Context.empty())` |
| `toIterable` | Expose as an async iterable. | `for await (const v of Flux.just(1).toIterable()) {}` |
| `toStream` | Alias-style async iterable export. | `for await (const v of Flux.just(1).toStream()) {}` |
| `toArray` | Collect all values into an array. | `await Flux.range(1, 3).toArray()` |
| `toPromise` | Resolve to the last value or `undefined`. | `await Flux.just(1, 2).toPromise()` |
| `blockFirst` | Promise for the first value. | `await Flux.just(1, 2).blockFirst()` |
| `blockLast` | Promise for the last value. | `await Flux.just(1, 2).blockLast()` |
| `subscribeWith` | Subscribe and return the same subscriber. | `Flux.just(1).subscribeWith(subscriber)` |
| `as` | Pass the Flux to a custom transformer and return anything. | `Flux.just(1).as(source => source.toArray())` |

### Flux Transform And Selection Methods

| Method | What it does and why | Example |
| --- | --- | --- |
| `map` | Transform each value. | `Flux.just(1).map(v => v + 1)` |
| `cast` | Type-cast values. | `Flux.just("a").cast<string>()` |
| `filter` | Keep values matching a predicate. | `Flux.range(1, 5).filter(v => v % 2 === 0)` |
| `filterWhen` | Keep values matching an async predicate. | `Flux.just(user).filterWhen(u => canView(u))` |
| `handle` | Map, drop or error through a synchronous sink. | `Flux.just(1).handle((v, s) => s.next(v * 2))` |
| `mapNotNull` | Map and drop `null` or `undefined`. | `Flux.just("a", "").mapNotNull(v => v || undefined)` |
| `ofType` | Keep only instances of a class. | `Flux.just(value).ofType(User)` |
| `take` | Keep the first `n` values. | `Flux.range(1, 10).take(3)` |
| `takeLast` | Keep the last `n` values. | `Flux.range(1, 10).takeLast(3)` |
| `takeWhile` | Keep values while a predicate is true. | `Flux.range(1, 10).takeWhile(v => v < 5)` |
| `takeUntil` | Keep values until a predicate becomes true. | `Flux.range(1, 10).takeUntil(v => v === 5)` |
| `takeUntilOther` | Stop when another publisher signals. | `Flux.interval(100).takeUntilOther(Mono.delay(500))` |
| `skip` | Drop the first `n` values. | `Flux.range(1, 5).skip(2)` |
| `skipLast` | Drop the last `n` values. | `Flux.range(1, 5).skipLast(2)` |
| `skipWhile` | Drop values while a predicate is true. | `Flux.range(1, 5).skipWhile(v => v < 3)` |
| `skipUntil` | Drop values until a predicate becomes true. | `Flux.range(1, 5).skipUntil(v => v === 3)` |
| `skipUntilOther` | Drop values until another publisher signals. | `Flux.interval(100).skipUntilOther(Mono.delay(300))` |
| `distinct` | Keep the first value for each key. | `Flux.just(1, 1, 2).distinct()` |
| `distinctUntilChanged` | Drop only adjacent duplicates. | `Flux.just(1, 1, 2, 1).distinctUntilChanged()` |
| `elementAt` | Pick a value by index. | `Flux.just("a", "b").elementAt(1)` |
| `defaultIfEmpty` | Emit a fallback value if empty. | `Flux.empty<number>().defaultIfEmpty(0)` |
| `switchIfEmpty` | Switch to another source if empty. | `Flux.empty<number>().switchIfEmpty(Flux.just(1))` |
| `materialize` | Turn stream signals into `Signal` values. | `Flux.just(1).materialize()` |
| `dematerialize` | Turn `Signal` values back into normal signals. | `Flux.just(signal).dematerialize()` |
| `transform` | Apply an assembly-time transformer. | `Flux.just(1).transform(f => f.map(v => v + 1))` |
| `transformDeferred` | Apply a transformer for each subscription. | `Flux.just(1).transformDeferred(f => f.map(v => v + Date.now()))` |
| `transformDeferredContextual` | Transform per subscription with `Context`. | `Flux.just(1).transformDeferredContextual((f, ctx) => f.map(v => [v, ctx.get("id")]))` |
| `hide` | Hide implementation identity. | `Flux.just(1).hide()` |

### Flux Flattening And Combining Methods

| Method | What it does and why | Example |
| --- | --- | --- |
| `flatMap` | Map each value to a publisher and merge results. | `Flux.just(1, 2).flatMap(v => Flux.just(v, v * 10))` |
| `flatMapDelayError` | Flat-map while delaying errors. | `Flux.just(1).flatMapDelayError(load)` |
| `flatMapSequential` | Flat-map with ordered output. | `Flux.just(1, 2).flatMapSequential(load)` |
| `flatMapSequentialDelayError` | Ordered flat-map with delayed errors. | `Flux.just(1, 2).flatMapSequentialDelayError(load)` |
| `flatMapIterable` | Map each value to an iterable and flatten. | `Flux.just([1, 2]).flatMapIterable(v => v)` |
| `concatMap` | Map each value to a publisher, one at a time. | `Flux.just(1, 2).concatMap(v => Flux.just(v, v + 10))` |
| `concatMapDelayError` | Concat-map while delaying errors. | `Flux.just(1, 2).concatMapDelayError(load)` |
| `concatMapIterable` | Map to iterables and concatenate them. | `Flux.just([1], [2]).concatMapIterable(v => v)` |
| `switchMap` | Switch to the newest inner publisher. | `Flux.just("a", "b").switchMap(load)` |
| `mergeWith` | Merge this source with others. | `Flux.just(1).mergeWith(Flux.just(2))` |
| `mergeComparingWith` | Merge with others using a comparator. | `Flux.just(1).mergeComparingWith((a, b) => a - b, Flux.just(2))` |
| `mergeOrderedWith` | Ordered comparator merge with this source. | `Flux.just(1).mergeOrderedWith((a, b) => a - b, Flux.just(2))` |
| `concatWith` | Append other publishers after this one. | `Flux.just(1).concatWith(Flux.just(2))` |
| `concatWithValues` | Append literal values. | `Flux.just(1).concatWithValues(2, 3)` |
| `startWith` | Prepend literal values. | `Flux.just(3).startWith(1, 2)` |
| `zipWith` | Zip this source with another publisher. | `Flux.just(1).zipWith(Flux.just(2), (a, b) => a + b)` |
| `zipWithIterable` | Zip this source with an iterable. | `Flux.just("a", "b").zipWithIterable([1, 2])` |
| `withLatestFrom` | Combine each value with latest value from another source. | `Flux.just(1).withLatestFrom(Flux.just(2))` |
| `switchOnFirst` | Inspect the first signal and choose a new source. | `Flux.just(1, 2).switchOnFirst((signal, source) => source)` |
| `or` | Race this source with another source. | `Flux.never().or(Flux.just(1))` |

### Flux Aggregation And Reduction Methods

| Method | What it does and why | Example |
| --- | --- | --- |
| `buffer` | Collect fixed-size arrays. | `Flux.range(1, 5).buffer(2)` |
| `bufferTimeout` | Buffer by max size or max time. | `Flux.interval(10).bufferTimeout(3, 100)` |
| `bufferUntil` | Buffer until a predicate matches. | `Flux.range(1, 5).bufferUntil(v => v % 2 === 0)` |
| `bufferUntilChanged` | Buffer adjacent values with the same key. | `Flux.just(1, 1, 2).bufferUntilChanged()` |
| `bufferWhen` | Open and close a buffer with publishers. | `Flux.just(1, 2).bufferWhen(Mono.just(0), () => Mono.delay(100))` |
| `bufferWhile` | Buffer while a predicate is true. | `Flux.range(1, 5).bufferWhile(v => v < 4)` |
| `window` | Split into fixed-size inner `Flux` windows. | `Flux.range(1, 5).window(2)` |
| `windowTimeout` | Window by max size or max time. | `Flux.interval(10).windowTimeout(3, 100)` |
| `windowUntil` | Window until a predicate matches. | `Flux.range(1, 5).windowUntil(v => v % 2 === 0)` |
| `windowUntilChanged` | Window adjacent values with the same key. | `Flux.just(1, 1, 2).windowUntilChanged()` |
| `windowWhen` | Open and close windows with publishers. | `Flux.just(1, 2).windowWhen(Mono.just(0), () => Mono.delay(100))` |
| `windowWhile` | Window while a predicate is true. | `Flux.range(1, 5).windowWhile(v => v < 4)` |
| `groupBy` | Group all values by key into grouped `Flux` values. | `Flux.just("a", "bb").groupBy(v => v.length)` |
| `scan` | Emit running accumulation. | `Flux.range(1, 3).scan((a, b) => a + b)` |
| `scanWith` | Emit running accumulation with supplied seed. | `Flux.range(1, 3).scanWith(() => 0, (a, b) => a + b)` |
| `reduce` | Reduce all values to one `Mono`. | `Flux.range(1, 3).reduce((a, b) => a + b)` |
| `reduceWith` | Reduce with a seed supplier. | `Flux.range(1, 3).reduceWith(() => 0, (a, b) => a + b)` |
| `collect` | Collect into a custom mutable container. | `Flux.just(1).collect(() => new Set<number>(), (set, v) => set.add(v))` |
| `collectList` | Collect all values into an array. | `Flux.range(1, 3).collectList()` |
| `collectMap` | Collect into a `Map` by key. | `Flux.just(user).collectMap(user => user.id)` |
| `collectMultimap` | Collect into a `Map<K, V[]>`. | `Flux.just(user).collectMultimap(user => user.role)` |
| `collectSortedList` | Collect and sort values. | `Flux.just(3, 1, 2).collectSortedList((a, b) => a - b)` |
| `sort` | Sort and re-emit values. | `Flux.just(3, 1, 2).sort((a, b) => a - b)` |
| `count` | Count values. | `Flux.just("a", "b").count()` |
| `any` | True if any value matches. | `Flux.range(1, 5).any(v => v > 3)` |
| `all` | True if all values match. | `Flux.range(1, 5).all(v => v > 0)` |
| `hasElement` | True if a specific value appears. | `Flux.just(1, 2).hasElement(2)` |
| `hasElements` | True if at least one value appears. | `Flux.empty().hasElements()` |
| `next` | Convert first value to `Mono`. | `Flux.just(1, 2).next()` |
| `single` | Expect one value, optionally with default. | `Flux.just(1).single()` |
| `singleOrEmpty` | Expect zero or one value. | `Flux.empty<number>().singleOrEmpty()` |
| `last` | Convert last value to `Mono`. | `Flux.just(1, 2).last()` |
| `ignoreElements` | Drop values and keep completion or error. | `Flux.just(1, 2).ignoreElements()` |
| `then` | Drop values and complete as `Mono<void>`. | `Flux.just(1, 2).then()` |
| `thenEmpty` | Wait for this, then another empty source. | `Flux.just(1).thenEmpty(save())` |
| `thenMany` | Wait for this, then run another publisher. | `Flux.just(1).thenMany(Flux.just(2))` |
| `thenReturn` | Wait for this, then emit one value. | `Flux.just(1).thenReturn("done")` |

### Flux Error And Repetition Methods

| Method | What it does and why | Example |
| --- | --- | --- |
| `onErrorResume` | Recover with another publisher. | `Flux.error(error).onErrorResume(() => Flux.just(0))` |
| `onErrorReturn` | Recover with a fixed value. | `Flux.error(error).onErrorReturn(0)` |
| `onErrorMap` | Convert an error into another error. | `Flux.error("x").onErrorMap(e => new Error(String(e)))` |
| `onErrorComplete` | Turn matching errors into completion. | `Flux.error(error).onErrorComplete()` |
| `onErrorContinue` | Observe an error and continue where supported. | `Flux.error(error).onErrorContinue((e, v) => console.log(e))` |
| `onErrorStop` | Restore stop-on-error behavior. | `Flux.just(1).onErrorStop()` |
| `retry` | Retry after failure. | `unstableFlux.retry(3)` |
| `retryWhen` | Retry under companion control. | `unstableFlux.retryWhen(errors => errors.take(3))` |
| `repeat` | Re-subscribe after completion. | `Flux.just(1).repeat(2)` |
| `repeatWhen` | Repeat under companion control. | `Flux.just(1).repeatWhen(signals => signals.take(1))` |

### Flux Lifecycle, Context And Metadata Methods

| Method | What it does and why | Example |
| --- | --- | --- |
| `cache` | Replay the result to later subscribers. | `loadUsers().cache()` |
| `share` | Share one subscription among subscribers where possible. | `Flux.interval(100).share()` |
| `shareNext` | Share and expose the next value as `Mono`. | `Flux.interval(100).shareNext()` |
| `publish` | Share with a transformer. | `Flux.just(1).publish(shared => shared.map(v => v + 1))` |
| `publishNext` | Publish the first value as a `Mono`. | `Flux.just(1, 2).publishNext()` |
| `replay` | Reactor-compatible replay hook. | `Flux.just(1).replay()` |
| `contextWrite` | Add or change `Context` for upstream operators. | `Flux.deferContextual(ctx => Flux.just(ctx.get("id"))).contextWrite(Context.of("id", 1))` |
| `contextCapture` | Reactor-compatible context capture hook. | `Flux.just(1).contextCapture()` |
| `doFirst` | Run before subscription work starts. | `Flux.just(1).doFirst(() => console.log("first"))` |
| `doOnSubscribe` | Observe subscription creation. | `Flux.just(1).doOnSubscribe(s => console.log(s))` |
| `doOnRequest` | Observe downstream demand. | `Flux.just(1).doOnRequest(n => console.log(n))` |
| `doOnNext` | Observe each value. | `Flux.just(1).doOnNext(console.log)` |
| `doOnError` | Observe errors. | `Flux.error(error).doOnError(console.error)` |
| `doOnComplete` | Observe successful completion. | `Flux.just(1).doOnComplete(() => console.log("done"))` |
| `doOnTerminate` | Run on complete or error. | `Flux.just(1).doOnTerminate(cleanup)` |
| `doAfterTerminate` | Run after termination was sent. | `Flux.just(1).doAfterTerminate(cleanup)` |
| `doFinally` | Run after complete, error or cancel. | `Flux.just(1).doFinally(signal => console.log(signal))` |
| `doOnCancel` | Observe cancellation. | `Flux.never().doOnCancel(cleanup)` |
| `doOnDiscard` | Observe discarded values of a type. | `Flux.just(file).doOnDiscard(File, closeFile)` |
| `doOnEach` | Observe every `Signal`. | `Flux.just(1).doOnEach(signal => console.log(signal.type))` |
| `tap` | Observe lifecycle with a listener object. | `Flux.just(1).tap({ onNext: console.log })` |
| `log` | Debug signals to `console.debug`. | `Flux.just(1).log("demo")` |
| `checkpoint` | Add a Reactor-style debugging boundary. | `Flux.just(1).checkpoint("after load")` |
| `name` | Reactor-compatible name metadata hook. | `Flux.just(1).name("numbers")` |
| `tag` | Reactor-compatible key/value metadata hook. | `Flux.just(1).tag("kind", "demo")` |
| `metrics` | Reactor-compatible metrics hook placeholder. | `Flux.just(1).metrics()` |
| `onTerminateDetach` | Reactor-compatible detach hook. | `Flux.just(1).onTerminateDetach()` |
| `limitRate` | Reactor-compatible request-shaping hook. | `Flux.range(1, 10).limitRate(2)` |
| `limitRequest` | Limit total requested values by taking `n`. | `Flux.range(1, 10).limitRequest(3)` |
| `onBackpressureBuffer` | Reactor-compatible backpressure buffer hook. | `Flux.just(1).onBackpressureBuffer()` |
| `onBackpressureDrop` | Reactor-compatible drop hook. | `Flux.just(1).onBackpressureDrop(v => console.log(v))` |
| `onBackpressureError` | Reactor-compatible backpressure error hook. | `Flux.just(1).onBackpressureError()` |
| `onBackpressureLatest` | Reactor-compatible latest-value hook. | `Flux.just(1).onBackpressureLatest()` |
| `cancelOn` | Schedule cancellation work. | `Flux.never().cancelOn(Schedulers.timeout())` |

### Flux Time, Scheduler And Coordination Methods

| Method | What it does and why | Example |
| --- | --- | --- |
| `delayElements` | Delay each value. | `Flux.just(1, 2).delayElements(100)` |
| `delaySequence` | Delay the whole sequence. | `Flux.just(1, 2).delaySequence(100)` |
| `delaySubscription` | Delay subscription start. | `Flux.just(1).delaySubscription(100)` |
| `delayUntil` | Wait for a trigger per value. | `Flux.just(user).delayUntil(user => audit(user))` |
| `timeout` | Fail or switch to fallback if values are too slow. | `Flux.never().timeout(100, Flux.just(0))` |
| `publishOn` | Move downstream delivery to a scheduler. | `Flux.just(1).publishOn(Schedulers.microtask())` |
| `subscribeOn` | Start subscription on a scheduler. | `Flux.from(fetchData()).subscribeOn(Schedulers.timeout())` |
| `timestamp` | Pair each value with scheduler time. | `Flux.just("a").timestamp()` |
| `elapsed` | Pair each value with elapsed time since previous value. | `Flux.just("a").elapsed()` |
| `timed` | Wrap each value in timing metadata. | `Flux.just(1).timed()` |
| `sample` | Emit latest values when a timer or sampler signals. | `Flux.interval(10).sample(100)` |
| `sampleFirst` | Emit first value in each time window. | `Flux.interval(10).sampleFirst(100)` |
| `sampleTimeout` | Emit a value if its throttler completes before a newer value arrives. | `Flux.just("a").sampleTimeout(() => Mono.delay(100))` |
| `groupJoin` | Join with grouped right-side values. | `left.groupJoin(right, lEnd, rEnd, (l, rs) => [l, rs])` |
| `join` | Join this source with another source. | `left.join(right, lEnd, rEnd, (l, r) => [l, r])` |
| `index` | Add a zero-based index. | `Flux.just("a", "b").index()` |
| `expand` | Breadth-first recursive expansion. | `Flux.just(1).expand(v => v < 3 ? Flux.just(v + 1) : Flux.empty())` |
| `expandDeep` | Depth-first recursive expansion. | `Flux.just(root).expandDeep(node => node.children)` |
| `parallel` | Reactor-compatible parallel hook; browser version stays lightweight. | `Flux.range(1, 3).parallel()` |

## Context

`Context` is an immutable key/value store that travels with a subscription. It
is useful for metadata that many functions need but that should not be threaded
through every function parameter.

Good examples:

- request id
- auth/session data
- tenant id
- locale
- tracing data

Bad examples:

- large mutable objects
- frequently changing state
- data that belongs in the actual stream value

### Context Basics

```ts
import { Context, Mono } from "reactor-core-ts";

const context = Context.empty()
  .put("requestId", "req-123")
  .put("locale", "en");

console.log(context.get("requestId")); // "req-123"
console.log(context.getOrDefault("missing", "fallback")); // "fallback"
console.log(context.hasKey("locale")); // true
```

`Context` is immutable. `put`, `putAll`, `putNonNull` and `delete` return a new
context.

```ts
const base = Context.of("requestId", "req-1");
const next = base.put("userId", 42);

console.log(base.hasKey("userId")); // false
console.log(next.get("userId")); // 42
```

### Context API

| Method | What it does | Example |
| --- | --- | --- |
| `Context.empty()` | Shared empty context. | `Context.empty()` |
| `Context.of(...)` | Build from key/value pairs. | `Context.of("id", 1, "role", "admin")` |
| `Context.from(entries)` | Build from an iterable or object. | `Context.from({ id: 1 })` |
| `put(key, value)` | Return a context with one value added or replaced. | `ctx.put("id", 1)` |
| `putNonNull(key, value)` | Add only when value is not `null` or `undefined`. | `ctx.putNonNull("id", maybeId)` |
| `putAll(other)` | Merge another context or entries. | `ctx.putAll(Context.of("role", "admin"))` |
| `delete(key)` | Return a context without a key. | `ctx.delete("id")` |
| `readOnly()` | Return a read-only `ContextView`. | `ctx.readOnly()` |
| `get(key)` | Read a required value or throw. | `ctx.get<string>("id")` |
| `getOrDefault(key, fallback)` | Read a value or fallback. | `ctx.getOrDefault("id", "none")` |
| `getOrEmpty(key)` | Read a value or `undefined`. | `ctx.getOrEmpty("id")` |
| `hasKey(key)` | Check whether a key exists. | `ctx.hasKey("id")` |
| `isEmpty()` | Check whether the context is empty. | `ctx.isEmpty()` |
| `size()` | Count entries. | `ctx.size()` |
| `forEach(consumer)` | Iterate entries. | `ctx.forEach((key, value) => console.log(key, value))` |
| `stream()` | Get an iterable of entries. | `[...ctx.stream()]` |
| `toMap()` | Get a read-only `Map`. | `ctx.toMap()` |

### Context In A Publisher

Use `contextWrite` near subscription side and `deferContextual` where you need
to read the values.

```ts
const message = await Mono.deferContextual(context =>
  Mono.just(`request=${context.get("requestId")}`)
)
  .contextWrite(Context.of("requestId", "req-123"))
  .block();

console.log(message); // "request=req-123"
```

Context flows through nested publishers:

```ts
const values = await Flux.just(1, 2)
  .flatMap(value =>
    Mono.deferContextual(context =>
      Mono.just(`${context.get("requestId")}:${value}`)
    )
  )
  .contextWrite(Context.of("requestId", "req-123"))
  .collectList()
  .block();

console.log(values); // ["req-123:1", "req-123:2"]
```

Use functions with `contextWrite` when you want to derive from an existing
context:

```ts
const value = await Mono.deferContextual(context =>
  Mono.just(context.get("requestId"))
)
  .contextWrite(context => context.put("requestId", "inner"))
  .block();
```

## Sinks

A `Sink` is an imperative bridge. Use it when values come from callbacks,
browser events, WebSocket handlers or other code that pushes data into your app.

Normal `Mono` and `Flux` are usually cold: they do work when subscribed. A sink
is usually hot: you call `emitNext`, `emitValue`, `emitComplete` or `emitError`
from the outside.

### Emission Styles

Sink APIs come in two styles:

| Style | Behavior | Example |
| --- | --- | --- |
| `tryEmit...` | Return an `EmitResult`; you decide what to do. | `sink.tryEmitNext(1)` |
| `emit...` | Use an emission failure handler and throw only when the failure is not handled. | `sink.emitNext(1)` |

Common results:

| Result | Meaning |
| --- | --- |
| `OK` | The signal was accepted. |
| `FAIL_TERMINATED` | The sink already completed or failed. |
| `FAIL_ZERO_SUBSCRIBER` | This sink variant requires a subscriber but none exists. |
| `FAIL_OVERFLOW` | Buffer capacity was exhausted. |
| `FAIL_CANCELLED` | The sink was cancelled. |
| `FAIL_NON_SERIALIZED` | Concurrent emission was detected. |

### Empty And One Sinks

| Sink | What it is for | Example |
| --- | --- | --- |
| `Sinks.empty<T>()` | Only complete or fail a `Mono`; it never emits a value. | `const sink = Sinks.empty<void>(); sink.emitEmpty(); await sink.asMono().block();` |
| `Sinks.one<T>()` | Complete a `Mono` with one value, empty completion or error. | `const sink = Sinks.one<number>(); sink.emitValue(1); await sink.asMono().block();` |

`Sinks.one` methods:

```ts
const sink = Sinks.one<number>();

sink.tryEmitValue(1);        // Emit one value.
sink.tryEmitEmpty();         // Complete without a value.
sink.tryEmitError(error);    // Fail the Mono.

sink.emitValue(1);           // Handler-based emission API.
sink.emitEmpty();
sink.emitError(error);

const mono = sink.asMono();
const count = sink.currentSubscriberCount();
```

`Sinks.empty` has the same terminal methods except value emission:

```ts
const sink = Sinks.empty<void>();

sink.emitEmpty();
await sink.asMono().block();
```

### Many Sink Variations

| Sink factory | What it does | Example |
| --- | --- | --- |
| `Sinks.many().unicast().onBackpressureBuffer<T>()` | One subscriber. Values emitted before the subscriber are buffered. | `const sink = Sinks.many().unicast().onBackpressureBuffer<number>();` |
| `Sinks.many().unicast().onBackpressureError<T>()` | One subscriber. Emission fails when no subscriber can receive it. | `sink.tryEmitNext(1) === "FAIL_ZERO_SUBSCRIBER"` |
| `Sinks.many().multicast().onBackpressureBuffer<T>()` | Many subscribers. Values can be buffered before subscribers appear. | `const sink = Sinks.many().multicast().onBackpressureBuffer<number>();` |
| `Sinks.many().multicast().directAllOrNothing<T>()` | Many subscribers. Fails when no subscriber can receive the value. | `Sinks.many().multicast().directAllOrNothing<number>()` |
| `Sinks.many().multicast().directBestEffort<T>()` | Many subscribers. Emits to currently available subscribers. | `Sinks.many().multicast().directBestEffort<number>()` |
| `Sinks.many().replay().all<T>()` | Remember every emitted value for late subscribers. | `Sinks.many().replay().all<number>()` |
| `Sinks.many().replay().limit<T>(historySize)` | Remember only the last `historySize` values. | `Sinks.many().replay().limit<number>(10)` |
| `Sinks.many().replay().latest<T>()` | Remember only the latest emitted value. | `Sinks.many().replay().latest<number>()` |
| `Sinks.many().replay().latestOrDefault<T>(value)` | Start with a default latest value. | `Sinks.many().replay().latestOrDefault<number>(0)` |
| `Sinks.unsafe().manyWithUpstream().multicastOnBackpressureBuffer<T>()` | Unsafe variant that can subscribe to one upstream publisher. | `Sinks.unsafe().manyWithUpstream().multicastOnBackpressureBuffer<number>()` |

### Many Sink Common Methods

```ts
const sink = Sinks.many().multicast().onBackpressureBuffer<number>();

sink.tryEmitNext(1);
sink.tryEmitComplete();
sink.tryEmitError(new Error("boom"));

sink.emitNext(2);
sink.emitComplete();
sink.emitError(new Error("boom"));

const flux = sink.asFlux();
const count = sink.currentSubscriberCount();

sink.subscribeTo(Flux.range(1, 3));
```

### Sinks.one Example

Use `Sinks.one` when a callback completes once.

```ts
function geolocation(): Mono<GeolocationPosition> {
  const sink = Sinks.one<GeolocationPosition>();

  navigator.geolocation.getCurrentPosition(
    position => sink.emitValue(position),
    error => sink.emitError(error)
  );

  return sink.asMono();
}
```

### Unicast Example

Use unicast when exactly one consumer should read the stream.

```ts
const sink = Sinks.many().unicast().onBackpressureBuffer<string>();

sink.emitNext("queued-before-subscribe");

const valuesPromise = sink.asFlux().take(2).toArray();

sink.emitNext("live");
sink.emitComplete();

console.log(await valuesPromise); // ["queued-before-subscribe", "live"]
```

### Multicast Example

Use multicast when multiple current subscribers should receive live values.

```ts
const sink = Sinks.many().multicast().onBackpressureBuffer<number>();

const first = sink.asFlux().take(2).toArray();
const second = sink.asFlux().take(2).toArray();

sink.emitNext(1);
sink.emitNext(2);
sink.emitComplete();

console.log(await first);  // [1, 2]
console.log(await second); // [1, 2]
```

### Replay Examples

Use replay when late subscribers must see previous values.

```ts
const all = Sinks.many().replay().all<number>();
all.emitNext(1);
all.emitNext(2);
all.emitComplete();

console.log(await all.asFlux().toArray()); // [1, 2]
```

```ts
const latest = Sinks.many().replay().latestOrDefault<number>(0);

console.log(await latest.asFlux().take(1).toArray()); // [0]

latest.emitNext(10);
latest.emitComplete();

console.log(await latest.asFlux().toArray()); // [10]
```

### Choosing A Sink

| Situation | Recommended sink |
| --- | --- |
| One callback result | `Sinks.one()` |
| Only completion or error matters | `Sinks.empty()` |
| One consumer and values may arrive early | `unicast().onBackpressureBuffer()` |
| One consumer and values must not arrive early | `unicast().onBackpressureError()` |
| Several live subscribers | `multicast().onBackpressureBuffer()` |
| Late subscribers need history | `replay().all()` or `replay().limit(n)` |
| Late subscribers need only latest state | `replay().latest()` or `latestOrDefault(value)` |

Performance warning: replay sinks store history. Do not use `replay().all()` for
unbounded streams unless you really want to keep every value in memory.

## Schedulers

A scheduler controls when a task runs. In the browser there are no JVM-style
thread pools, so Reactor scheduler names map to browser queues.

Durations can be a number of milliseconds or an object:

```ts
Mono.delay(250);
Mono.delay({ seconds: 1 });
Mono.delay({ minutes: 1, seconds: 30 });
```

### Scheduler Table

| Scheduler | Browser implementation | Use it when | Example |
| --- | --- | --- | --- |
| `Schedulers.immediate()` | Runs synchronously. | You want no async boundary. | `Schedulers.immediate().schedule(task)` |
| `Schedulers.microtask()` | Uses `queueMicrotask`. | You want to yield to the microtask queue. | `Flux.just(1).publishOn(Schedulers.microtask())` |
| `Schedulers.timeout()` | Uses `setTimeout`. | You need delays, timers or macrotask scheduling. | `Mono.delay(100, Schedulers.timeout())` |
| `Schedulers.animationFrame()` | Uses `requestAnimationFrame` when available. | UI work should align with painting. | `Schedulers.animationFrame().schedule(render)` |
| `Schedulers.single()` | Shared microtask scheduler. | Reactor-style single scheduler in browser code. | `Flux.just(1).subscribeOn(Schedulers.single())` |
| `Schedulers.parallel()` | Shared microtask scheduler. | Reactor-style parallel API shape without browser threads. | `Flux.just(1).publishOn(Schedulers.parallel())` |
| `Schedulers.boundedElastic()` | Shared timeout scheduler. | Timer-backed async boundaries. | `Mono.fromCallable(load).subscribeOn(Schedulers.boundedElastic())` |
| `Schedulers.newSingle(name?)` | New microtask scheduler instance. | You want a named isolated scheduler object. | `Schedulers.newSingle("ui")` |
| `Schedulers.newParallel(name?)` | New microtask scheduler instance. | You want Reactor-compatible naming. | `Schedulers.newParallel("workers")` |
| `Schedulers.newBoundedElastic(name?)` | New timeout scheduler instance. | You want a named timer-backed scheduler. | `Schedulers.newBoundedElastic("io")` |
| `Schedulers.fromExecutor(executor, name?)` | Wraps your executor function. | You need custom scheduling. | `Schedulers.fromExecutor(task => postTask(task), "postTask")` |

### Standalone Scheduler Usage

```ts
const scheduler = Schedulers.timeout();

const delayed: Disposable = scheduler.schedule(() => {
  console.log("runs later");
}, 250);

// Cancel before it runs.
delayed.dispose();
```

```ts
const worker = Schedulers.timeout().createWorker();

worker.schedule(() => console.log("first"), 100);
worker.schedulePeriodically(() => console.log("tick"), 0, 1_000);

// Disposes every task scheduled by this worker.
worker.dispose();
```

### Scheduler Usage In Publishers

Use scheduler-aware factories and operators when timing matters.

```ts
const ticks = await Flux.interval(100, Schedulers.timeout())
  .take(3)
  .toArray();

console.log(ticks); // [0, 1, 2]
```

```ts
const value = await Mono.fromCallable(() => JSON.parse(raw))
  .subscribeOn(Schedulers.timeout())
  .publishOn(Schedulers.microtask())
  .timeout({ seconds: 1 }, Mono.just({ fallback: true }))
  .block();
```

`subscribeOn` affects where subscription work starts. `publishOn` affects where
downstream delivery continues.

```ts
await Flux.just(1, 2)
  .doOnNext(value => console.log("before", value))
  .publishOn(Schedulers.microtask())
  .doOnNext(value => console.log("after", value))
  .then()
  .block();
```

For UI work, `animationFrame` is often a better boundary than `timeout`:

```ts
Flux.interval(16, Schedulers.animationFrame())
  .take(10)
  .subscribe(frame => draw(frame));
```

## Performance Notes For Browser Apps

- Prefer `Mono` when the source can only produce one value. It communicates
  intent and avoids accidental many-value chains.
- Prefer `Flux.fromArray(array)` or `Flux.fromIterable(iterable)` for existing
  in-memory data.
- Keep chains lazy. Build pipelines once, subscribe when you need results.
- Put `take`, `timeout` or cancellation around long-lived streams.
- Avoid `replay().all()` for unbounded data.
- Avoid heavy synchronous work in `map` on the UI path. Use a scheduler boundary
  or move the work outside the hot interaction path.
- Use `Context` for small metadata only.
- Use `Sinks` at the boundary of imperative systems. Inside your domain code,
  prefer pure `Mono` and `Flux` transformations.
- Remember that browser JavaScript has one main thread unless you explicitly use
  Workers. `Schedulers.parallel()` provides API compatibility, not CPU
  parallelism by itself.

## Project Scripts

| Script | Purpose |
| --- | --- |
| `npm run build` | Clean `dist`, compile TypeScript and rewrite path aliases. |
| `npm run typecheck` | Type-check without emitting files. |
| `npm run test:unit` | Run public API, entrypoint, scheduler and TSDoc tests. |
| `npm run test:matrix` | Run method matrix coverage. |
| `npm run test:operators` | Run operator behavior tests. |
| `npm run test:sinks` | Run sink parity tests. |
| `npm run test:java-oracle` | Compare TypeScript behavior with the committed Reactor oracle fixture. |
| `npm run api:parity:strict` | Compare the public API with the committed Reactor API fixture. |
| `npm run fixtures:reactor` | Manually download Maven Reactor artifacts and regenerate the committed fixture. |
| `npm run ci` | Run the full validation pipeline. |

Tests read only the committed fixture in `test/fixtures`. They never expect a
local Project Reactor checkout next to this repository. The `fixtures:reactor`
script is manual and refreshes that fixture from Maven artifacts.

## Practical Rules

- Start with `Mono` for one result and `Flux` for many results.
- Use `map` for synchronous value changes.
- Use `flatMap` when the next step returns a `Mono`, `Flux`, promise, iterable
  or async iterable.
- Use `concatMap` when order matters more than concurrency.
- Use `switchMap` when only the newest request matters, such as typeahead.
- Use `timeout` around remote or long-lived work.
- Use `onErrorResume` for real fallback logic.
- Use `doOn...` methods for observation, not business logic.
- Use `Context` for metadata, not data flow.
- Use `Sinks` sparingly, mostly at callback/event boundaries.
