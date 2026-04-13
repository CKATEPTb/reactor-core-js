# reactor-core-ts

A TypeScript implementation of [Reactive Streams](https://www.reactive-streams.org/), inspired by [Project Reactor](https://projectreactor.io/). Provides `Flux` and `Mono` publishers with full backpressure support, a suite of composable operators, programmable Sinks, and pluggable Schedulers.

## Table of Contents

- [Installation](#installation)
- [Core Concepts](#core-concepts)
- [Flux](#flux)
  - [Factories](#flux-factories)
  - [Transformation](#flux-transformation)
  - [Filtering](#flux-filtering)
  - [Aggregation](#flux-aggregation)
  - [Combining](#flux-combining)
  - [Side effects](#flux-side-effects)
  - [Scheduling](#flux-scheduling)
  - [Utilities](#flux-utilities)
  - [Subscribe](#flux-subscribe)
- [Mono](#mono)
  - [Factories](#mono-factories)
  - [Transformation](#mono-transformation)
  - [Filtering](#mono-filtering)
  - [Combining](#mono-combining)
  - [Side effects](#mono-side-effects)
  - [Scheduling](#mono-scheduling)
  - [Utilities](#mono-utilities)
  - [Subscribe](#mono-subscribe)
- [Sinks](#sinks)
- [Schedulers](#schedulers)
- [TypeScript generics](#typescript-generics)
- [Backpressure](#backpressure)
- [Contributing](#contributing)
- [License](#license)

---

## Installation

```bash
npm install reactor-core-ts
# or
pnpm add reactor-core-ts
# or
yarn add reactor-core-ts
```

---

## Core Concepts

| Concept | Description |
|---|---|
| **Publisher\<T\>** | Source of a data stream. Emits items on subscriber demand. |
| **Subscriber\<T\>** | Consumer of a stream. Receives `onSubscribe`, `onNext`, `onError`, `onComplete`. |
| **Subscription** | Handle returned by `subscribe()`. Call `request(n)` to pull items, `unsubscribe()` to cancel. |
| **Flux\<T\>** | 0…N items publisher. The multi-value building block. |
| **Mono\<T\>** | 0…1 item publisher. Ideal for single async results. |
| **Sink\<T\>** | Imperative push interface: `next(v)`, `error(e)`, `complete()`. |
| **SinkPublisher\<T\>** | A `Sink<T>` that is also a `Publisher<T>` — both push and subscribe sides in one object. |
| **Scheduler** | Abstraction over task execution (sync, micro-task, macro-task, delayed). |

### Backpressure

This library implements [Reactive Streams specification](https://www.reactive-streams.org/). Items are **never** pushed to a subscriber unless it has issued a `request(n)`. The convenience `subscribe()` overloads (`onNext`, `onError`, `onComplete` callbacks) issue `request(Number.MAX_SAFE_INTEGER)` for Flux and `request(1)` for Mono automatically.

---

## Flux

`Flux<T>` is a cold publisher of 0 to N items. Each subscriber gets an independent run of the stream.

### Flux Factories

| Method | Description |
|---|---|
| `Flux.just(...items)` | Emit each provided value in order, then complete. |
| `Flux.range(start, count)` | Emit integers `[start, start+count)`. |
| `Flux.fromIterable(iterable)` | Emit all items from any `Iterable<T>`. |
| `Flux.generate(sink => …)` | Imperative generator. Call `sink.next()` / `sink.complete()` / `sink.error()`. Delivery is deferred until downstream requests. |
| `Flux.from(publisher)` | Wrap any `Publisher<T>` as a `Flux<T>`. |
| `Flux.defer(factory)` | Lazily create a new Flux per subscription via the factory function. |
| `Flux.empty()` | Complete immediately without emitting. |
| `Flux.never()` | Never emit any signal. |
| `Flux.error(err)` | Signal `onError` immediately. |

```typescript
import { Flux } from 'reactor-core-ts';

Flux.just(1, 2, 3).subscribe(v => console.log(v));
// 1  2  3

Flux.range(0, 5).subscribe(v => console.log(v));
// 0  1  2  3  4

Flux.fromIterable(['a', 'b', 'c']).subscribe(v => console.log(v));
// a  b  c

Flux.generate<number>(sink => {
    sink.next(10);
    sink.next(20);
    sink.complete();
}).subscribe(v => console.log(v));
// 10  20
```

### Flux Transformation

| Method | Description |
|---|---|
| `.map(fn)` | Transform each item `T → R`. |
| `.mapNotNull(fn)` | Like `map`, but `null`/`undefined` results are filtered; upstream demand is replenished for each skipped item. |
| `.flatMap(fn)` | Map each item to a `Flux<R>` or `Mono<R>`, subscribing to all concurrently (merge semantics). |
| `.concatMap(fn)` | Like `flatMap` but subscribes to each inner publisher sequentially, preserving order. |
| `.switchMap(fn)` | Like `flatMap` but cancels the previous inner subscription when a new outer item arrives. |
| `.handle(handler)` | Fine-grained per-item transform via a `Sink<R>`. The handler may emit 0 or 1 item. |
| `.scan(reducer)` | Emit running accumulation; first item is used as initial accumulator. |
| `.scanWith(seedFactory, reducer)` | Emit running accumulation starting from an explicit seed value. |
| `.cast<R>()` | Unsafe type cast — changes the declared element type without any runtime conversion. |
| `.indexed()` | Pair each item with its zero-based index: `Flux<[number, T]>`. |

```typescript
Flux.just(1, 2, 3)
    .map(n => n * 2)
    .subscribe(v => console.log(v));
// 2  4  6

Flux.just('hello', '', 'world')
    .mapNotNull(s => s.length > 0 ? s.toUpperCase() : null)
    .subscribe(v => console.log(v));
// HELLO  WORLD

Flux.just(1, 2, 3)
    .flatMap(n => Flux.just(n, n * 10))
    .subscribe(v => console.log(v));
// 1  10  2  20  3  30  (order may vary with async inner publishers)

Flux.just(1, 2, 3, 4)
    .scan((acc, n) => acc + n)
    .subscribe(v => console.log(v));
// 1  3  6  10
```

### Flux Filtering

| Method | Description |
|---|---|
| `.filter(predicate)` | Pass only items matching the predicate; replenishes demand for dropped items. |
| `.filterWhen(predicate)` | Async filter — for each item, subscribe to the `Publisher<boolean>` returned by `predicate`; forward item only if it emits `true`. |
| `.take(n)` | Emit at most `n` items, then cancel upstream and complete. |
| `.takeWhile(predicate)` | Emit items while `predicate` returns `true`; complete on the first `false`. |
| `.takeUntilOther(trigger)` | Emit items until `trigger` emits any signal, then cancel and complete. |
| `.skip(n)` | Skip the first `n` items. |
| `.skipWhile(predicate)` | Skip items while `predicate` returns `true`; then pass through. |
| `.skipUntil(other)` | Skip items until `other` emits; then pass remaining items through. |
| `.distinct()` | Suppress duplicate items (equality checked by `===`). |
| `.distinctUntilChanged(comparator?)` | Suppress consecutive duplicate items. Custom `comparator(a, b)` returns `true` when items are considered equal. |
| `.defaultIfEmpty(value)` | Emit `value` if the source completes without emitting anything. |

```typescript
Flux.just(1, 2, 3, 4, 5, 6)
    .filter(n => n % 2 === 0)
    .subscribe(v => console.log(v));
// 2  4  6

Flux.just(1, 2, 3, 4, 5)
    .take(3)
    .subscribe(v => console.log(v));
// 1  2  3

Flux.just(1, 1, 2, 2, 3, 1)
    .distinctUntilChanged()
    .subscribe(v => console.log(v));
// 1  2  3  1
```

### Flux Aggregation

These operators reduce a Flux to a Mono.

| Method | Description |
|---|---|
| `.count()` | `Mono<number>` — total number of items emitted. |
| `.first()` | `Mono<T>` — the first item, or empty if source is empty. |
| `.last()` | `Mono<T>` — the last item, or empty if source is empty. |
| `.elementAt(index, default?)` | `Mono<T>` — item at zero-based `index`. Emits `default` if out of bounds. |
| `.hasElements()` | `Mono<boolean>` — `true` if source emits at least one item. |
| `.any(predicate)` | `Mono<boolean>` — `true` if any item satisfies the predicate. |
| `.all(predicate)` | `Mono<boolean>` — `true` if all items satisfy the predicate. |
| `.none(predicate)` | `Mono<boolean>` — `true` if no item satisfies the predicate. |
| `.reduce(reducer)` | `Mono<T>` — reduce all items to a single value (first item is the initial accumulator). |
| `.reduceWith(seedFactory, reducer)` | `Mono<A>` — reduce with an explicit seed of type `A`. |
| `.collect()` | `Mono<T[]>` — collect all items into an array. |
| `.collectList()` | Alias for `.collect()`. |
| `.sort(comparator?)` | `Flux<T>` — collect then re-emit items in sorted order. |
| `.buffer(maxSize)` | `Flux<T[]>` — group items into fixed-size arrays. |
| `.then()` | `Mono<void>` — wait for completion; ignore all items. |
| `.thenEmpty(other)` | `Mono<void>` — wait for completion then subscribe to `other`. |

```typescript
Flux.just(1, 2, 3, 4)
    .reduce((acc, n) => acc + n)
    .subscribe(sum => console.log(sum));
// 10

Flux.just('a', 'b', 'c')
    .collect()
    .subscribe(arr => console.log(arr));
// ['a', 'b', 'c']

Flux.just(3, 1, 4, 1, 5)
    .sort()
    .subscribe(v => console.log(v));
// 1  1  3  4  5
```

### Flux Combining

| Method | Description |
|---|---|
| `.concatWith(other)` | Append `other` after `this` completes (sequential). |
| `.mergeWith(other)` | Merge `other` with `this` concurrently; items interleave as they arrive. |
| `.zipWith(other, combiner)` | Pair items from `this` and `other` one-by-one using `combiner(a, b) → V`. |

```typescript
Flux.just(1, 2).concatWith(Flux.just(3, 4)).subscribe(v => console.log(v));
// 1  2  3  4

Flux.just(1, 2, 3).zipWith(Flux.just('a', 'b', 'c'), (n, s) => `${n}${s}`)
    .subscribe(v => console.log(v));
// '1a'  '2b'  '3c'
```

### Flux Side Effects

These operators observe signals without modifying the stream.

| Method | Description |
|---|---|
| `.doFirst(fn)` | Run `fn` before the first item is emitted. |
| `.doOnNext(fn)` | Run `fn` for each item. |
| `.doOnError(fn)` | Run `fn` when `onError` is received. |
| `.doOnComplete(fn)` | Run `fn` when `onComplete` is received. |
| `.doOnTerminate(fn)` | Run `fn` on both `onComplete` and `onError`. |
| `.doOnCancel(fn)` | Run `fn` when the subscription is cancelled. |
| `.doFinally(fn)` | Run `fn` after the stream terminates for any reason (complete, error, or cancel). |
| `.doOnSubscribe(fn)` | Run `fn` when `onSubscribe` is received, receiving the `Subscription` object. |

```typescript
Flux.just(1, 2, 3)
    .doOnNext(v => console.log('next:', v))
    .doOnComplete(() => console.log('done'))
    .subscribe();
// next: 1  next: 2  next: 3  done
```

### Flux Scheduling

| Method | Description |
|---|---|
| `.publishOn(scheduler)` | Deliver `onNext`, `onError`, `onComplete` signals through the given `Scheduler`. |
| `.subscribeOn(scheduler)` | Perform the upstream subscription (and therefore the source work) on the given `Scheduler`. |
| `.delayElements(ms)` | Delay each item by `ms` milliseconds using `Schedulers.delay(ms)`. |

```typescript
const asyncScheduler = Schedulers.macro(); // setTimeout-based

Flux.just(1, 2, 3)
    .publishOn(asyncScheduler)
    .subscribe(v => console.log('received asynchronously:', v));

Flux.just('a', 'b', 'c')
    .delayElements(500)
    .subscribe(v => console.log(v)); // each item arrives 500 ms after the previous
```

### Flux Utilities

| Method | Description |
|---|---|
| `.retry(maxRetries?)` | Re-subscribe on error up to `maxRetries` times (default: unbounded). |
| `.cache()` | Subscribe to the source once; replay all items to subsequent subscribers. |
| `.switchIfEmpty(alternative)` | Switch to `alternative` if source completes without emitting. |
| `.onErrorReturn(replacement)` | On error, switch to `replacement` publisher. |
| `.onErrorContinue(predicate)` | If `predicate(err)` is `true`, complete; otherwise re-throw. |
| `.pipe(producer, onRequest, onUnsubscribe)` | Low-level escape hatch for building custom downstream operators. |

### Flux Subscribe

```typescript
// Full Subscriber — full backpressure control
const sub = flux.subscribe({
    onSubscribe(s) { s.request(10); },
    onNext(v)      { console.log(v); },
    onError(e)     { console.error(e); },
    onComplete()   { console.log('done'); },
});

// Callback convenience — auto-requests Number.MAX_SAFE_INTEGER
flux.subscribe(
    v  => console.log(v),      // onNext (optional)
    e  => console.error(e),    // onError (optional, defaults to re-throw)
    () => console.log('done'), // onComplete (optional)
);

// Minimal — just consume items
flux.subscribe(v => console.log(v));

// No callbacks — drain silently
flux.subscribe();

// All overloads return a Subscription
sub.unsubscribe(); // cancel at any time
```

---

## Mono

`Mono<T>` is a cold publisher of at most 1 item. Each subscriber gets an independent run.

### Mono Factories

| Method | Description |
|---|---|
| `Mono.just(value)` | Emit exactly one value, then complete. |
| `Mono.justOrEmpty(value)` | Emit `value` if not `null`/`undefined`, otherwise complete empty. |
| `Mono.fromPromise(promise)` | Wrap a `Promise<T>` — resolves to `onNext` + `onComplete`, rejects to `onError`. |
| `Mono.generate(sink => …)` | Imperative generator. Call `sink.next()` exactly once (or `sink.error()`/`sink.complete()`). |
| `Mono.from(publisher)` | Wrap any `Publisher<T>` as a `Mono<T>` (takes only the first item). |
| `Mono.defer(factory)` | Lazily create a new Mono per subscription via the factory function. |
| `Mono.empty()` | Complete immediately without emitting. |
| `Mono.error(err)` | Signal `onError` immediately. |

```typescript
import { Mono } from 'reactor-core-ts';

Mono.just(42).subscribe(v => console.log(v));
// 42

Mono.fromPromise(fetch('/api/user').then(r => r.json()))
    .subscribe(user => console.log(user));

Mono.justOrEmpty(null).subscribe(
    v  => console.log('got:', v),
    _e => {},
    () => console.log('empty'),
);
// empty
```

### Mono Transformation

| Method | Description |
|---|---|
| `.map(fn)` | Transform the value `T → R`. |
| `.mapNotNull(fn)` | Transform `T → R | null | undefined`; if result is null/undefined, complete empty. |
| `.flatMap(fn)` | Map the value to a `Mono<R>`, then subscribe to that inner Mono. |
| `.flatMapMany(fn)` | Map the value to a `Flux<R>` or `Mono<R>`, returning a `Flux<R>`. |
| `.cast<R>()` | Unsafe type cast. |

```typescript
Mono.just(42)
    .map(n => n.toString())
    .subscribe(s => console.log(s)); // '42'

Mono.just(5)
    .flatMapMany(n => Flux.range(0, n))
    .subscribe(v => console.log(v));
// 0  1  2  3  4
```

### Mono Filtering

| Method | Description |
|---|---|
| `.filter(predicate)` | Pass the value only if `predicate` returns `true`; otherwise complete empty. |
| `.filterWhen(predicate)` | Async filter — subscribe to `predicate(value)` and pass the value only if it emits `true`. |

### Mono Combining

| Method | Description |
|---|---|
| `.zipWith(other)` | Concurrently subscribe to this and `other`; combine their values into `[T, R]`. |
| `.zipWhen(fn)` | Emit value `v`, then derive `fn(v)` as a second Mono; combine into `[T, R]`. |

```typescript
Mono.just(1).zipWith(Mono.just('a'))
    .subscribe(([n, s]) => console.log(n, s));
// 1  'a'

Mono.just(42)
    .zipWhen(n => Mono.just(n.toString()))
    .subscribe(([n, s]) => console.log(n, s));
// 42  '42'
```

### Mono Side Effects

Same as Flux: `.doOnNext`, `.doOnError`, `.doOnSubscribe`, `.doFirst`, `.doFinally`.

### Mono Scheduling

Same as Flux: `.publishOn(scheduler)`, `.subscribeOn(scheduler)`.

### Mono Utilities

| Method | Description |
|---|---|
| `.hasElement()` | `Mono<boolean>` — `true` if a value is emitted, `false` if it completes empty. |
| `.toPromise()` | `Promise<T | null>` — resolves with the emitted value or `null` if empty. |
| `.switchIfEmpty(alternative)` | Switch to `alternative` Mono if this completes empty. |
| `.onErrorReturn(replacement)` | On error, switch to `replacement` publisher. |
| `.retry(maxRetries?)` | Re-subscribe on error. |
| `.pipe(producer, onRequest, onUnsubscribe)` | Low-level custom operator builder. |

### Mono Subscribe

```typescript
// Full Subscriber — full backpressure control
mono.subscribe({
    onSubscribe(s) { s.request(1); },
    onNext(v)      { console.log(v); },
    onError(e)     { console.error(e); },
    onComplete()   { console.log('done'); },
});

// Callback convenience — auto-requests 1
mono.subscribe(
    v  => console.log(v),   // onNext (optional)
    e  => console.error(e), // onError (optional)
    () => console.log('done'), // onComplete (optional)
);

// Promise bridge
const value: number | null = await Mono.just(42).toPromise();
```

---

## Sinks

Sinks are imperative bridges that let external code push values into a reactive stream. The `Sinks` factory returns a `SinkPublisher<T>` — an object that implements both `Sink<T>` (push API) and `Publisher<T>` (subscribe API).

```typescript
import { Sinks, Flux } from 'reactor-core-ts';
```

### `Sinks.empty<T>()`

Completes immediately on subscribe. Useful as a sentinel or no-op source.

### `Sinks.one<T>()`

Accepts a single `next(v)` call. Further calls are ignored. Suitable for request/response patterns.

```typescript
const sink = Sinks.one<number>();

Flux.from(sink).subscribe(v => console.log('got:', v));

sink.next(42); // got: 42
```

### `Sinks.many().unicast()`

Only one subscriber allowed. Queues items until the subscriber requests them.

| Variant | Description |
|---|---|
| `.onBackpressureBuffer<T>()` | Buffer all items until subscriber requests them. |
| `.onBackpressureError<T>()` | Drop items with an error when the subscriber has no pending demand. |

```typescript
const sink = Sinks.many().unicast().onBackpressureBuffer<number>();

Flux.from(sink).subscribe(v => console.log(v));

sink.next(1);
sink.next(2);
sink.next(3);
sink.complete();
// 1  2  3
```

### `Sinks.many().multicast()`

Multiple subscribers share the same stream. Items are dispatched to all current subscribers simultaneously.

| Variant | Description |
|---|---|
| `.directAllOrNothing<T>()` | Deliver to all subscribers if all have demand; otherwise signal an error. |
| `.directBestEffort<T>()` | Deliver to subscribers that have demand; silently skip those that don't. |
| `.onBackpressureBuffer<T>(bufferSize?, autoCancel?)` | Buffer items per-subscriber (up to `bufferSize`, default 256). When `autoCancel` is `true` (default), upstream is cancelled when all subscribers unsubscribe. |

```typescript
const sink = Sinks.many().multicast().onBackpressureBuffer<string>();

Flux.from(sink).subscribe(v => console.log('A:', v));
Flux.from(sink).subscribe(v => console.log('B:', v));

sink.next('hello');
// A: hello
// B: hello
```

### `Sinks.many().replay()`

Cache emitted items and replay them to new subscribers.

| Variant | Description |
|---|---|
| `.all<T>()` | Replay every item ever emitted. |
| `.latest<T>(limit)` | Replay the last `limit` items. |
| `.latestOrDefault<T>(value)` | Replay the latest item, or `value` if nothing has been emitted yet. |

```typescript
const sink = Sinks.many().replay().all<number>();

sink.next(1);
sink.next(2);
sink.next(3);

// New subscriber receives all previously emitted items
Flux.from(sink).subscribe(v => console.log(v));
// 1  2  3
```

---

## Schedulers

Schedulers control the thread/task context in which work runs.

```typescript
import { Schedulers } from 'reactor-core-ts';
```

| Factory | Backed by | Use case |
|---|---|---|
| `Schedulers.immediate()` | Synchronous call | Inline execution — no scheduling overhead. |
| `Schedulers.micro()` | `queueMicrotask` | After the current task but before any macro-tasks. |
| `Schedulers.macro()` | `setTimeout(fn, 0)` | Next event loop iteration. |
| `Schedulers.delay(ms)` | `setTimeout(fn, ms)` | One-shot delayed execution. Returns `{ cancel() }`. |
| `Schedulers.interval(ms)` | `setInterval(fn, ms)` | Repeating execution. Returns `{ cancel() }`. |

All schedulers implement `Scheduler` (`schedule(fn)`). The `delay` and `interval` schedulers additionally implement `CancellableScheduler` (return `{ cancel() }` from `schedule`).

```typescript
// Deliver items on the next event-loop tick
Flux.just(1, 2, 3)
    .publishOn(Schedulers.macro())
    .subscribe(v => console.log(v));

// Run the subscription (and source work) asynchronously
Flux.range(0, 1000)
    .subscribeOn(Schedulers.micro())
    .subscribe(v => process(v));

// One-shot delay
const task = Schedulers.delay(2000).schedule(() => console.log('2 s later'));
task.cancel(); // cancel before it fires

// Custom scheduler from any object that has a schedule(fn) method
const myScheduler = { schedule: (fn: () => void) => requestAnimationFrame(fn) };
Flux.just(1).publishOn(myScheduler).subscribe(v => console.log(v));
```

---

## Backpressure

Every subscription starts with zero demand. Items flow only after `request(n)` is called.

```typescript
const sub = Flux.range(0, 100).subscribe({
    onSubscribe(s) {
        // request 10 items to start
        s.request(10);
    },
    onNext(v) {
        console.log(v);
        // request the next batch when processing is done
    },
    onError(e) { console.error(e); },
    onComplete() { console.log('done'); },
});

// cancel any time
sub.unsubscribe();
```

The convenience callback overloads issue `request(Number.MAX_SAFE_INTEGER)` for `Flux` and `request(1)` for `Mono`, effectively making them "unbounded" without any ceremony.

---

## Contributing

```bash
git clone https://github.com/CKATEPTb/reactor-core-ts.git
cd reactor-core-ts
pnpm install

pnpm run build   # compile
pnpm test        # run all tests (Jest + TCK)
```

Feel free to open issues and submit pull requests.

---

## License

LGPL-3.0-only. See [LICENSE.md](LICENSE.md) for details.

**Author**: CKATEPTb
