import { describe, expect, it, vi } from "vitest";
import { CompositeDisposable } from "@/core/composite-disposable.js";
import { AsyncQueue } from "@/internal/async-queue.js";
import { Context, Flux, Mono, Sinks, type Subscriber, type Subscription } from "@/index.js";

describe("Disposables", () => {
  it("keeps composite add, remove and dispose semantics", () => {
    const composite = new CompositeDisposable();
    let firstDisposed = 0;
    let secondDisposed = 0;
    const first = {
      dispose() {
        firstDisposed += 1;
      },
      isDisposed() {
        return firstDisposed > 0;
      }
    };
    const second = {
      dispose() {
        secondDisposed += 1;
      },
      isDisposed() {
        return secondDisposed > 0;
      }
    };

    expect(composite.remove(first)).toBe(false);
    expect(composite.add(first)).toBe(true);
    expect(composite.add(first)).toBe(true);
    expect(composite.add(second)).toBe(true);
    expect(composite.remove(first)).toBe(true);

    composite.dispose();

    expect(firstDisposed).toBe(0);
    expect(secondDisposed).toBe(1);
    expect(composite.add(first)).toBe(false);
    expect(firstDisposed).toBe(1);
  });
});

describe("Flux", () => {
  it("composes common synchronous operators", async () => {
    await expect(
      Flux.range(1, 6)
        .filter(value => value % 2 === 0)
        .map(value => value * 10)
        .collectList()
        .block()
    ).resolves.toEqual([20, 40, 60]);
  });

  it("honors downstream request amounts", async () => {
    const seen: number[] = [];
    let subscription: Subscription | undefined;
    let completed = false;

    const subscriber: Subscriber<number> = {
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
        nextSubscription.request(2);
      },
      onNext(value) {
        seen.push(value);
        if (value === 2) {
          subscription?.request(2);
        }
      },
      onError(error) {
        throw error;
      },
      onComplete() {
        completed = true;
      }
    };

    Flux.range(1, 5).subscribe(subscriber);
    await vi.waitFor(() => expect(seen).toEqual([1, 2, 3, 4]));
    expect(completed).toBe(false);

    subscription?.request(1);
    await vi.waitFor(() => expect(completed).toBe(true));
    expect(seen).toEqual([1, 2, 3, 4, 5]);
  });

  it("supports asynchronous interval sources", async () => {
    await expect(Flux.interval(1).take(3).toArray()).resolves.toEqual([0, 1, 2]);
  });

  it("runs create cancellation callbacks", async () => {
    let cancelled = 0;
    let subscription: Subscription | undefined;

    Flux.create<number>(sink => {
      sink.onCancel(() => {
        cancelled += 1;
      });
      sink.onCancel(() => {
        cancelled += 1;
      });
      sink.next(1);
    }).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
        nextSubscription.request(1);
      },
      onNext() {
        subscription?.cancel();
      },
      onError(error) {
        throw error;
      },
      onComplete() {
        // no-op
      }
    });

    await vi.waitFor(() => expect(cancelled).toBe(2));
  });

  it("generates values across repeated turns", async () => {
    let next = 0;

    await expect(
      Flux.generate<number>(sink => {
        if (next === 3) {
          sink.complete();
          return;
        }
        sink.next(next);
        next += 1;
      }).toArray()
    ).resolves.toEqual([0, 1, 2]);
  });

  it("emits thenReturn value after draining the source", async () => {
    const seen: number[] = [];

    await expect(Flux.just(1, 2, 3).doOnNext(value => seen.push(value)).thenReturn("done").block()).resolves.toBe("done");
    expect(seen).toEqual([1, 2, 3]);
  });

  it("recovers from errors", async () => {
    await expect(
      Flux.concat(Flux.just(1), Flux.error<number>(new Error("boom")))
        .onErrorResume(() => Flux.just(2, 3))
        .toArray()
    ).resolves.toEqual([1, 2, 3]);
  });

  it("zips publishers", async () => {
    await expect(
      Flux.zip<[number, string]>([Flux.range(1, 2), Flux.just("a", "b")])
        .map(values => `${values[0]}${values[1]}`)
        .toArray()
    ).resolves.toEqual(["1a", "2b"]);

    await expect(Flux.zip<[number]>([Flux.just(1, 2)]).toArray()).resolves.toEqual([[1], [2]]);
    await expect(Flux.zip<[number], number>([Flux.just(1, 2)], values => values[0] * 10).toArray()).resolves.toEqual([10, 20]);
    await expect(Flux.combineLatest<[number], number>([Flux.just(1, 2)], values => values[0] * 10).toArray()).resolves.toEqual([10, 20]);
    await expect(Flux.combineLatest<[number, string], string>([Flux.just(1), Flux.just("a")], values => `${values[0]}${values[1]}`).toArray()).resolves.toEqual(["1a"]);
  });

  it("completes an empty zip without values", async () => {
    await expect(Flux.zip<[]>([]).toArray()).resolves.toEqual([]);
  });

  it("uses plain arrays for tuple-like operator results", async () => {
    await expect(Flux.just(1).zipWith(Flux.just("a")).toArray()).resolves.toEqual([[1, "a"]]);

    const timestamped = await Flux.just("value").timestamp().next().block();
    expect(Array.isArray(timestamped)).toBe(true);
    expect(timestamped?.[1]).toBe("value");

    const elapsed = await Flux.just("value").elapsed().next().block();
    expect(Array.isArray(elapsed)).toBe(true);
    expect(elapsed?.[1]).toBe("value");
  });
});

describe("Mono", () => {
  it("maps and flatMaps one value", async () => {
    await expect(
      Mono.just(2)
        .map(value => value + 1)
        .flatMap(value => Mono.just(value * 3))
        .block()
    ).resolves.toBe(9);
  });

  it("supports promise and empty factories", async () => {
    await expect(Mono.fromPromise(Promise.resolve("ok")).block()).resolves.toBe("ok");
    await expect(Mono.justOrEmpty<string>(undefined).defaultIfEmpty("fallback").block()).resolves.toBe("fallback");
  });

  it("runs create cancellation callbacks", async () => {
    let cancelled = 0;
    let subscription: Subscription | undefined;

    Mono.create<number>(sink => {
      sink.onCancel(() => {
        cancelled += 1;
      });
      sink.onCancel(() => {
        cancelled += 1;
      });
    }).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
        nextSubscription.request(1);
      },
      onNext() {
        // no-op
      },
      onError(error) {
        throw error;
      },
      onComplete() {
        // no-op
      }
    });

    subscription?.cancel();

    await vi.waitFor(() => expect(cancelled).toBe(2));
  });

  it("waits for several publishers with when", async () => {
    const error = new Error("boom");

    await expect(Mono.when(Flux.just(1, 2), Mono.delay(1)).block()).resolves.toBeUndefined();
    await expect(Mono.when(Flux.just(1)).flux().toArray()).resolves.toEqual([]);
    await expect(Mono.whenDelayError(Flux.just(1)).flux().toArray()).resolves.toEqual([]);
    await expect(Mono.whenDelayError(Mono.error(error)).block()).rejects.toMatchObject({errors: [error]});
  });

  it("completes an empty delayed-error zip without values", async () => {
    const error = new Error("zip");

    await expect(Mono.zipDelayError<[]>([]).flux().toArray()).resolves.toEqual([]);
    await expect(Mono.zipDelayError<[number], number>([Mono.just(2)], values => values[0] * 5).block()).resolves.toBe(10);
    await expect(Mono.zipDelayError<[number]>([Mono.error(error)]).block()).rejects.toMatchObject({errors: [error]});
  });

  it("reuses stateless empty publishers for empty factories", () => {
    expect(Flux.just()).toBe(Flux.empty());
    expect(Flux.fromArray([])).toBe(Flux.empty());
    expect(Flux.range(1, 0)).toBe(Flux.empty());
    expect(Flux.firstWithValue()).toBe(Flux.empty());
    expect(Mono.when()).toBe(Mono.empty());
    expect(Mono.whenDelayError()).toBe(Mono.empty());
    expect(Mono.firstWithSignal()).toBe(Mono.empty());
    expect(Mono.firstWithValue()).toBe(Mono.empty());
  });

  it("compares publisher sequences element by element", async () => {
    await expect(Mono.sequenceEqual(Flux.just(1, 2), Flux.just(1, 2)).block()).resolves.toBe(true);
    await expect(Mono.sequenceEqual(Flux.just(1, 2), Flux.just(1, 3)).block()).resolves.toBe(false);
  });
});

describe("Context", () => {
  it("reuses empty and unchanged immutable contexts", () => {
    const context = Context.empty().put("requestId", "abc");
    const readOnly = context.readOnly();
    const token = Symbol("token");

    expect(Context.of()).toBe(Context.empty());
    expect(Context.from({ [token]: "secret" }).get(token)).toBe("secret");
    expect(context.put("requestId", "abc")).toBe(context);
    expect(context.putAll([])).toBe(context);
    expect(context.putAll(Context.of("requestId", "abc"))).toBe(context);
    expect(context.readOnly()).toBe(readOnly);

    const next = context.put("traceId", "xyz");
    expect(readOnly.hasKey("traceId")).toBe(false);
    expect(next.get("traceId")).toBe("xyz");
  });

  it("rejects nullish Context.of entries while building pairs", () => {
    expect(() => Context.of("requestId", undefined)).toThrow(TypeError);
    expect(() => Context.of(null, "abc")).toThrow(TypeError);
  });

  it("rejects nullish Context.from iterable entries while building the map", () => {
    expect(Context.from(new Map())).toBe(Context.empty());
    expect(() => Context.from([["requestId", undefined]])).toThrow(TypeError);
    expect(() => Context.from([[undefined, "abc"]])).toThrow(TypeError);
  });
});

describe("Sinks", () => {
  it("reuses immutable builder facades", () => {
    const many = Sinks.many();
    const unicast = many.unicast();

    expect(Object.isFrozen(Sinks)).toBe(true);
    expect(Object.isFrozen(many)).toBe(true);
    expect(Object.isFrozen(unicast)).toBe(true);
    expect(Sinks.many()).toBe(many);
    expect(Sinks.many().unicast()).toBe(unicast);
  });

  it("delivers one sink values to several active subscribers", async () => {
    const sink = Sinks.one<number>();
    const first = sink.asMono().block();
    const second = sink.asMono().block();

    await vi.waitFor(() => expect(sink.currentSubscriberCount()).toBe(2));
    expect(sink.tryEmitValue(7)).toBe("OK");

    await expect(Promise.all([first, second])).resolves.toEqual([7, 7]);
    expect(sink.currentSubscriberCount()).toBe(0);
  });

  it("delivers many sink values to several active subscribers", async () => {
    const sink = Sinks.many().multicast().directAllOrNothing<number>();
    const first = sink.asFlux().take(2).toArray();
    const second = sink.asFlux().take(2).toArray();

    await vi.waitFor(() => expect(sink.currentSubscriberCount()).toBe(2));
    expect(sink.tryEmitNext(1)).toBe("OK");
    expect(sink.tryEmitNext(2)).toBe("OK");

    await expect(Promise.all([first, second])).resolves.toEqual([[1, 2], [1, 2]]);
    await vi.waitFor(() => expect(sink.currentSubscriberCount()).toBe(0));
  });

  it("replays one sink completion to late subscribers", async () => {
    const sink = Sinks.one<number>();
    expect(sink.tryEmitValue(42)).toBe("OK");
    expect(await sink.asMono().block()).toBe(42);
    expect(sink.tryEmitValue(43)).toBe("FAIL_TERMINATED");
  });

  it("buffers unicast values before the first subscriber", async () => {
    const sink = Sinks.many().unicast().onBackpressureBuffer<number>();
    expect(sink.tryEmitNext(1)).toBe("OK");
    expect(sink.tryEmitNext(2)).toBe("OK");
    expect(sink.tryEmitComplete()).toBe("OK");
    await expect(sink.asFlux().toArray()).resolves.toEqual([1, 2]);
  });

  it("replays latest value to late subscribers", async () => {
    const sink = Sinks.many().replay().latest<number>();
    sink.emitNext(1);
    sink.emitNext(2);
    sink.emitComplete();
    await expect(sink.asFlux().toArray()).resolves.toEqual([2]);
  });

  it("reports zero subscribers for direct multicast", () => {
    const sink = Sinks.many().multicast().directAllOrNothing<number>();
    expect(sink.tryEmitNext(1)).toBe("FAIL_ZERO_SUBSCRIBER");
  });
});

describe("internal queues", () => {
  it("rejects immediately when created with an already aborted signal", async () => {
    const controller = new AbortController();
    controller.abort();
    const queue = new AsyncQueue<number>(controller.signal);

    expect(queue.push(1)).toBe(false);
    await expect(queue.next()).rejects.toThrow("Subscription has been cancelled");
  });

  it("rejects a pending read when the abort signal fires", async () => {
    const controller = new AbortController();
    const queue = new AsyncQueue<number>(controller.signal);
    const next = queue.next();

    controller.abort();

    await expect(next).rejects.toThrow("Subscription has been cancelled");
    expect(queue.push(1)).toBe(false);
  });
});
