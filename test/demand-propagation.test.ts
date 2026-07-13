import {describe, expect, it} from "vitest";
import {Context, Flux, type Publisher, type Subscriber, type Subscription} from "@/index.js";

describe("demand-preserving operators", () => {
    it("forwards one batched request through Flux.from, map and controller logging hooks", () => {
        const publisher = new TrackingPublisher<number>();
        const values: number[] = [];
        const lifecycle: string[] = [];
        let subscription: Subscription | undefined;

        Flux.from(publisher)
            .map(value => value * 2)
            .doOnSubscribe(() => lifecycle.push("subscribe"))
            .doOnNext(value => lifecycle.push(`next:${value}`))
            .doOnComplete(() => lifecycle.push("complete"))
            .doOnError(() => lifecycle.push("error"))
            .doFinally(signal => lifecycle.push(`finally:${signal}`))
            .subscribe({
                onSubscribe(nextSubscription) {
                    subscription = nextSubscription;
                },
                onNext(value) {
                    values.push(value);
                },
                onError(error) {
                    throw error;
                },
                onComplete() {
                    lifecycle.push("downstream-complete");
                }
            });

        expect(lifecycle).toEqual(["subscribe"]);
        expect(publisher.requests).toEqual([]);

        subscription?.request(30);
        expect(publisher.requests).toEqual([30]);

        publisher.next(4);
        expect(values).toEqual([8]);
        expect(publisher.requests).toEqual([30]);

        publisher.complete();
        expect(lifecycle).toEqual([
            "subscribe",
            "next:8",
            "complete",
            "downstream-complete",
            "finally:complete"
        ]);
    });

    it("reports actual request amounts to doOnRequest without changing them", () => {
        const publisher = new TrackingPublisher<number>();
        const observed: number[] = [];
        let subscription: Subscription | undefined;

        Flux.from(publisher).doOnRequest(request => observed.push(request)).subscribe(passiveSubscriber(next => {
            subscription = next;
        }));

        expect(observed).toEqual([]);
        subscription?.request(7);
        subscription?.request(11);

        expect(observed).toEqual([7, 11]);
        expect(publisher.requests).toEqual([7, 11]);
    });

    it("cancels once and signals once when a mapper throws", () => {
        const publisher = new TrackingPublisher<number>();
        const failure = new Error("decode failed");
        const errors: unknown[] = [];
        const finalSignals: string[] = [];
        let subscription: Subscription | undefined;

        Flux.from(publisher)
            .map(() => {
                throw failure;
            })
            .doFinally(signal => finalSignals.push(signal))
            .subscribe({
                onSubscribe(nextSubscription) {
                    subscription = nextSubscription;
                },
                onNext() {
                    throw new Error("unexpected value");
                },
                onError(error) {
                    errors.push(error);
                },
                onComplete() {
                    throw new Error("unexpected completion");
                }
            });

        subscription?.request(5);
        publisher.next(1);
        publisher.next(2);

        expect(errors).toEqual([failure]);
        expect(publisher.cancellations).toBe(1);
        expect(finalSignals).toEqual(["error"]);
    });

    it("keeps downstream onNext failures at the source boundary without double termination", () => {
        const publisher = new TrackingPublisher<number>();
        const failure = new Error("consumer failed");
        const errors: unknown[] = [];
        let subscription: Subscription | undefined;

        Flux.from(publisher).map(value => value).subscribe({
            onSubscribe(nextSubscription) {
                subscription = nextSubscription;
            },
            onNext() {
                throw failure;
            },
            onError(error) {
                errors.push(error);
            },
            onComplete() {
                throw new Error("unexpected completion");
            }
        });

        subscription?.request(2);
        publisher.nextFromBoundary(1);
        publisher.nextFromBoundary(2);

        expect(errors).toEqual([failure]);
        expect(publisher.cancellations).toBe(1);
    });

    it("forwards subscriber context and cancellation through lifted operators", () => {
        const publisher = new TrackingPublisher<number>();
        const finalSignals: string[] = [];
        const downstreamContext = Context.of("downstream", "present");
        let subscription: Subscription | undefined;
        const subscriber: Subscriber<number> & {currentContext(): Context} = {
            currentContext: () => downstreamContext,
            onSubscribe(nextSubscription) {
                subscription = nextSubscription;
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
        };

        Flux.from(publisher)
            .map(value => value)
            .contextWrite(context => context.put("operator", "present"))
            .doFinally(signal => finalSignals.push(signal))
            .subscribe(subscriber);

        expect(publisher.context?.get("downstream")).toBe("present");
        expect(publisher.context?.get("operator")).toBe("present");

        subscription?.cancel();
        subscription?.cancel();
        expect(publisher.cancellations).toBe(1);
        expect(finalSignals).toEqual(["cancel"]);
    });

    it("does not let a doFinally observer replace completion", () => {
        const publisher = new TrackingPublisher<number>();
        let completed = 0;

        Flux.from(publisher).doFinally(() => {
            throw new Error("observer failed");
        }).subscribe({
            ...passiveSubscriber<number>(),
            onComplete() {
                completed += 1;
            }
        });

        expect(() => publisher.complete()).not.toThrow();
        expect(completed).toBe(1);
    });

    it("does not let a doFinally observer make cancellation throw", () => {
        const publisher = new TrackingPublisher<number>();
        let subscription: Subscription | undefined;

        Flux.from(publisher).doFinally(() => {
            throw new Error("observer failed");
        }).subscribe(passiveSubscriber(next => {
            subscription = next;
        }));

        expect(() => subscription?.cancel()).not.toThrow();
        expect(publisher.cancellations).toBe(1);
    });

    it("keeps the Reactive Streams handshake when doOnSubscribe throws", () => {
        const publisher = new TrackingPublisher<number>();
        const failure = new Error("subscribe observer failed");
        const events: string[] = [];
        const errors: unknown[] = [];

        Flux.from(publisher).doOnSubscribe(() => {
            throw failure;
        }).subscribe({
            onSubscribe() {
                events.push("subscribe");
            },
            onNext() {
                events.push("next");
            },
            onError(error) {
                errors.push(error);
                events.push("error");
            },
            onComplete() {
                events.push("complete");
            }
        });

        expect(events).toEqual(["subscribe", "error"]);
        expect(errors).toEqual([failure]);
        expect(publisher.requests).toEqual([]);
        expect(publisher.cancellations).toBe(1);
    });

    it("does not let a doFinally observer replace async-iterator completion", async () => {
        await expect(Flux.just(1).doFinally(() => {
            throw new Error("observer failed");
        }).toArray()).resolves.toEqual([1]);
    });

    it("suppresses pending signals when a one-to-one lifecycle hook cancels", () => {
        const nextPublisher = new TrackingPublisher<number>();
        const nextEvents: string[] = [];
        let nextSubscription: Subscription | undefined;

        Flux.from(nextPublisher).tap({
            onNext() {
                nextSubscription?.cancel();
            }
        }).subscribe({
            onSubscribe(subscription) {
                nextSubscription = subscription;
                subscription.request(1);
            },
            onNext() {
                nextEvents.push("next");
            },
            onError() {
                nextEvents.push("error");
            },
            onComplete() {
                nextEvents.push("complete");
            }
        });
        nextPublisher.next(1);

        const completionPublisher = new TrackingPublisher<number>();
        const completionEvents: string[] = [];
        let completionSubscription: Subscription | undefined;

        Flux.from(completionPublisher).tap({
            onComplete() {
                completionSubscription?.cancel();
            }
        }).subscribe({
            onSubscribe(subscription) {
                completionSubscription = subscription;
            },
            onNext() {
                completionEvents.push("next");
            },
            onError() {
                completionEvents.push("error");
            },
            onComplete() {
                completionEvents.push("complete");
            }
        });
        completionPublisher.complete();

        expect(nextEvents).toEqual([]);
        expect(nextPublisher.cancellations).toBe(1);
        expect(completionEvents).toEqual([]);
        expect(completionPublisher.cancellations).toBe(1);
    });

    it("does not request upstream after a doOnRequest hook cancels", () => {
        const publisher = new TrackingPublisher<number>();
        let subscription: Subscription | undefined;

        Flux.from(publisher).doOnRequest(() => subscription?.cancel()).subscribe(passiveSubscriber(next => {
            subscription = next;
        }));
        subscription?.request(5);

        expect(publisher.requests).toEqual([]);
        expect(publisher.cancellations).toBe(1);
    });

    it("requests publisher-backed async iteration one item at a time instead of unboundedly", async () => {
        const publisher = new TrackingPublisher<number>();
        const iterator = Flux.from(publisher).distinct()[Symbol.asyncIterator]();

        const pending = iterator.next();
        expect(publisher.requests).toEqual([1]);
        publisher.next(9);
        await expect(pending).resolves.toEqual({done: false, value: 9});

        await iterator.return?.();
        expect(publisher.cancellations).toBe(1);
    });

    it.each([
        ["toArray", (source: Flux<number>) => source.toArray(), [0, 1, 2]],
        ["toPromise", (source: Flux<number>) => source.toPromise(), 2],
        ["collectList", (source: Flux<number>) => source.collectList().block(), [0, 1, 2]],
        ["collect", (source: Flux<number>) => source.collect(() => [] as number[], (values, value) => values.push(value)).block(), [0, 1, 2]],
        ["count", (source: Flux<number>) => source.count().block(), 3],
        ["reduce", (source: Flux<number>) => source.reduce(0, (total, value) => total + value).block(), 3],
        ["last", (source: Flux<number>) => source.last().block(), 2],
        ["thenReturn", (source: Flux<number>) => source.thenReturn("done").block(), "done"],
        ["takeLast", (source: Flux<number>) => source.takeLast(2).toArray(), [1, 2]],
        ["cache", (source: Flux<number>) => source.cache().toArray(), [0, 1, 2]],
        ["doOnTerminate", (source: Flux<number>) => source.doOnTerminate(() => undefined).toArray(), [0, 1, 2]],
        ["buffer", (source: Flux<number>) => source.buffer(2).toArray(), [[0, 1], [2]]]
    ] as const)("requests unbounded demand once for %s", async (_name, consume, expected) => {
        const publisher = new SynchronousRangePublisher(3);

        await expect(consume(Flux.from(publisher).distinct().map(value => value))).resolves.toEqual(expected);
        expect(publisher.requests).toEqual([Number.POSITIVE_INFINITY]);
    });

    it("drains a websocket-like async Publisher in one request round trip", async () => {
        const publisher = new AsyncBatchPublisher(100);

        await expect(Flux.from(publisher).distinct().map(value => value + 1).toArray())
            .resolves.toEqual(Array.from({length: 100}, (_value, index) => index + 1));
        expect(publisher.requests).toEqual([Number.POSITIVE_INFINITY]);
        expect(publisher.turns).toBe(1);
    });

    it("keeps unbounded terminal demand through both thenMany phases", async () => {
        const first = new SynchronousRangePublisher(2);
        const second = new SynchronousRangePublisher(3);

        await expect(Flux.from(first).thenMany(Flux.from(second).skip(1)).toArray()).resolves.toEqual([1, 2]);
        expect(first.requests).toEqual([Number.POSITIVE_INFINITY]);
        expect(second.requests).toEqual([Number.POSITIVE_INFINITY]);
    });

    it("marks eager coordination sources before they start iterating", async () => {
        const first = new SynchronousRangePublisher(2);
        const second = new SynchronousRangePublisher(2);

        const values = await Flux.merge(Flux.from(first), Flux.from(second)).collectList().block();

        expect(values?.sort()).toEqual([0, 0, 1, 1]);
        expect(first.requests).toEqual([Number.POSITIVE_INFINITY]);
        expect(second.requests).toEqual([Number.POSITIVE_INFINITY]);
    });

    it("waits for aggregate downstream demand before requesting upstream unboundedly", () => {
        const publisher = new TrackingPublisher<number>();
        const values: number[][] = [];
        let subscription: Subscription | undefined;

        Flux.from(publisher).collectList().subscribe({
            onSubscribe(nextSubscription) {
                subscription = nextSubscription;
            },
            onNext(value) {
                values.push(value);
            },
            onError(error) {
                throw error;
            },
            onComplete() {
                // no-op
            }
        });

        expect(publisher.requests).toEqual([]);
        subscription?.request(1);
        expect(publisher.requests).toEqual([Number.POSITIVE_INFINITY]);

        publisher.next(1);
        publisher.next(2);
        publisher.complete();
        expect(values).toEqual([[1, 2]]);
    });

    it.each([
        ["collectList", (source: Flux<number>): Flux<unknown> => source.collectList()],
        ["next", (source: Flux<number>): Flux<unknown> => source.next()],
        ["take", (source: Flux<number>): Flux<unknown> => source.take(1)],
        ["buffer", (source: Flux<number>): Flux<unknown> => source.buffer(3)]
    ] as const)("suppresses completion when %s is cancelled from onNext", (_name, transform) => {
        const events: string[] = [];
        let subscription: Subscription | undefined;

        transform(Flux.just(1)).subscribe({
            onSubscribe(nextSubscription) {
                subscription = nextSubscription;
                nextSubscription.request(1);
            },
            onNext() {
                events.push("next");
                subscription?.cancel();
            },
            onError(error) {
                throw error;
            },
            onComplete() {
                events.push("complete");
            }
        });

        expect(events).toEqual(["next"]);
    });

    it("does not cancel an already completed source when terminal validation fails", () => {
        const publisher = new TrackingPublisher<number>();
        const errors: unknown[] = [];

        Flux.from(publisher).last().subscribe({
            onSubscribe(subscription) {
                subscription.request(1);
            },
            onNext() {
                throw new Error("unexpected value");
            },
            onError(error) {
                errors.push(error);
            },
            onComplete() {
                throw new Error("unexpected completion");
            }
        });
        publisher.complete();

        expect(errors).toHaveLength(1);
        expect(publisher.cancellations).toBe(0);
    });

    it("cancels a terminal source before demand without requesting it", () => {
        const publisher = new TrackingPublisher<number>();
        const events: string[] = [];
        let subscription: Subscription | undefined;

        Flux.from(publisher).collectList().subscribe({
            onSubscribe(nextSubscription) {
                subscription = nextSubscription;
            },
            onNext() {
                events.push("next");
            },
            onError() {
                events.push("error");
            },
            onComplete() {
                events.push("complete");
            }
        });
        subscription?.cancel();
        subscription?.cancel();
        publisher.next(1);
        publisher.complete();

        expect(publisher.requests).toEqual([]);
        expect(publisher.cancellations).toBe(1);
        expect(events).toEqual([]);
    });

    it("cancels once and forwards a terminal accumulator failure", () => {
        const publisher = new TrackingPublisher<number>();
        const failure = new Error("accumulator failed");
        const errors: unknown[] = [];

        Flux.from(publisher).reduce(0, () => {
            throw failure;
        }).subscribe({
            onSubscribe(subscription) {
                subscription.request(1);
            },
            onNext() {
                throw new Error("unexpected value");
            },
            onError(error) {
                errors.push(error);
            },
            onComplete() {
                throw new Error("unexpected completion");
            }
        });
        publisher.next(1);

        expect(publisher.requests).toEqual([Number.POSITIVE_INFINITY]);
        expect(publisher.cancelInvocations).toBe(1);
        expect(errors).toEqual([failure]);
    });

    it("does not cancel an early terminal twice when downstream onNext throws", () => {
        const publisher = new TrackingPublisher<number>();
        const failure = new Error("consumer failed");
        const errors: unknown[] = [];

        Flux.from(publisher).next().subscribe({
            onSubscribe(subscription) {
                subscription.request(1);
            },
            onNext() {
                throw failure;
            },
            onError(error) {
                errors.push(error);
            },
            onComplete() {
                throw new Error("unexpected completion");
            }
        });
        publisher.next(1);

        expect(publisher.cancelInvocations).toBe(1);
        expect(errors).toEqual([failure]);
    });

    it("preserves downstream and contextWrite context through terminal collection", () => {
        const publisher = new TrackingPublisher<number>();
        const downstreamContext = Context.of("downstream", "present");
        const subscriber: Subscriber<number[]> & {currentContext(): Context} = {
            currentContext: () => downstreamContext,
            onSubscribe(subscription) {
                subscription.request(1);
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
        };

        Flux.from(publisher)
            .contextWrite(context => context.put("operator", "present"))
            .collectList()
            .subscribe(subscriber);

        expect(publisher.requests).toEqual([Number.POSITIVE_INFINITY]);
        expect(publisher.context?.get("downstream")).toBe("present");
        expect(publisher.context?.get("operator")).toBe("present");
    });

    it("caps unbounded terminal demand at the take limit", async () => {
        const publisher = new SynchronousRangePublisher(10);

        await expect(Flux.from(publisher).take(3).toArray()).resolves.toEqual([0, 1, 2]);
        expect(publisher.requests).toEqual([3]);
    });

    it("does not leak unbounded iteration through a take limit", async () => {
        const publisher = new SynchronousRangePublisher(10);

        await expect(Flux.from(publisher).take(3).distinct().toArray()).resolves.toEqual([0, 1, 2]);
        expect(publisher.requests).toEqual([1, 1, 1]);
    });

    it("trampolines reentrant one-by-one demand through take", () => {
        const publisher = new SynchronousRangePublisher(6_000);
        let subscription: Subscription | undefined;
        let values = 0;
        let completions = 0;

        Flux.from(publisher).take(5_001).subscribe({
            onSubscribe(nextSubscription) {
                subscription = nextSubscription;
                nextSubscription.request(1);
            },
            onNext() {
                values += 1;
                subscription?.request(1);
            },
            onError(error) {
                throw error;
            },
            onComplete() {
                completions += 1;
            }
        });

        expect(values).toBe(5_001);
        expect(completions).toBe(1);
        expect(publisher.maxRequestDepth).toBe(1);
        expect(publisher.requests).toHaveLength(5_001);
    });

    it("scales finite buffer demand and emits the final partial buffer", () => {
        const publisher = new TrackingPublisher<number>();
        const values: number[][] = [];
        let subscription: Subscription | undefined;

        Flux.from(publisher).buffer(3).subscribe({
            onSubscribe(nextSubscription) {
                subscription = nextSubscription;
            },
            onNext(value) {
                values.push(value);
            },
            onError(error) {
                throw error;
            },
            onComplete() {
                // no-op
            }
        });

        subscription?.request(2);
        expect(publisher.requests).toEqual([6]);
        publisher.next(1);
        publisher.next(2);
        publisher.next(3);
        publisher.next(4);
        publisher.complete();
        expect(values).toEqual([[1, 2, 3], [4]]);
    });

    it("trampolines reentrant buffer demand", () => {
        const publisher = new SynchronousRangePublisher(6_000);
        let subscription: Subscription | undefined;
        let buffers = 0;

        Flux.from(publisher).buffer(3).subscribe({
            onSubscribe(nextSubscription) {
                subscription = nextSubscription;
                nextSubscription.request(1);
            },
            onNext() {
                buffers += 1;
                subscription?.request(1);
            },
            onError(error) {
                throw error;
            },
            onComplete() {
                // no-op
            }
        });

        expect(buffers).toBe(2_000);
        expect(publisher.maxRequestDepth).toBe(1);
        expect(publisher.requests).toHaveLength(2_000);
        expect(publisher.requests.every(request => request === 3)).toBe(true);
    });

    it.each([
        ["next", 3, (source: Flux<number>) => source.next().block(), 0, 1],
        ["single", 1, (source: Flux<number>) => source.single().block(), 0, 2],
        ["elementAt", 5, (source: Flux<number>) => source.elementAt(2).block(), 2, 3],
        ["hasElements", 3, (source: Flux<number>) => source.hasElements().block(), true, 1]
    ] as const)("uses one bounded batch for %s", async (_name, count, consume, expected, request) => {
        const publisher = new SynchronousRangePublisher(count);

        await expect(consume(Flux.from(publisher))).resolves.toEqual(expected);
        expect(publisher.requests).toEqual([request]);
    });
});

describe("filter demand compensation", () => {
    it("compensates synchronous drops without recursive request calls", () => {
        const publisher = new SynchronousRangePublisher(5_001);
        const values: number[] = [];

        Flux.from(publisher).filter(value => value === 5_000).subscribe({
            onSubscribe(subscription) {
                subscription.request(1);
            },
            onNext(value) {
                values.push(value);
            },
            onError(error) {
                throw error;
            },
            onComplete() {
                // no-op
            }
        });

        expect(values).toEqual([5_000]);
        expect(publisher.maxRequestDepth).toBe(1);
        expect(publisher.requests).toHaveLength(5_001);
        expect(publisher.requests.every(request => request === 1)).toBe(true);
    });

    it("does not compensate dropped values after unbounded demand", () => {
        const publisher = new TrackingPublisher<number>();

        Flux.from(publisher).filter(() => false).subscribe({
            ...passiveSubscriber<number>(),
            onSubscribe(subscription) {
                subscription.request(Number.POSITIVE_INFINITY);
            }
        });

        publisher.next(1);
        publisher.next(2);
        expect(publisher.requests).toEqual([Number.POSITIVE_INFINITY]);
    });

    it("cancels once and signals once when the predicate throws", () => {
        const publisher = new TrackingPublisher<number>();
        const failure = new Error("predicate failed");
        const errors: unknown[] = [];
        const values: number[] = [];
        let subscription: Subscription | undefined;

        Flux.from(publisher).filter(() => {
            throw failure;
        }).subscribe({
            onSubscribe(nextSubscription) {
                subscription = nextSubscription;
            },
            onNext(value) {
                values.push(value);
            },
            onError(error) {
                errors.push(error);
            },
            onComplete() {
                // no-op
            }
        });

        subscription?.request(2);
        publisher.next(1);
        publisher.next(2);

        expect(values).toEqual([]);
        expect(errors).toEqual([failure]);
        expect(publisher.cancellations).toBe(1);
    });
});

describe("Reactive Streams request validation", () => {
    it.each([Number.NaN, Number.NEGATIVE_INFINITY])("rejects invalid non-finite demand %s", request => {
        const errors: unknown[] = [];
        const values: number[] = [];

        Flux.just(1).subscribe({
            onSubscribe(subscription) {
                subscription.request(request);
            },
            onNext(value) {
                values.push(value);
            },
            onError(error) {
                errors.push(error);
            },
            onComplete() {
                // no-op
            }
        });

        expect(values).toEqual([]);
        expect(errors).toHaveLength(1);
        expect(errors[0]).toBeInstanceOf(RangeError);
    });
});

/** Publisher whose subscription exposes every demand and cancellation signal to tests. */
class TrackingPublisher<T> implements Publisher<T> {
    public readonly requests: number[] = [];
    public cancellations = 0;
    public cancelInvocations = 0;
    public context: Context | undefined;
    private subscriber: Subscriber<T> | undefined;
    private cancelled = false;

    public subscribe(subscriber: Subscriber<T>): void {
        this.subscriber = subscriber;
        this.context = (subscriber as Subscriber<T> & {currentContext?(): Context}).currentContext?.();
        subscriber.onSubscribe({
            request: request => this.requests.push(request),
            cancel: () => {
                this.cancelInvocations += 1;
                if (!this.cancelled) {
                    this.cancelled = true;
                    this.cancellations += 1;
                }
            }
        });
    }

    public next(value: T): void {
        if (!this.cancelled) {
            this.subscriber?.onNext(value);
        }
    }

    public nextFromBoundary(value: T): void {
        if (this.cancelled || !this.subscriber) {
            return;
        }
        try {
            this.subscriber.onNext(value);
        } catch (error) {
            this.cancelled = true;
            this.cancellations += 1;
            this.subscriber.onError(error);
        }
    }

    public complete(): void {
        if (!this.cancelled) {
            this.subscriber?.onComplete();
        }
    }
}

/** Synchronous source used to prove filter compensation is trampolined. */
class SynchronousRangePublisher implements Publisher<number> {
    public readonly requests: number[] = [];
    public maxRequestDepth = 0;
    private requestDepth = 0;
    private index = 0;
    private cancelled = false;
    private subscriber: Subscriber<number> | undefined;

    public constructor(private readonly count: number) {
    }

    public subscribe(subscriber: Subscriber<number>): void {
        this.subscriber = subscriber;
        subscriber.onSubscribe({
            request: request => this.emit(request),
            cancel: () => {
                this.cancelled = true;
            }
        });
    }

    private emit(request: number): void {
        this.requests.push(request);
        this.requestDepth += 1;
        this.maxRequestDepth = Math.max(this.maxRequestDepth, this.requestDepth);
        try {
            let emitted = 0;
            while (!this.cancelled && emitted < request && this.index < this.count) {
                const value = this.index;
                this.index += 1;
                emitted += 1;
                this.subscriber?.onNext(value);
            }
            if (!this.cancelled && this.index === this.count) {
                this.subscriber?.onComplete();
            }
        } finally {
            this.requestDepth -= 1;
        }
    }
}

/** Async demand source where every request models one network round trip. */
class AsyncBatchPublisher implements Publisher<number> {
    public readonly requests: number[] = [];
    public turns = 0;
    private index = 0;
    private cancelled = false;
    private subscriber: Subscriber<number> | undefined;

    public constructor(private readonly count: number) {
    }

    public subscribe(subscriber: Subscriber<number>): void {
        this.subscriber = subscriber;
        subscriber.onSubscribe({
            request: demand => {
                this.requests.push(demand);
                queueMicrotask(() => this.emit(demand));
            },
            cancel: () => {
                this.cancelled = true;
            }
        });
    }

    private emit(demand: number): void {
        if (this.cancelled) {
            return;
        }
        this.turns += 1;
        let emitted = 0;
        while (!this.cancelled && emitted < demand && this.index < this.count) {
            this.subscriber?.onNext(this.index);
            this.index += 1;
            emitted += 1;
        }
        if (!this.cancelled && this.index === this.count) {
            this.subscriber?.onComplete();
        }
    }
}

/** Creates a no-op subscriber with an optional subscription hook. */
function passiveSubscriber<T>(onSubscribe: (subscription: Subscription) => void = () => undefined): Subscriber<T> {
    return {
        onSubscribe,
        onNext() {
            // no-op
        },
        onError(error) {
            throw error;
        },
        onComplete() {
            // no-op
        }
    };
}
