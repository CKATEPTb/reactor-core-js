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

    it("requests publisher-backed async iteration one item at a time instead of unboundedly", async () => {
        const publisher = new TrackingPublisher<number>();
        const iterator = Flux.from(publisher)[Symbol.asyncIterator]();

        const pending = iterator.next();
        expect(publisher.requests).toEqual([1]);
        publisher.next(9);
        await expect(pending).resolves.toEqual({done: false, value: 9});

        await iterator.return?.();
        expect(publisher.cancellations).toBe(1);
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
    public context: Context | undefined;
    private subscriber: Subscriber<T> | undefined;
    private cancelled = false;

    public subscribe(subscriber: Subscriber<T>): void {
        this.subscriber = subscriber;
        this.context = (subscriber as Subscriber<T> & {currentContext?(): Context}).currentContext?.();
        subscriber.onSubscribe({
            request: request => this.requests.push(request),
            cancel: () => {
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
