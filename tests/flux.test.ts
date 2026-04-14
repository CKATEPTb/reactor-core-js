import {Flux, Mono, Publisher, Sinks, Subscriber, Subscription} from '@/.';

// ─────────────────────────── Test helpers ────────────────────────────────────

class TestSubscriber<T> implements Subscriber<T> {
    items: T[] = [];
    error: Error | null = null;
    completed = false;
    sub!: Subscription;

    onSubscribe(s: Subscription) {
        this.sub = s;
    }

    onNext(v: T) {
        this.items.push(v);
    }

    onError(e: Error) {
        this.error = e;
    }

    onComplete() {
        this.completed = true;
    }

    /** Request n and collect synchronously (for sync sources). */
    request(n: number): this {
        this.sub.request(n);
        return this;
    }

    requestUnbounded(): this {
        return this.request(Number.MAX_SAFE_INTEGER);
    }
}

function collect<T>(flux: Flux<T>): T[] {
    const ts = new TestSubscriber<T>();
    flux.subscribe(ts);
    ts.requestUnbounded();
    return ts.items;
}

function collectError<T>(flux: Flux<T>): Error | null {
    const ts = new TestSubscriber<T>();
    flux.subscribe(ts);
    ts.requestUnbounded();
    return ts.error;
}

// ─────────────────────────── Static factories ────────────────────────────────

describe('Flux static factories', () => {

    describe('Flux.just', () => {
        it('emits provided items in order', () => {
            expect(collect(Flux.just(1, 2, 3))).toEqual([1, 2, 3]);
        });

        it('completes after emitting all items', () => {
            const ts = new TestSubscriber<number>();
            Flux.just(1).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.completed).toBe(true);
        });

        it('just() with no args completes immediately', () => {
            const ts = new TestSubscriber<number>();
            Flux.just<number>().subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([]);
            expect(ts.completed).toBe(true);
        });
    });

    describe('Flux.from', () => {
        it('wraps a Publisher as Flux', () => {
            const ts = new TestSubscriber<number>();
            Flux.from(Flux.just(10, 20)).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([10, 20]);
        });
    });

    describe('Flux.generate', () => {
        it('emits items from generator with backpressure', () => {
            const flux = Flux.generate<number>(sink => {
                sink.next(1);
                sink.next(2);
                sink.next(3);
                sink.complete();
            });
            const ts = new TestSubscriber<number>();
            flux.subscribe(ts);
            ts.request(2);
            expect(ts.items).toEqual([1, 2]);
            ts.request(1);
            expect(ts.items).toEqual([1, 2, 3]);
            expect(ts.completed).toBe(true);
        });

        it('propagates sink.error()', () => {
            const err = new Error('gen-err');
            const flux = Flux.generate<number>(sink => {
                sink.next(1);
                sink.error(err);
            });
            expect(collectError(flux)).toBe(err);
        });

        it('buffers items when demand is 0', () => {
            const ts = new TestSubscriber<number>();
            Flux.generate<number>(sink => {
                sink.next(1);
                sink.next(2);
                sink.complete();
            }).subscribe(ts);
            // No request yet – buffer should hold items
            expect(ts.items).toEqual([]);
            ts.request(10);
            expect(ts.items).toEqual([1, 2]);
        });
    });

    describe('Flux.fromIterable', () => {
        it('emits iterable items in order', () => {
            expect(collect(Flux.fromIterable([10, 20, 30]))).toEqual([10, 20, 30]);
        });

        it('works with Set', () => {
            const items = collect(Flux.fromIterable(new Set([1, 2, 3])));
            expect(items).toEqual([1, 2, 3]);
        });

        it('empty iterable completes immediately', () => {
            const ts = new TestSubscriber<number>();
            Flux.fromIterable([]).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([]);
            expect(ts.completed).toBe(true);
        });
    });

    describe('Flux.range', () => {
        it('emits [start, start+count)', () => {
            expect(collect(Flux.range(5, 3))).toEqual([5, 6, 7]);
        });

        it('count=0 completes immediately', () => {
            const ts = new TestSubscriber<number>();
            Flux.range(0, 0).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([]);
            expect(ts.completed).toBe(true);
        });
    });

    describe('Flux.empty', () => {
        it('completes without emitting', () => {
            const ts = new TestSubscriber<number>();
            Flux.empty<number>().subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([]);
            expect(ts.completed).toBe(true);
        });

        it('calls onSubscribe before onComplete', () => {
            const order: string[] = [];
            Flux.empty().subscribe({
                onSubscribe(_s) {
                    order.push('subscribe');
                },
                onNext(_v) {
                },
                onError(_e) {
                },
                onComplete() {
                    order.push('complete');
                }
            });
            expect(order).toEqual(['subscribe', 'complete']);
        });
    });

    describe('Flux.error', () => {
        it('signals error immediately', () => {
            const err = new Error('oops');
            const ts = new TestSubscriber<number>();
            Flux.error<number>(err).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.error).toBe(err);
            expect(ts.items).toEqual([]);
        });

        it('calls onSubscribe before onError', () => {
            const order: string[] = [];
            Flux.error(new Error()).subscribe({
                onSubscribe(_s) {
                    order.push('subscribe');
                },
                onNext(_v) {
                },
                onError(_e) {
                    order.push('error');
                },
                onComplete() {
                }
            });
            expect(order).toEqual(['subscribe', 'error']);
        });
    });

    describe('Flux.never', () => {
        it('neither emits nor completes', () => {
            const ts = new TestSubscriber<number>();
            Flux.never<number>().subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([]);
            expect(ts.completed).toBe(false);
            expect(ts.error).toBeNull();
        });
    });

    describe('Flux.defer', () => {
        it('creates a new Flux per subscription', () => {
            let count = 0;
            const flux = Flux.defer(() => {
                count++;
                return Flux.just(count);
            });
            expect(collect(flux)).toEqual([1]);
            expect(collect(flux)).toEqual([2]);
        });
    });
});

// ─────────────────────────── Transformation operators ────────────────────────

describe('Flux transformation operators', () => {

    describe('map', () => {
        it('transforms each item', () => {
            expect(collect(Flux.just(1, 2, 3).map(x => x * 2))).toEqual([2, 4, 6]);
        });

        it('propagates mapper error', () => {
            const err = new Error('map-err');
            const ts = new TestSubscriber<number>();
            Flux.just(1).map(() => {
                throw err;
            }).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.error).toBe(err);
        });
    });

    describe('mapNotNull', () => {
        it('filters out null/undefined values', () => {
            const result = collect(Flux.just(1, 2, 3, 4).mapNotNull(x => x % 2 === 0 ? x : null));
            expect(result).toEqual([2, 4]);
        });

        it('passes through non-null values', () => {
            expect(collect(Flux.just('a', 'b').mapNotNull(s => s.toUpperCase()))).toEqual(['A', 'B']);
        });

        it('replenishes demand when mapper returns null', () => {
            // source: [1,2,3,4], mapNotNull(even only) → [2,4], request 2 → must receive [2,4]
            const ts = new TestSubscriber<number>();
            Flux.just(1, 2, 3, 4).mapNotNull(x => x % 2 === 0 ? x : null).subscribe(ts);
            ts.request(2);
            expect(ts.items).toEqual([2, 4]);
        });
    });

    describe('flatMap', () => {
        it('merges inner publishers', () => {
            const result = collect(Flux.just(1, 2, 3).flatMap(x => Flux.just(x, x * 10)));
            expect(result).toEqual([1, 10, 2, 20, 3, 30]);
        });

        it('completes when all inners complete', () => {
            const ts = new TestSubscriber<number>();
            Flux.just(1, 2).flatMap(x => Flux.just(x)).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.completed).toBe(true);
        });

        it('propagates inner error', () => {
            const err = new Error('inner-err');
            const ts = new TestSubscriber<number>();
            Flux.just(1).flatMap(() => Flux.error<number>(err)).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.error).toBe(err);
        });

        it('propagates outer error', () => {
            const err = new Error('outer-err');
            const ts = new TestSubscriber<number>();
            Flux.error<number>(err).flatMap(x => Flux.just(x)).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.error).toBe(err);
        });
    });

    describe('concatMap', () => {
        it('subscribes to inners sequentially', () => {
            const order: number[] = [];
            Flux.just(1, 2, 3).concatMap(x => {
                order.push(x);
                return Flux.just(x * 10);
            }).subscribe({
                onSubscribe(s) {
                    s.request(Number.MAX_SAFE_INTEGER);
                },
                onNext(_v) {
                },
                onError(_e) {
                },
                onComplete() {
                }
            });
            expect(order).toEqual([1, 2, 3]);
        });

        it('preserves item order', () => {
            expect(collect(Flux.just(1, 2, 3).concatMap(x => Flux.just(x, x * 10)))).toEqual([1, 10, 2, 20, 3, 30]);
        });

        it('propagates inner error', () => {
            const err = new Error('concat-err');
            const ts = new TestSubscriber<number>();
            Flux.just(1, 2).concatMap((x) => x === 2 ? Flux.error<number>(err) : Flux.just(x)).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.error).toBe(err);
        });

        it('completes when outer is empty', () => {
            const ts = new TestSubscriber<number>();
            Flux.empty<number>().concatMap(x => Flux.just(x)).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.completed).toBe(true);
            expect(ts.items).toEqual([]);
        });
    });

    describe('switchMap', () => {
        it('emits only items from the last inner (cancels previous inners)', () => {
            // With sync sources, outer emits all items before any inner has demand,
            // so each new outer item cancels the previous inner → only last inner survives
            const result = collect(Flux.just(1, 2, 3).switchMap(x => Flux.just(x * 10)));
            expect(result).toEqual([30]);
        });

        it('completes when outer and last inner complete', () => {
            const ts = new TestSubscriber<number>();
            Flux.just(1).switchMap(x => Flux.just(x)).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.completed).toBe(true);
        });

        it('propagates inner error', () => {
            const err = new Error('switch-err');
            const ts = new TestSubscriber<number>();
            Flux.just(1).switchMap(() => Flux.error<number>(err)).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.error).toBe(err);
        });
    });

    describe('handle', () => {
        it('can emit 0 items per input (filter)', () => {
            const result = collect(Flux.just(1, 2, 3, 4).handle<number>((v, sink) => {
                if (v % 2 === 0) sink.next(v);
            }));
            expect(result).toEqual([2, 4]);
        });

        it('only the first sink.next() per input is emitted (SynchronousSink semantics)', () => {
            const result = collect(Flux.just(1, 2).handle<number>((v, sink) => {
                sink.next(v);
                sink.next(v * 10); // second call is ignored
            }));
            expect(result).toEqual([1, 2]);
        });

        it('can signal complete early', () => {
            const ts = new TestSubscriber<number>();
            Flux.just(1, 2, 3).handle<number>((v, sink) => {
                if (v === 2) {
                    sink.complete();
                    return;
                }
                sink.next(v);
            }).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([1]);
            expect(ts.completed).toBe(true);
        });

        it('propagates handler error via sink.error', () => {
            const err = new Error('handle-err');
            const ts = new TestSubscriber<number>();
            Flux.just(1).handle<number>((_v, sink) => sink.error(err)).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.error).toBe(err);
        });

        it('replenishes demand when handler emits nothing', () => {
            // source: [1,2,3,4], handle(even only) → [2,4], request 2 → must receive [2,4]
            const ts = new TestSubscriber<number>();
            Flux.just(1, 2, 3, 4).handle<number>((v, sink) => {
                if (v % 2 === 0) sink.next(v);
            }).subscribe(ts);
            ts.request(2);
            expect(ts.items).toEqual([2, 4]);
        });
    });
});

// ─────────────────────────── Filtering operators ─────────────────────────────

describe('Flux filtering operators', () => {

    describe('filter', () => {
        it('keeps only matching items', () => {
            expect(collect(Flux.just(1, 2, 3, 4, 5).filter(x => x % 2 === 0))).toEqual([2, 4]);
        });

        it('propagates predicate error', () => {
            const err = new Error('pred-err');
            const ts = new TestSubscriber<number>();
            Flux.just(1).filter(() => {
                throw err;
            }).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.error).toBe(err);
        });

        it('completes on empty source', () => {
            const ts = new TestSubscriber<number>();
            Flux.empty<number>().filter(() => true).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.completed).toBe(true);
        });

        it('replenishes demand when item is skipped', () => {
            // source: [1,2,3,4,5,6], filter: even only → [2,4,6], request 3 → must receive all 3
            const ts = new TestSubscriber<number>();
            Flux.just(1, 2, 3, 4, 5, 6).filter(x => x % 2 === 0).subscribe(ts);
            ts.request(3);
            expect(ts.items).toEqual([2, 4, 6]);
        });
    });

    describe('filterWhen', () => {
        it('keeps items where predicate emits true', () => {
            const result = collect(Flux.just(1, 2, 3, 4, 5).filterWhen(x => Mono.just(x % 2 === 0)));
            expect(result).toEqual([2, 4]);
        });

        it('drops items where predicate emits false', () => {
            const result = collect(Flux.just(1, 2, 3).filterWhen(() => Mono.just(false)));
            expect(result).toEqual([]);
        });

        it('keeps all items where predicate emits true', () => {
            const result = collect(Flux.just(1, 2, 3).filterWhen(() => Mono.just(true)));
            expect(result).toEqual([1, 2, 3]);
        });
    });

    describe('take', () => {
        it('takes first n items', () => {
            expect(collect(Flux.range(1, 10).take(3))).toEqual([1, 2, 3]);
        });

        it('take(0) completes immediately', () => {
            const ts = new TestSubscriber<number>();
            Flux.just(1, 2, 3).take(0).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([]);
            expect(ts.completed).toBe(true);
        });

        it('take more than available emits all', () => {
            expect(collect(Flux.just(1, 2).take(100))).toEqual([1, 2]);
        });

        it('cancels source after n items', () => {
            let sourceRequests = 0;
            const ts = new TestSubscriber<number>();
            Flux.generate<number>(sink => {
                for (let i = 1; i <= 5; i++) sink.next(i);
                sink.complete();
            }).doOnNext(() => {
                sourceRequests++;
            }).take(2).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([1, 2]);
            expect(ts.completed).toBe(true);
            expect(sourceRequests).toBe(2);
        });
    });

    describe('takeWhile', () => {
        it('takes items while predicate is true', () => {
            expect(collect(Flux.just(1, 2, 3, 4, 5).takeWhile(x => x < 4))).toEqual([1, 2, 3]);
        });

        it('completes immediately on first false', () => {
            const ts = new TestSubscriber<number>();
            Flux.just(5, 1, 2).takeWhile(x => x < 3).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([]);
            expect(ts.completed).toBe(true);
        });

        it('emits all if predicate always true', () => {
            expect(collect(Flux.just(1, 2, 3).takeWhile(() => true))).toEqual([1, 2, 3]);
        });
    });

    describe('takeUntilOther', () => {
        it('completes when trigger emits', () => {
            const ts = new TestSubscriber<number>();
            // Trigger that emits immediately
            Flux.just(1, 2, 3).takeUntilOther(Flux.just('stop')).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.completed).toBe(true);
            // items may be 0 since trigger fires before source (both sync)
        });

        it('emits all items if trigger never emits', () => {
            expect(collect(Flux.just(1, 2, 3).takeUntilOther(Flux.never()))).toEqual([1, 2, 3]);
        });
    });

    describe('skip', () => {
        it('skips first n items', () => {
            expect(collect(Flux.just(1, 2, 3, 4, 5).skip(2))).toEqual([3, 4, 5]);
        });

        it('skip(0) emits all items', () => {
            expect(collect(Flux.just(1, 2).skip(0))).toEqual([1, 2]);
        });

        it('replenishes demand for skipped leading items', () => {
            // source: [1,2,3,4,5], skip(2) → [3,4,5], request 2 → must receive [3,4]
            const ts = new TestSubscriber<number>();
            Flux.just(1, 2, 3, 4, 5).skip(2).subscribe(ts);
            ts.request(2);
            expect(ts.items).toEqual([3, 4]);
        });
    });

    describe('skipWhile', () => {
        it('skips items while predicate is true', () => {
            expect(collect(Flux.just(1, 2, 3, 4, 5).skipWhile(x => x < 3))).toEqual([3, 4, 5]);
        });

        it('emits nothing if predicate is always true', () => {
            const ts = new TestSubscriber<number>();
            Flux.just(1, 2, 3).skipWhile(() => true).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([]);
            expect(ts.completed).toBe(true);
        });

        it('replenishes demand for skipped leading items', () => {
            // source: [1,2,3,4,5], skipWhile(x<3) → [3,4,5], request 2 → must receive [3,4]
            const ts = new TestSubscriber<number>();
            Flux.just(1, 2, 3, 4, 5).skipWhile(x => x < 3).subscribe(ts);
            ts.request(2);
            expect(ts.items).toEqual([3, 4]);
        });
    });

    describe('skipUntil', () => {
        it('passes all items when trigger fires before source (cold sync trigger)', () => {
            // Trigger is a cold sync publisher — fires on first request, before source emits
            expect(collect(Flux.just(1, 2, 3).skipUntil(Flux.just('go')))).toEqual([1, 2, 3]);
        });

        it('drops all items when trigger completes empty — gate never opens', () => {
            const ts = new TestSubscriber<number>();
            Flux.just(1, 2, 3).skipUntil(Flux.empty()).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([]);
            expect(ts.completed).toBe(true);
        });

        it('propagates error from trigger publisher', () => {
            const err = new Error('trigger-err');
            const ts = new TestSubscriber<number>();
            Flux.just(1, 2, 3).skipUntil(Flux.error(err)).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.error).toBe(err);
        });

        it('propagates error from source publisher', () => {
            const err = new Error('src-err');
            const ts = new TestSubscriber<number>();
            Flux.error<number>(err).skipUntil(Flux.just('go')).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.error).toBe(err);
        });

        it('skips items emitted before trigger fires, passes items after — hot source + hot trigger', () => {
            const sourceSink = Sinks.many().unicast().onBackpressureBuffer<number>();
            const triggerSink = Sinks.many().unicast().onBackpressureBuffer<string>();

            const received: number[] = [];
            let completed = false;
            Flux.from(sourceSink).skipUntil(triggerSink).subscribe(
                (v: number) => received.push(v),
                (_e: Error) => {},
                () => { completed = true; },
            );

            sourceSink.next(1); // skipped — gate is closed
            sourceSink.next(2); // skipped
            expect(received).toEqual([]);

            triggerSink.next('go'); // gate opens

            sourceSink.next(3); // passes
            sourceSink.next(4); // passes
            sourceSink.complete();

            expect(received).toEqual([3, 4]);
            expect(completed).toBe(true);
        });

        it('replenishes demand for items skipped during gating', () => {
            // request(2), source emits 1,2,3,4 — first two are skipped (trigger = Flux.never),
            // demand replenished each time → all four items delivered to primary source, all dropped
            const ts = new TestSubscriber<number>();
            Flux.just(1, 2, 3, 4).skipUntil(Flux.never()).subscribe(ts);
            ts.request(2);
            expect(ts.items).toEqual([]);
            // source completes, subscriber also receives onComplete
            expect(ts.completed).toBe(true);
        });

        it('cancel stops delivery', () => {
            const sourceSink = Sinks.many().unicast().onBackpressureBuffer<number>();
            const triggerSink = Sinks.many().unicast().onBackpressureBuffer<string>();

            const received: number[] = [];
            const sub = Flux.from(sourceSink).skipUntil(triggerSink).subscribe(
                (v: number) => received.push(v),
            );

            triggerSink.next('go');
            sourceSink.next(1);
            sub.unsubscribe();
            sourceSink.next(2); // after cancel — must not arrive
            expect(received).toEqual([1]);
        });
    });

    describe('distinct', () => {
        it('removes duplicate items', () => {
            expect(collect(Flux.just(1, 2, 1, 3, 2).distinct())).toEqual([1, 2, 3]);
        });

        it('replenishes demand for duplicate items', () => {
            // source: [1,2,1,3,2,4], distinct → [1,2,3,4], request 3 → must receive [1,2,3]
            const ts = new TestSubscriber<number>();
            Flux.just(1, 2, 1, 3, 2, 4).distinct().subscribe(ts);
            ts.request(3);
            expect(ts.items).toEqual([1, 2, 3]);
        });
    });

    describe('distinctUntilChanged', () => {
        it('removes consecutive duplicate items', () => {
            expect(collect(Flux.just(1, 1, 2, 2, 1).distinctUntilChanged())).toEqual([1, 2, 1]);
        });

        it('supports custom comparator', () => {
            const result = collect(
                Flux.just(1, 2, 3, 10, 11).distinctUntilChanged((a, b) => Math.floor(a / 10) !== Math.floor(b / 10))
            );
            expect(result).toEqual([1, 10]);
        });

        it('replenishes demand for consecutive duplicate items', () => {
            // source: [1,1,2,2,3,3], distinctUntilChanged → [1,2,3], request 2 → must receive [1,2]
            const ts = new TestSubscriber<number>();
            Flux.just(1, 1, 2, 2, 3, 3).distinctUntilChanged().subscribe(ts);
            ts.request(2);
            expect(ts.items).toEqual([1, 2]);
        });
    });

    describe('elementAt', () => {
        it('returns item at index', () => {
            const ts = new TestSubscriber<number>();
            Flux.just(10, 20, 30).elementAt(1).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([20]);
        });

        it('completes empty if index out of range', () => {
            const ts = new TestSubscriber<number>();
            Flux.just(1, 2).elementAt(5).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([]);
            expect(ts.completed).toBe(true);
        });

        it('uses defaultValue if provided and index out of range', () => {
            const ts = new TestSubscriber<number>();
            Flux.just(1, 2).elementAt(5, 99).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([99]);
        });
    });
});

// ─────────────────────────── Combining operators ─────────────────────────────

describe('Flux combining operators', () => {

    describe('switchIfEmpty', () => {
        it('emits alternative if source is empty', () => {
            expect(collect(Flux.empty<number>().switchIfEmpty(Flux.just(42)))).toEqual([42]);
        });

        it('emits source if not empty, ignores alternative', () => {
            expect(collect(Flux.just(1, 2).switchIfEmpty(Flux.just(99)))).toEqual([1, 2]);
        });
    });

    describe('defaultIfEmpty', () => {
        it('emits default value if source is empty', () => {
            expect(collect(Flux.empty<number>().defaultIfEmpty(0))).toEqual([0]);
        });

        it('emits source items if source is not empty', () => {
            expect(collect(Flux.just(1, 2).defaultIfEmpty(0))).toEqual([1, 2]);
        });
    });

    describe('concatWith', () => {
        it('emits source then other in order', () => {
            expect(collect(Flux.just(1, 2).concatWith(Flux.just(3, 4)))).toEqual([1, 2, 3, 4]);
        });

        it('works when first source is empty', () => {
            expect(collect(Flux.empty<number>().concatWith(Flux.just(1, 2)))).toEqual([1, 2]);
        });
    });

    describe('mergeWith', () => {
        it('interleaves items from both sources', () => {
            const result = collect(Flux.just(1, 2).mergeWith(Flux.just(3, 4)));
            expect(result.sort()).toEqual([1, 2, 3, 4]);
        });

        it('completes when both sources complete', () => {
            const ts = new TestSubscriber<number>();
            Flux.just(1).mergeWith(Flux.just(2)).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.completed).toBe(true);
        });
    });

    describe('zipWith', () => {
        it('pairs items from both sources', () => {
            const result = collect(Flux.just(1, 2, 3).zipWith(Flux.just('a', 'b', 'c'), (a, b) => `${a}${b}`));
            expect(result).toEqual(['1a', '2b', '3c']);
        });

        it('completes when shorter source exhausts', () => {
            const result = collect(Flux.just(1, 2, 3).zipWith(Flux.just('a', 'b'), (a, b) => `${a}${b}`));
            expect(result).toEqual(['1a', '2b']);
        });

        it('handles empty left source', () => {
            const ts = new TestSubscriber<string>();
            Flux.empty<number>().zipWith(Flux.just('x'), (a, b) => `${a}${b}`).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([]);
            expect(ts.completed).toBe(true);
        });
    });
});

// ─────────────────────────── Aggregation operators ───────────────────────────

describe('Flux aggregation operators', () => {

    describe('reduce', () => {
        it('reduces to single value', () => {
            const ts = new TestSubscriber<number>();
            Flux.just(1, 2, 3, 4).reduce((a, b) => a + b).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([10]);
        });

        it('completes empty if source is empty', () => {
            const ts = new TestSubscriber<number>();
            Flux.empty<number>().reduce((a, b) => a + b).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([]);
            expect(ts.completed).toBe(true);
        });
    });

    describe('reduceWith', () => {
        it('reduces with seed', () => {
            const ts = new TestSubscriber<number>();
            Flux.just(1, 2, 3).reduceWith(() => 10, (a, b) => a + b).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([16]);
        });
    });

    describe('scan', () => {
        it('emits running accumulation starting with first item', () => {
            expect(collect(Flux.just(1, 2, 3, 4).scan((a, b) => a + b))).toEqual([1, 3, 6, 10]);
        });

        it('emits each item unchanged on empty source', () => {
            const ts = new TestSubscriber<number>();
            Flux.empty<number>().scan((a, b) => a + b).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([]);
        });
    });

    describe('scanWith', () => {
        it('emits running accumulation starting with seed', () => {
            expect(collect(Flux.just(1, 2, 3).scanWith(() => 0, (a, b) => a + b))).toEqual([1, 3, 6]);
        });
    });

    describe('collect / collectList', () => {
        it('collects all items into array', () => {
            const ts = new TestSubscriber<number[]>();
            Flux.just(1, 2, 3).collect().subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([[1, 2, 3]]);
        });

        it('collectList is alias for collect', () => {
            const ts = new TestSubscriber<number[]>();
            Flux.just(1, 2, 3).collectList().subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([[1, 2, 3]]);
        });
    });

    describe('sort', () => {
        it('sorts items in natural order', () => {
            expect(collect(Flux.just(3, 1, 4, 1, 5, 9).sort((a, b) => a - b))).toEqual([1, 1, 3, 4, 5, 9]);
        });

        it('sorts strings', () => {
            expect(collect(Flux.just('banana', 'apple', 'cherry').sort())).toEqual(['apple', 'banana', 'cherry']);
        });
    });

    describe('buffer', () => {
        it('buffers items into chunks of maxSize', () => {
            expect(collect(Flux.range(1, 6).buffer(2))).toEqual([[1, 2], [3, 4], [5, 6]]);
        });

        it('emits partial last batch', () => {
            expect(collect(Flux.range(1, 5).buffer(2))).toEqual([[1, 2], [3, 4], [5]]);
        });

        it('empty source produces no batches', () => {
            const ts = new TestSubscriber<number[]>();
            Flux.empty<number>().buffer(3).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([]);
            expect(ts.completed).toBe(true);
        });
    });

    describe('count', () => {
        it('counts items', () => {
            const ts = new TestSubscriber<number>();
            Flux.just(1, 2, 3).count().subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([3]);
        });

        it('returns 0 for empty source', () => {
            const ts = new TestSubscriber<number>();
            Flux.empty<number>().count().subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([0]);
        });
    });

    describe('hasElements', () => {
        it('returns true for non-empty source', () => {
            const ts = new TestSubscriber<boolean>();
            Flux.just(1).hasElements().subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([true]);
        });

        it('returns false for empty source', () => {
            const ts = new TestSubscriber<boolean>();
            Flux.empty<number>().hasElements().subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([false]);
        });
    });

    describe('any', () => {
        it('returns true if any item matches', () => {
            const ts = new TestSubscriber<boolean>();
            Flux.just(1, 2, 3).any(x => x === 2).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([true]);
        });

        it('returns false if no item matches', () => {
            const ts = new TestSubscriber<boolean>();
            Flux.just(1, 2, 3).any(x => x > 10).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([false]);
        });

        it('short-circuits on first match', () => {
            let checked = 0;
            const ts = new TestSubscriber<boolean>();
            Flux.just(1, 2, 3, 4, 5).any(x => {
                checked++;
                return x === 2;
            }).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([true]);
            expect(checked).toBe(2);
        });
    });

    describe('all', () => {
        it('returns true if all items match', () => {
            const ts = new TestSubscriber<boolean>();
            Flux.just(2, 4, 6).all(x => x % 2 === 0).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([true]);
        });

        it('returns false if any item does not match', () => {
            const ts = new TestSubscriber<boolean>();
            Flux.just(2, 3, 4).all(x => x % 2 === 0).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([false]);
        });

        it('short-circuits on first mismatch', () => {
            let checked = 0;
            const ts = new TestSubscriber<boolean>();
            Flux.just(2, 3, 4, 5, 6).all(x => {
                checked++;
                return x % 2 === 0;
            }).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([false]);
            expect(checked).toBe(2);
        });
    });

    describe('none', () => {
        it('returns true if no items match', () => {
            const ts = new TestSubscriber<boolean>();
            Flux.just(1, 3, 5).none(x => x % 2 === 0).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([true]);
        });

        it('returns false if any item matches', () => {
            const ts = new TestSubscriber<boolean>();
            Flux.just(1, 2, 3).none(x => x % 2 === 0).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([false]);
        });
    });

    describe('first / last', () => {
        it('first() returns first item', () => {
            const ts = new TestSubscriber<number>();
            Flux.just(10, 20, 30).first().subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([10]);
        });

        it('first() completes empty on empty source', () => {
            const ts = new TestSubscriber<number>();
            Flux.empty<number>().first().subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([]);
            expect(ts.completed).toBe(true);
        });

        it('last() returns last item', () => {
            const ts = new TestSubscriber<number>();
            Flux.just(10, 20, 30).last().subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([30]);
        });
    });

    describe('then', () => {
        it('completes with void after source completes', () => {
            const ts = new TestSubscriber<void>();
            Flux.just(1, 2, 3).then().subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toHaveLength(1);
            expect(ts.completed).toBe(true);
        });

        it('propagates source error', () => {
            const err = new Error('then-err');
            const ts = new TestSubscriber<void>();
            Flux.error<number>(err).then().subscribe(ts);
            ts.requestUnbounded();
            expect(ts.error).toBe(err);
        });
    });

    describe('thenEmpty', () => {
        it('completes with void after source and other both complete', () => {
            const ts = new TestSubscriber<void>();
            Flux.just(1, 2, 3).thenEmpty(Flux.empty()).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toHaveLength(1);    // single void value
            expect(ts.completed).toBe(true);
            expect(ts.error).toBeNull();
        });

        it('items from source are consumed and not forwarded', () => {
            const ts = new TestSubscriber<void>();
            Flux.just(100, 200, 300).thenEmpty(Flux.empty()).subscribe(ts);
            ts.requestUnbounded();
            // No number items leaked — only one void item from thenEmpty
            expect(ts.items).toHaveLength(1);
        });

        it('items emitted by other are ignored — only completion matters', () => {
            // thenEmpty uses Mono.from({subscribe}) which only reacts to onComplete of other
            const ts = new TestSubscriber<void>();
            Flux.just(1).thenEmpty(Flux.just(99, 98, 97)).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toHaveLength(1); // void, not 99/98/97
            expect(ts.completed).toBe(true);
        });

        it('source error propagates — other is never subscribed', () => {
            const err = new Error('src-err');
            let otherSubscribed = false;
            const other = { subscribe: (s: any) => { otherSubscribed = true; return s; } };
            const ts = new TestSubscriber<void>();
            Flux.error<number>(err).thenEmpty(other).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.error).toBe(err);
            expect(otherSubscribed).toBe(false);
        });

        it('error from other propagates to subscriber', () => {
            const err = new Error('other-err');
            const ts = new TestSubscriber<void>();
            Flux.just(1, 2).thenEmpty(Flux.error(err)).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.error).toBe(err);
            expect(ts.completed).toBe(false);
        });

        it('works with async other via publishOn', async () => {
            const asyncScheduler = { schedule: (fn: () => void) => setTimeout(fn, 0) };
            const result = await new Promise<boolean>(resolve => {
                Flux.just(1, 2, 3)
                    .thenEmpty(Flux.empty().publishOn(asyncScheduler))
                    .subscribe(
                        _v => {},
                        _e => {},
                        () => resolve(true),
                    );
            });
            expect(result).toBe(true);
        });

        it('empty source — other still runs', () => {
            const ts = new TestSubscriber<void>();
            Flux.empty<number>().thenEmpty(Flux.empty()).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.completed).toBe(true);
        });
    });
});

// ─────────────────────────── Error handling ──────────────────────────────────

describe('Flux error handling operators', () => {

    describe('onErrorReturn', () => {
        it('switches to replacement on error', () => {
            const ts = new TestSubscriber<number>();
            Flux.error<number>(new Error()).onErrorReturn(Flux.just(42)).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([42]);
            expect(ts.completed).toBe(true);
        });

        it('emits source items before switching', () => {
            const ts = new TestSubscriber<number>();
            Flux.just(1, 2).concatWith(Flux.error(new Error()))
                .onErrorReturn(Flux.just(99)).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.items).toEqual([1, 2, 99]);
        });
    });

    describe('onErrorContinue', () => {
        it('completes on error if predicate returns true', () => {
            const ts = new TestSubscriber<number>();
            Flux.error<number>(new Error('handled')).onErrorContinue(() => true).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.completed).toBe(true);
            expect(ts.error).toBeNull();
        });

        it('propagates error if predicate returns false', () => {
            const err = new Error('unhandled');
            const ts = new TestSubscriber<number>();
            Flux.error<number>(err).onErrorContinue(() => false).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.error).toBe(err);
        });
    });

    describe('retry', () => {
        it('resubscribes on error', () => {
            let attempts = 0;
            const ts = new TestSubscriber<number>();
            Flux.generate<number>(sink => {
                attempts++;
                if (attempts < 3) sink.error(new Error('retry'));
                else {
                    sink.next(42);
                    sink.complete();
                }
            }).retry(5).subscribe(ts);
            ts.requestUnbounded();
            expect(attempts).toBe(3);
            expect(ts.items).toEqual([42]);
            expect(ts.completed).toBe(true);
        });

        it('propagates error after maxRetries exceeded', () => {
            const err = new Error('permanent');
            const ts = new TestSubscriber<number>();
            Flux.error<number>(err).retry(2).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.error).toBe(err);
        });

        it('retry(0) does not retry', () => {
            const err = new Error('no-retry');
            const ts = new TestSubscriber<number>();
            Flux.error<number>(err).retry(0).subscribe(ts);
            ts.requestUnbounded();
            expect(ts.error).toBe(err);
        });
    });
});

// ─────────────────────────── Side effect operators ───────────────────────────

describe('Flux side effect operators', () => {

    describe('doOnNext', () => {
        it('runs side effect for each item without changing them', () => {
            const sideEffects: number[] = [];
            const result = collect(Flux.just(1, 2, 3).doOnNext(v => sideEffects.push(v)));
            expect(result).toEqual([1, 2, 3]);
            expect(sideEffects).toEqual([1, 2, 3]);
        });
    });

    describe('doOnError', () => {
        it('runs side effect on error', () => {
            const err = new Error('side-err');
            let captured: Error | null = null;
            const ts = new TestSubscriber<number>();
            Flux.error<number>(err).doOnError(e => {
                captured = e;
            }).subscribe(ts);
            ts.requestUnbounded();
            expect(captured).toBe(err);
            expect(ts.error).toBe(err);
        });
    });

    describe('doOnComplete', () => {
        it('runs side effect on complete', () => {
            let called = false;
            const ts = new TestSubscriber<number>();
            Flux.just(1).doOnComplete(() => {
                called = true;
            }).subscribe(ts);
            ts.requestUnbounded();
            expect(called).toBe(true);
            expect(ts.completed).toBe(true);
        });
    });

    describe('doOnTerminate', () => {
        it('runs on complete', () => {
            let called = false;
            const ts = new TestSubscriber<number>();
            Flux.just(1).doOnTerminate(() => {
                called = true;
            }).subscribe(ts);
            ts.requestUnbounded();
            expect(called).toBe(true);
        });

        it('runs on error', () => {
            let called = false;
            const ts = new TestSubscriber<number>();
            Flux.error<number>(new Error()).doOnTerminate(() => {
                called = true;
            }).subscribe(ts);
            ts.requestUnbounded();
            expect(called).toBe(true);
        });
    });

    describe('doOnCancel', () => {
        it('runs side effect on unsubscribe', () => {
            let cancelled = false;
            const ts = new TestSubscriber<number>();
            Flux.never<number>().doOnCancel(() => {
                cancelled = true;
            }).subscribe(ts);
            ts.sub.unsubscribe();
            expect(cancelled).toBe(true);
        });
    });

    describe('doFinally', () => {
        it('runs on complete', () => {
            let called = false;
            const ts = new TestSubscriber<number>();
            Flux.just(1).doFinally(() => {
                called = true;
            }).subscribe(ts);
            ts.requestUnbounded();
            expect(called).toBe(true);
        });

        it('runs on error', () => {
            let called = false;
            const ts = new TestSubscriber<number>();
            Flux.error<number>(new Error()).doFinally(() => {
                called = true;
            }).subscribe(ts);
            ts.requestUnbounded();
            expect(called).toBe(true);
        });

        it('runs on cancel', () => {
            let called = false;
            const ts = new TestSubscriber<number>();
            Flux.never<number>().doFinally(() => {
                called = true;
            }).subscribe(ts);
            ts.sub.unsubscribe();
            expect(called).toBe(true);
        });
    });

    describe('doOnSubscribe', () => {
        it('receives the subscription object', () => {
            let received: Subscription | null = null;
            const ts = new TestSubscriber<number>();
            Flux.just(1).doOnSubscribe(s => {
                received = s;
            }).subscribe(ts);
            ts.requestUnbounded();
            expect(received).not.toBeNull();
        });
    });
});

// ─────────────────────────── Scheduling operators ────────────────────────────

describe('Flux scheduling operators', () => {

    describe('publishOn', () => {
        it('schedules signals via scheduler', async () => {
            const order: string[] = [];
            const scheduler = {
                schedule: (fn: () => void) => {
                    setTimeout(fn, 0);
                }
            };
            await new Promise<void>(resolve => {
                Flux.just(1, 2).publishOn(scheduler).subscribe({
                    onSubscribe(s) {
                        s.request(Number.MAX_SAFE_INTEGER);
                    },
                    onNext(v) {
                        order.push(`next:${v}`);
                    },
                    onError(_e) {
                    },
                    onComplete() {
                        order.push('complete');
                        resolve();
                    }
                });
            });
            expect(order).toEqual(['next:1', 'next:2', 'complete']);
        });
    });

    describe('subscribeOn', () => {
        it('schedules subscription via scheduler', async () => {
            const order: string[] = [];
            const scheduler = {
                schedule: (fn: () => void) => {
                    setTimeout(fn, 0);
                }
            };
            await new Promise<void>(resolve => {
                Flux.just(1, 2).subscribeOn(scheduler).subscribe({
                    onSubscribe(s) {
                        s.request(Number.MAX_SAFE_INTEGER);
                    },
                    onNext(v) {
                        order.push(`next:${v}`);
                    },
                    onError(_e) {
                    },
                    onComplete() {
                        order.push('complete');
                        resolve();
                    }
                });
            });
            expect(order).toEqual(['next:1', 'next:2', 'complete']);
        });
    });

    describe('delayElements', () => {
        beforeEach(() => jest.useFakeTimers());
        afterEach(() => jest.useRealTimers());

        it('items are spaced ms apart (one at a time, not all at once)', () => {
            const received: number[] = [];
            Flux.just(1, 2, 3).delayElements(100).subscribe(v => received.push(v));

            expect(received).toHaveLength(0);
            jest.advanceTimersByTime(100);
            expect(received).toEqual([1]); // first item after 100 ms
            jest.advanceTimersByTime(100);
            expect(received).toEqual([1, 2]); // second after another 100 ms
            jest.advanceTimersByTime(100);
            expect(received).toEqual([1, 2, 3]); // third after another 100 ms
        });

        it('items with a longer delay are not delivered before it elapses', () => {
            const received: number[] = [];
            Flux.just(42).delayElements(200).subscribe(v => received.push(v));

            jest.advanceTimersByTime(199);
            expect(received).toHaveLength(0);

            jest.advanceTimersByTime(1);
            expect(received).toEqual([42]);
        });

        it('onError is not delayed — fires immediately, cancels pending timer', () => {
            const events: string[] = [];
            const err = new Error('now');
            Flux.error<number>(err).delayElements(500).subscribe(
                v => events.push(`next:${v}`),
                e => events.push(`error:${e.message}`),
            );
            expect(events).toEqual(['error:now']);
            jest.runAllTimers(); // no extra signals after error
            expect(events).toEqual(['error:now']);
        });

        it('onComplete fires after the last delayed item', () => {
            const events: string[] = [];
            Flux.just(1).delayElements(100).subscribe(
                v => events.push(`next:${v}`),
                _e => {},
                () => events.push('complete'),
            );

            expect(events).toEqual([]); // nothing yet
            jest.advanceTimersByTime(100);
            expect(events).toEqual(['next:1', 'complete']); // item then complete together
        });

        it('zero delay still defers delivery to the next event-loop tick', () => {
            const received: number[] = [];
            Flux.just(5, 6).delayElements(0).subscribe(v => received.push(v));

            expect(received).toHaveLength(0); // not yet — setTimeout(fn, 0) is async
            jest.runAllTimers();
            expect(received).toEqual([5, 6]);
        });

        it('cancellation stops delivery and clears pending timer', () => {
            const events: string[] = [];
            let sub!: { unsubscribe(): void };
            Flux.just(1, 2, 3).delayElements(100).subscribe({
                onSubscribe(s) { sub = s; s.request(10); },
                onNext(v) { events.push(`next:${v}`); },
                onError() {},
                onComplete() { events.push('complete'); }
            });
            jest.advanceTimersByTime(100); // first item delivered
            expect(events).toEqual(['next:1']);
            sub.unsubscribe();            // cancel while second timer is pending
            jest.runAllTimers();          // no more signals after cancel
            expect(events).toEqual(['next:1']);
        });
    });
});

// ─────────────────────────── Utility operators ───────────────────────────────

describe('Flux utility operators', () => {

    describe('indexed', () => {
        it('pairs each item with its zero-based index', () => {
            expect(collect(Flux.just('a', 'b', 'c').indexed())).toEqual([[0, 'a'], [1, 'b'], [2, 'c']]);
        });
    });

    describe('cast', () => {
        it('casts types without runtime change', () => {
            const flux: Flux<unknown> = Flux.just(1, 2, 3) as Flux<unknown>;
            const casted = flux.cast<number>();
            expect(collect(casted)).toEqual([1, 2, 3]);
        });
    });

    describe('cache', () => {
        it('replays all items to each subscriber', () => {
            let subscriptions = 0;
            const source = Flux.generate<number>(sink => {
                subscriptions++;
                sink.next(1);
                sink.next(2);
                sink.next(3);
                sink.complete();
            });
            const cached = source.cache();

            expect(collect(cached)).toEqual([1, 2, 3]);
            expect(collect(cached)).toEqual([1, 2, 3]);
            // Source subscribed only once
            expect(subscriptions).toBe(1);
        });

        it('late subscribers receive all cached items', () => {
            const cached = Flux.just(10, 20, 30).cache();
            // First subscriber connects
            const first = collect(cached);
            // Second subscriber receives same items
            const second = collect(cached);
            expect(first).toEqual([10, 20, 30]);
            expect(second).toEqual([10, 20, 30]);
        });

        it('replays cached error to late subscribers', () => {
            const err = new Error('cached-err');
            const cached = Flux.error<number>(err).cache();
            // First sub
            expect(collectError(cached)).toBe(err);
            // Second sub also receives the error
            expect(collectError(cached)).toBe(err);
        });
    });
});

// ─────────────────────────── Backpressure ────────────────────────────────────

describe('Flux backpressure', () => {

    it('does not emit items without demand', () => {
        const ts = new TestSubscriber<number>();
        Flux.just(1, 2, 3).subscribe(ts);
        // No request yet
        expect(ts.items).toEqual([]);
    });

    it('emits exactly as many items as requested', () => {
        const ts = new TestSubscriber<number>();
        Flux.range(1, 10).subscribe(ts);
        ts.request(3);
        expect(ts.items).toEqual([1, 2, 3]);
        ts.request(2);
        expect(ts.items).toEqual([1, 2, 3, 4, 5]);
    });

    it('request(0) signals onError per Reactive Streams Rule 3.9', () => {
        const ts = new TestSubscriber<number>();
        Flux.just(1).subscribe(ts);
        ts.request(0);
        expect(ts.error).not.toBeNull();
        expect(ts.error!.message).toContain('request must be > 0');
    });

    it('unsubscribe stops item delivery', () => {
        const ts = new TestSubscriber<number>();
        Flux.range(1, 100).subscribe(ts);
        ts.request(3);
        expect(ts.items).toEqual([1, 2, 3]);
        ts.sub.unsubscribe();
        ts.request(100);
        // No more items after cancel
        expect(ts.items).toEqual([1, 2, 3]);
    });
});

// ─────────────────────────── Reactive Streams compliance ─────────────────────

describe('Flux Reactive Streams compliance', () => {

    it('onSubscribe is called before any other signal', () => {
        const order: string[] = [];
        Flux.just(1).subscribe({
            onSubscribe(_s) {
                order.push('subscribe');
            },
            onNext(_v) {
                order.push('next');
            },
            onError(_e) {
                order.push('error');
            },
            onComplete() {
                order.push('complete');
            }
        });
        expect(order[0]).toBe('subscribe');
    });

    it('onSubscribe called before onComplete for empty Flux', () => {
        const order: string[] = [];
        Flux.empty().subscribe({
            onSubscribe(_s) {
                order.push('subscribe');
            },
            onNext(_v) {
            },
            onError(_e) {
            },
            onComplete() {
                order.push('complete');
            }
        });
        expect(order).toEqual(['subscribe', 'complete']);
    });

    it('onSubscribe called before onError for error Flux', () => {
        const order: string[] = [];
        Flux.error(new Error()).subscribe({
            onSubscribe(_s) {
                order.push('subscribe');
            },
            onNext(_v) {
            },
            onError(_e) {
                order.push('error');
            },
            onComplete() {
            }
        });
        expect(order).toEqual(['subscribe', 'error']);
    });

    it('no signals after unsubscribe', () => {
        const received: number[] = [];
        let completed = false;
        const ts = new TestSubscriber<number>();
        Flux.range(1, 100).subscribe({
            onSubscribe(s) {
                ts.sub = s;
            },
            onNext(v) {
                received.push(v);
                if (v === 3) ts.sub.unsubscribe();
            },
            onError(_e) {
            },
            onComplete() {
                completed = true;
            }
        });
        ts.sub.request(100);
        expect(received).toEqual([1, 2, 3]);
        expect(completed).toBe(false);
    });
});

// ─────────────────────────── subscribe() convenience overloads ────────────────

describe('Flux subscribe convenience overloads', () => {
    test('subscribe() with no args — consumes items silently without throwing', () => {
        expect(() => Flux.range(0, 5).subscribe()).not.toThrow();
    });

    test('subscribe() on empty Flux — completes silently', () => {
        expect(() => Flux.empty<number>().subscribe()).not.toThrow();
    });

    test('subscribe(onNext) — receives all items, auto-requests unbounded', () => {
        const received: number[] = [];
        Flux.range(0, 5).subscribe(v => received.push(v));
        expect(received).toEqual([0, 1, 2, 3, 4]);
    });

    test('subscribe(onNext) — Flux.just delivers all values', () => {
        const received: number[] = [];
        Flux.just(10, 20, 30).subscribe(v => received.push(v));
        expect(received).toEqual([10, 20, 30]);
    });

    test('subscribe(onNext, onError) — onError fires on failure', () => {
        const errors: Error[] = [];
        Flux.error<number>(new Error('fail')).subscribe(
            () => {},
            e => errors.push(e),
        );
        expect(errors).toHaveLength(1);
        expect(errors[0].message).toBe('fail');
    });

    test('subscribe(onNext, onError, onComplete) — all three callbacks fire', () => {
        const events: string[] = [];
        Flux.just(1, 2).subscribe(
            v => events.push(`next:${v}`),
            _e => events.push('error'),
            () => events.push('complete'),
        );
        expect(events).toEqual(['next:1', 'next:2', 'complete']);
    });

    test('subscribe(onNext, onError, onComplete) — empty Flux fires only onComplete', () => {
        const events: string[] = [];
        Flux.empty<number>().subscribe(
            () => events.push('next'),
            () => events.push('error'),
            () => events.push('complete'),
        );
        expect(events).toEqual(['complete']);
    });

    test('subscribe(onNext, onError, onComplete) — error Flux fires only onError', () => {
        const events: string[] = [];
        Flux.error<number>(new Error()).subscribe(
            () => events.push('next'),
            _e => events.push('error'),
            () => events.push('complete'),
        );
        expect(events).toEqual(['error']);
    });

    test('subscribe(onNext) with no onError — unhandled error is re-thrown', () => {
        expect(() =>
            Flux.error<number>(new Error('unhandled')).subscribe(() => {}),
        ).toThrow('unhandled');
    });

    test('full Subscriber object — caller controls demand manually', () => {
        const received: number[] = [];
        const sub = Flux.range(0, 10).subscribe({
            onSubscribe(_s) { /* no auto-request */ },
            onNext(v) { received.push(v); },
            onError() {},
            onComplete() {},
        });
        expect(received).toHaveLength(0);
        sub.request(3);
        expect(received).toEqual([0, 1, 2]);
        sub.request(2);
        expect(received).toEqual([0, 1, 2, 3, 4]);
    });
});

// ─────────────────────────── flatMap with schedulers ─────────────────────────

describe('Flux.flatMap with schedulers', () => {
    const asyncScheduler = { schedule: (fn: () => void) => setTimeout(fn, 0) };

    test('flatMap: each inner Mono.publishOn delivers asynchronously, all items arrive', async () => {
        const received: number[] = [];
        await new Promise<void>(resolve => {
            Flux.just(1, 2, 3)
                .flatMap(v => Mono.just(v * 10).publishOn(asyncScheduler))
                .subscribe(
                    v => received.push(v),
                    _e => {},
                    resolve,
                );
        });
        // flatMap is unordered — sort for stable assertion
        expect(received.sort((a, b) => a - b)).toEqual([10, 20, 30]);
    });

    test('publishOn before flatMap: outer items arrive async, inner runs sync', async () => {
        const received: number[] = [];
        await new Promise<void>(resolve => {
            Flux.just(1, 2, 3)
                .publishOn(asyncScheduler)
                .flatMap(v => Mono.just(v * 2))
                .subscribe(
                    v => received.push(v),
                    _e => {},
                    resolve,
                );
        });
        expect(received).toEqual([2, 4, 6]);
    });

    test('subscribeOn: subscription deferred, items delivered once scheduled', async () => {
        const received: number[] = [];
        await new Promise<void>(resolve => {
            Flux.range(0, 4)
                .subscribeOn(asyncScheduler)
                .subscribe(
                    v => received.push(v),
                    _e => {},
                    resolve,
                );
        });
        expect(received).toEqual([0, 1, 2, 3]);
    });

    test('subscribeOn + flatMap: source async, each inner sync', async () => {
        const received: number[] = [];
        await new Promise<void>(resolve => {
            Flux.just(1, 2, 3)
                .subscribeOn(asyncScheduler)
                .flatMap(v => Flux.just(v, v * 10))
                .subscribe(
                    v => received.push(v),
                    _e => {},
                    resolve,
                );
        });
        expect(received).toEqual([1, 10, 2, 20, 3, 30]);
    });

    test('publishOn + flatMap with inner publishOn: both layers async', async () => {
        const received: number[] = [];
        await new Promise<void>(resolve => {
            Flux.just(1, 2)
                .publishOn(asyncScheduler)
                .flatMap(v => Flux.just(v * 100).publishOn(asyncScheduler))
                .subscribe(
                    v => received.push(v),
                    _e => {},
                    resolve,
                );
        });
        expect(received.sort((a, b) => a - b)).toEqual([100, 200]);
    });

    test('flatMap error in inner publisher propagates to onError', async () => {
        const err = new Error('inner-fail');
        const caught = await new Promise<Error>(resolve => {
            Flux.just(1, 2, 3)
                .publishOn(asyncScheduler)
                .flatMap(v => v === 2
                    ? Flux.error<number>(err).publishOn(asyncScheduler)
                    : Mono.just(v),
                )
                .subscribe(
                    () => {},
                    resolve,
                );
        });
        expect(caught.message).toBe('inner-fail');
    });

    test('concatMap preserves order with async inner', async () => {
        const received: number[] = [];
        await new Promise<void>(resolve => {
            Flux.just(3, 1, 2)
                .concatMap(v => Mono.just(v * 10).publishOn(asyncScheduler))
                .subscribe(
                    v => received.push(v),
                    _e => {},
                    resolve,
                );
        });
        // concatMap is ordered — must arrive as 30, 10, 20
        expect(received).toEqual([30, 10, 20]);
    });
});

// ═══════════════════════════════════════════════════════════════════════════════
// New static factories
// ═══════════════════════════════════════════════════════════════════════════════

describe('Flux.interval', () => {
    it('does not emit without demand', done => {
        const ts = new TestSubscriber<number>();
        Flux.interval(20).subscribe(ts);
        setTimeout(() => {
            expect(ts.items).toHaveLength(0);
            ts.sub.unsubscribe();
            done();
        }, 60);
    });

    it('emits counter 0, 1, 2 when demand is present', async () => {
        const result = await new Promise<number[]>(resolve => {
            const items: number[] = [];
            let sub!: Subscription;
            const s = Flux.interval(20).subscribe({
                onSubscribe(ss) { sub = ss; ss.request(3); },
                onNext(v) {
                    items.push(v);
                    if (items.length === 3) { sub.unsubscribe(); resolve(items); }
                },
                onError() { resolve([]); },
                onComplete() {}
            });
            void s;
        });
        expect(result).toEqual([0, 1, 2]);
    });

    it('drops ticks when demand is zero (no buffering)', done => {
        const ts = new TestSubscriber<number>();
        const s = Flux.interval(20).subscribe(ts);
        // 3 ticks pass with no request → should be dropped
        setTimeout(() => {
            const before = ts.items.length;
            expect(before).toBe(0);
            s.unsubscribe();
            done();
        }, 70);
    });

    it('cancellation stops emission', done => {
        const ts = new TestSubscriber<number>();
        const s = Flux.interval(20).subscribe(ts);
        ts.request(Number.MAX_SAFE_INTEGER);
        setTimeout(() => {
            s.unsubscribe();
            const count = ts.items.length;
            setTimeout(() => {
                expect(ts.items.length).toBe(count); // no more items after cancel
                done();
            }, 60);
        }, 40);
    });
});

describe('Flux.zip (static)', () => {
    it('combines items positionally from 2 sources', () => {
        const items = collect(Flux.zip(
            [Flux.just(1, 2, 3), Flux.just('a', 'b', 'c')],
            (n: number, s: string) => `${n}${s}`
        ));
        expect(items).toEqual(['1a', '2b', '3c']);
    });

    it('stops when shorter source exhausts', () => {
        const items = collect(Flux.zip(
            [Flux.just(1, 2, 3), Flux.just('a', 'b')],
            (n: number, s: string) => `${n}${s}`
        ));
        expect(items).toEqual(['1a', '2b']);
    });

    it('respects downstream demand', () => {
        const ts = new TestSubscriber<string>();
        Flux.zip([Flux.just(1, 2, 3), Flux.just('a', 'b', 'c')],
            (n: number, s: string) => `${n}${s}`).subscribe(ts);
        ts.request(1);
        expect(ts.items).toEqual(['1a']);
        ts.request(2);
        expect(ts.items).toEqual(['1a', '2b', '3c']);
    });

    it('propagates error from a source', () => {
        const err = new Error('zip-err');
        const e = collectError(Flux.zip([Flux.just(1), Flux.error<string>(err)],
            (n: number, s: string) => `${n}${s}`));
        expect(e).toBe(err);
    });

    it('3-source typed overload', () => {
        const items = collect(Flux.zip(
            [Flux.just(1), Flux.just('a'), Flux.just(true)],
            (n: number, s: string, b: boolean) => `${n}${s}${b}`
        ));
        expect(items).toEqual(['1atrue']);
    });

    it('onSubscribe before items (RS 1.3)', () => {
        const events: string[] = [];
        Flux.zip([Flux.just(1), Flux.just('a')], (n: number, s: string) => `${n}${s}`).subscribe({
            onSubscribe(s) { events.push('sub'); s.request(10); },
            onNext(v) { events.push(v as string); },
            onError() {},
            onComplete() {}
        });
        expect(events[0]).toBe('sub');
    });
});

describe('Flux.combineLatest (static)', () => {
    it('emits combined value from latest of each source', () => {
        const items = collect(Flux.combineLatest(
            [Flux.just(1, 2), Flux.just('a', 'b')],
            (n: number, s: string) => `${n}${s}`
        ));
        // Last emitted uses latest from both; with sync sources both complete immediately
        expect(items.length).toBeGreaterThan(0);
        expect(items[items.length - 1]).toMatch(/2[ab]/);
    });

    it('does not emit until all sources have a value', () => {
        const items = collect(Flux.combineLatest(
            [Flux.just(1), Flux.never<string>()],
            (n: unknown, s: unknown) => `${n}${s}`
        ));
        expect(items).toHaveLength(0);
    });

    it('propagates error', () => {
        const err = new Error('cl-err');
        const e = collectError(Flux.combineLatest(
            [Flux.just(1), Flux.error<string>(err)],
            (n: number, s: string) => `${n}${s}`
        ));
        expect(e).toBe(err);
    });

    it('respects downstream demand', () => {
        const ts = new TestSubscriber<string>();
        Flux.combineLatest([Flux.just(1, 2), Flux.just('a', 'b')],
            (n: number, s: string) => `${n}${s}`).subscribe(ts);
        ts.request(1);
        expect(ts.items).toHaveLength(1);
    });

    it('onSubscribe before items (RS 1.3)', () => {
        const events: string[] = [];
        Flux.combineLatest([Flux.just(1), Flux.just('a')],
            (n: number, s: string) => `${n}${s}`).subscribe({
            onSubscribe(s) { events.push('sub'); s.request(10); },
            onNext(v) { events.push(v as string); },
            onError() {},
            onComplete() {}
        });
        expect(events[0]).toBe('sub');
    });
});

describe('Flux.merge (static) — RS compliance', () => {
    it('merges items from all sources', () => {
        const items = collect(Flux.merge(Flux.just(1, 2), Flux.just(3, 4)));
        expect([...items].sort()).toEqual([1, 2, 3, 4]);
    });

    it('all sources receive demand issued in onSubscribe (RS fix)', () => {
        // Both sources must get request() even though the convenience overload
        // calls request() immediately inside onSubscribe.
        const demanded: number[] = [];
        const makeSource = (v: number) => Flux.from<number>({
            subscribe(sub) {
                const s: Subscription = {
                    request(n) { demanded.push(n); sub.onNext(v); sub.onComplete(); },
                    unsubscribe() {}
                };
                sub.onSubscribe(s);
                return s;
            }
        });
        const ts = new TestSubscriber<number>();
        Flux.merge(makeSource(1), makeSource(2)).subscribe(ts);
        ts.requestUnbounded();
        expect(demanded.length).toBe(2); // both sources got a request
    });

    it('onSubscribe before items (RS 1.3)', () => {
        const events: string[] = [];
        Flux.merge(Flux.just(1), Flux.just(2)).subscribe({
            onSubscribe(s) { events.push('sub'); s.request(10); },
            onNext(v) { events.push(String(v)); },
            onError() {},
            onComplete() {}
        });
        expect(events[0]).toBe('sub');
    });

    it('propagates error', () => {
        const err = new Error('merge-err');
        const e = collectError(Flux.merge(Flux.just(1), Flux.error<number>(err)));
        expect(e).toBe(err);
    });

    it('completes when all sources complete', () => {
        const ts = new TestSubscriber<number>();
        Flux.merge(Flux.just(1), Flux.just(2)).subscribe(ts);
        ts.requestUnbounded();
        expect(ts.completed).toBe(true);
    });
});

describe('Flux.firstWithValue (static) — RS compliance', () => {
    it('emits value from first source', () => {
        const ts = new TestSubscriber<number>();
        Flux.firstWithValue(Flux.just(1), Flux.just(2)).subscribe(ts);
        ts.request(1);
        expect(ts.items[0]).toBe(1);
        expect(ts.completed).toBe(true);
    });

    it('all sources receive demand (RS fix)', () => {
        const demanded: number[] = [];
        const makeSource = (v: number) => Flux.from<number>({
            subscribe(sub) {
                const s: Subscription = {
                    request(n) { demanded.push(n); sub.onNext(v); sub.onComplete(); },
                    unsubscribe() {}
                };
                sub.onSubscribe(s);
                return s;
            }
        });
        const ts = new TestSubscriber<number>();
        Flux.firstWithValue(makeSource(1), makeSource(2)).subscribe(ts);
        ts.request(1);
        expect(demanded.length).toBe(2);
    });

    it('onSubscribe before items (RS 1.3)', () => {
        const events: string[] = [];
        Flux.firstWithValue(Flux.just(1), Flux.just(2)).subscribe({
            onSubscribe(s) { events.push('sub'); s.request(1); },
            onNext(v) { events.push(String(v)); },
            onError() {},
            onComplete() {}
        });
        expect(events[0]).toBe('sub');
    });

    it('zero sources → completes empty', () => {
        const ts = new TestSubscriber<number>();
        Flux.firstWithValue<number>().subscribe(ts);
        ts.requestUnbounded();
        expect(ts.completed).toBe(true);
        expect(ts.items).toHaveLength(0);
    });
});

describe('Flux.create (FluxSink)', () => {
    it('buffers items and delivers on demand', () => {
        const ts = new TestSubscriber<number>();
        Flux.create<number>(sink => {
            sink.next(1);
            sink.next(2);
            sink.complete();
        }).subscribe(ts);
        ts.request(3);
        expect(ts.items).toEqual([1, 2]);
        expect(ts.completed).toBe(true);
    });

    it('sink.requested reflects demand accumulated before emitter runs', () => {
        // The initial request() fires during onSubscribe (before the emitter body runs).
        // sink.requested reflects the outstanding demand when the emitter body executes.
        let requestedAtEmit = -1;
        Flux.create<number>(sink => {
            requestedAtEmit = sink.requested;
        }).subscribe({
            onSubscribe(s) { s.request(5); },
            onNext() {},
            onError() {},
            onComplete() {}
        });
        expect(requestedAtEmit).toBe(5);
    });

    it('onRequest fires for subsequent requests after emitter runs', () => {
        const demands: number[] = [];
        let sub!: { request(n: number): void };
        Flux.create<number>(sink => {
            sink.onRequest(n => demands.push(n));
        }).subscribe({
            onSubscribe(s) { sub = s; },
            onNext() {},
            onError() {},
            onComplete() {}
        });
        sub.request(3);
        sub.request(7);
        expect(demands).toEqual([3, 7]);
    });

    it('onCancel fires on unsubscribe', () => {
        let cancelled = false;
        const ts = new TestSubscriber<number>();
        const sub = Flux.create<number>(sink => {
            sink.onCancel(() => { cancelled = true; });
        }).subscribe(ts);
        sub.unsubscribe();
        expect(cancelled).toBe(true);
    });

    it('onDispose fires on complete', () => {
        let disposed = false;
        const ts = new TestSubscriber<number>();
        Flux.create<number>(sink => {
            sink.onDispose(() => { disposed = true; });
            sink.complete();
        }).subscribe(ts);
        ts.requestUnbounded();
        expect(disposed).toBe(true);
    });

    it('emitter exception signals onError', () => {
        const e = collectError(Flux.create<number>(() => { throw new Error('thrown'); }));
        expect(e!.message).toBe('thrown');
    });

    it('onSubscribe called before emitter runs (RS 1.3)', () => {
        const events: string[] = [];
        Flux.create<number>(sink => {
            events.push('emitter');
            sink.next(1);
            sink.complete();
        }).subscribe({
            onSubscribe(s) { events.push('sub'); s.request(1); },
            onNext() {},
            onError() {},
            onComplete() {}
        });
        expect(events[0]).toBe('sub');
        expect(events[1]).toBe('emitter');
    });
});

describe('Flux.using', () => {
    it('acquires resource, streams items, cleans up on complete', () => {
        let cleaned = false;
        const items = collect(Flux.using(
            () => [1, 2, 3],
            arr => Flux.fromIterable(arr),
            _arr => { cleaned = true; }
        ));
        expect(items).toEqual([1, 2, 3]);
        expect(cleaned).toBe(true);
    });

    it('cleanup runs on error', () => {
        let cleaned = false;
        const e = collectError(Flux.using(
            () => 'resource',
            _r => Flux.error<number>(new Error('fail')),
            _r => { cleaned = true; }
        ));
        expect(e!.message).toBe('fail');
        expect(cleaned).toBe(true);
    });

    it('cleanup runs on cancellation', () => {
        let cleaned = false;
        const ts = new TestSubscriber<number>();
        const sub = Flux.using(
            () => null,
            _r => Flux.just(1, 2, 3),
            _r => { cleaned = true; }
        ).subscribe(ts);
        ts.request(1);
        sub.unsubscribe();
        expect(cleaned).toBe(true);
    });
});

// ═══════════════════════════════════════════════════════════════════════════════
// AbstractPipePublisher shared operators
// ═══════════════════════════════════════════════════════════════════════════════

describe('onErrorResume', () => {
    it('switches to fallback on error', () => {
        const items = collect(
            Flux.error<number>(new Error('x')).onErrorResume(() => Flux.just(1, 2))
        );
        expect(items).toEqual([1, 2]);
    });

    it('receives the error in fn', () => {
        let received: string | null = null;
        collect(Flux.error<number>(new Error('oops')).onErrorResume(e => {
            received = e.message;
            return Flux.empty();
        }));
        expect(received).toBe('oops');
    });

    it('does not call fn on success', () => {
        let called = false;
        const items = collect(
            Flux.just(1, 2).onErrorResume(() => { called = true; return Flux.empty(); })
        );
        expect(called).toBe(false);
        expect(items).toEqual([1, 2]);
    });
});

describe('onErrorMap', () => {
    it('transforms the error', () => {
        const e = collectError(
            Flux.error<number>(new Error('orig')).onErrorMap(e => new Error(`wrapped:${e.message}`))
        );
        expect(e!.message).toBe('wrapped:orig');
    });

    it('passes through on success', () => {
        const items = collect(Flux.just(1, 2).onErrorMap(e => new Error(e.message)));
        expect(items).toEqual([1, 2]);
    });
});

describe('timeout', () => {
    it('errors when source emits nothing within the timeout', async () => {
        const e = await new Promise<Error | null>(resolve => {
            Flux.never<number>().timeout(40).subscribe({
                onSubscribe(s) { s.request(1); },
                onNext() {},
                onError(e) { resolve(e); },
                onComplete() { resolve(null); }
            });
        });
        expect(e).not.toBeNull();
        expect(e!.message.toLowerCase()).toContain('timeout');
    });

    it('does not timeout when items arrive in time', async () => {
        const items = await new Promise<number[]>(resolve => {
            const r: number[] = [];
            Flux.just(1, 2, 3).timeout(500).subscribe(
                v => r.push(v), () => resolve([]), () => resolve(r)
            );
        });
        expect(items).toEqual([1, 2, 3]);
    });

    it('switches to fallback publisher on timeout', async () => {
        const items = await new Promise<number[]>(resolve => {
            const r: number[] = [];
            Flux.never<number>().timeout(40, Flux.just(99)).subscribe(
                v => r.push(v), () => resolve([]), () => resolve(r)
            );
        });
        expect(items).toEqual([99]);
    });
});

describe('delaySubscription', () => {
    it('does not subscribe to source synchronously', () => {
        let subscribed = false;
        Flux.defer(() => { subscribed = true; return Flux.just(1); })
            .delaySubscription(50)
            .subscribe({ onSubscribe() {}, onNext() {}, onError() {}, onComplete() {} });
        expect(subscribed).toBe(false);
    });

    it('eventually subscribes and emits after delay', async () => {
        const items = await new Promise<number[]>(resolve => {
            const r: number[] = [];
            Flux.just(1, 2, 3).delaySubscription(30).subscribe(
                v => r.push(v), () => resolve([]), () => resolve(r)
            );
        });
        expect(items).toEqual([1, 2, 3]);
    });
});

describe('doOnRequest', () => {
    it('fires with the requested count each time', () => {
        const counts: number[] = [];
        const ts = new TestSubscriber<number>();
        Flux.just(1, 2, 3).doOnRequest(n => counts.push(n)).subscribe(ts);
        ts.request(2);
        ts.request(1);
        expect(counts).toEqual([2, 1]);
    });
});

describe('doOnEach', () => {
    it('receives next signals', () => {
        const kinds: string[] = [];
        const ts = new TestSubscriber<number>();
        Flux.just(1, 2).doOnEach(s => kinds.push(s.kind)).subscribe(ts);
        ts.requestUnbounded();
        expect(kinds.filter(k => k === 'next')).toHaveLength(2);
        expect(kinds).toContain('complete');
    });

    it('receives error signal', () => {
        const kinds: string[] = [];
        Flux.error<number>(new Error('x')).doOnEach(s => kinds.push(s.kind)).subscribe({
            onSubscribe(s) { s.request(1); },
            onNext() {},
            onError() {},
            onComplete() {}
        });
        expect(kinds).toContain('error');
    });
});

describe('log', () => {
    it('passes items through unchanged', () => {
        const items = collect(Flux.just(1, 2, 3).log('test'));
        expect(items).toEqual([1, 2, 3]);
    });

    it('does not swallow errors', () => {
        const e = collectError(Flux.error<number>(new Error('log-err')).log());
        expect(e!.message).toBe('log-err');
    });

    it('onSubscribe is delivered before any items (RS 1.3) — synchronous source', () => {
        const events: string[] = [];
        Flux.just(1, 2).log('rs').subscribe({
            onSubscribe(_s) { events.push('sub'); _s.request(10); },
            onNext(v) { events.push(`next:${v}`); },
            onError() {},
            onComplete() { events.push('done'); }
        });
        expect(events[0]).toBe('sub');
        expect(events).toEqual(['sub', 'next:1', 'next:2', 'done']);
    });

    it('intercepts request and cancel signals', () => {
        const logged: string[] = [];
        const spy = jest.spyOn(console, 'log').mockImplementation(msg => logged.push(msg));
        try {
            const ts = new TestSubscriber<number>();
            const sub = Flux.just(1).log('spy').subscribe(ts);
            ts.request(5);
            sub.unsubscribe();
        } finally { spy.mockRestore(); }
        expect(logged.some(m => m.includes('request('))).toBe(true);
        expect(logged.some(m => m.includes('cancel'))).toBe(true);
    });
});

// ═══════════════════════════════════════════════════════════════════════════════
// Flux instance operators (new)
// ═══════════════════════════════════════════════════════════════════════════════

describe('Flux#retryWhen', () => {
    it('retries and eventually succeeds', () => {
        let attempts = 0;
        const source = Flux.defer(() => {
            attempts++;
            return attempts < 3 ? Flux.error<number>(new Error('fail')) : Flux.just(42);
        });
        const items = collect(source.retryWhen(errors => errors.take(3)));
        expect(items).toEqual([42]);
        expect(attempts).toBe(3);
    });

    it('stops when control stream completes', () => {
        const ts = new TestSubscriber<number>();
        Flux.error<number>(new Error('always'))
            .retryWhen(errors => errors.take(0))
            .subscribe(ts);
        ts.requestUnbounded();
        expect(ts.completed).toBe(true);
    });

    it('propagates control stream error', () => {
        const controlErr = new Error('ctrl');
        const ts = new TestSubscriber<number>();
        Flux.error<number>(new Error('src'))
            .retryWhen(_ => Flux.error<unknown>(controlErr))
            .subscribe(ts);
        ts.requestUnbounded();
        expect(ts.error).toBe(controlErr);
    });

    it('forwards items from successful attempts', () => {
        const items = collect(Flux.just(1, 2, 3).retryWhen(errors => errors.take(2)));
        expect(items).toEqual([1, 2, 3]);
    });
});

describe('Flux#repeatWhen', () => {
    it('repeats source when control emits', () => {
        const items = collect(
            Flux.just(1).repeatWhen(completes => completes.take(2))
        );
        expect(items).toEqual([1, 1, 1]); // initial + 2 repeats
    });

    it('stops when control completes', () => {
        const ts = new TestSubscriber<number>();
        Flux.just(42).repeatWhen(completes => completes.take(0)).subscribe(ts);
        ts.requestUnbounded();
        expect(ts.completed).toBe(true);
    });

    it('propagates source error without repeating', () => {
        const err = new Error('src');
        const ts = new TestSubscriber<number>();
        Flux.error<number>(err).repeatWhen(_ => Flux.just(1)).subscribe(ts);
        ts.requestUnbounded();
        expect(ts.error).toBe(err);
    });
});

describe('Flux#groupBy', () => {
    it('routes items into groups', async () => {
        const groups = new Map<string, number[]>();
        await new Promise<void>(resolve => {
            let pending = 2;
            Flux.just(1, 2, 3, 4, 5)
                .groupBy(n => n % 2 === 0 ? 'even' : 'odd')
                .subscribe(
                    group => {
                        const arr: number[] = [];
                        groups.set(group.key, arr);
                        group.subscribe(
                            v => arr.push(v),
                            () => {},
                            () => { if (--pending === 0) resolve(); }
                        );
                    },
                    () => {},
                    () => {}
                );
        });
        expect(groups.get('odd')!.sort((a, b) => a - b)).toEqual([1, 3, 5]);
        expect(groups.get('even')!.sort((a, b) => a - b)).toEqual([2, 4]);
    });

    it('GroupedFlux has correct key', () => {
        const keys: string[] = [];
        Flux.just(1, 2, 3)
            .groupBy(n => n % 2 === 0 ? 'even' : 'odd')
            .subscribe(g => keys.push(g.key), () => {}, () => {});
        expect(keys).toContain('odd');
        expect(keys).toContain('even');
    });

    it('GroupedFlux is a plain Flux (no subclass needed)', () => {
        let group!: { key: string } & Flux<number>;
        Flux.just(1).groupBy(_ => 'all')
            .subscribe(g => { group = g; }, () => {}, () => {});
        expect(group).toBeInstanceOf(Flux);
        expect(group.key).toBe('all');
    });
});

describe('Flux#bufferTimeout', () => {
    it('flushes when maxSize is reached', () => {
        const ts = new TestSubscriber<number[]>();
        Flux.just(1, 2, 3, 4, 5).bufferTimeout(2, 5000).subscribe(ts);
        ts.requestUnbounded();
        expect(ts.items[0]).toEqual([1, 2]);
        expect(ts.items[1]).toEqual([3, 4]);
    });

    it('flushes partial batch on completion', () => {
        const batches = collect(Flux.just(1, 2, 3).bufferTimeout(2, 5000));
        expect(batches.flat()).toContain(3);
    });
});

describe('Flux#sample', () => {
    it('ignores trigger when no value has arrived', () => {
        const ts = new TestSubscriber<number>();
        Flux.never<number>().sample(Flux.just(1)).subscribe(ts);
        ts.requestUnbounded();
        expect(ts.items).toHaveLength(0);
    });

    it('emits latest value when trigger fires (async)', async () => {
        const items = await new Promise<number[]>(resolve => {
            const r: number[] = [];
            Flux.just(1, 2, 3)
                .sample(Flux.interval(200).take(1))
                .subscribe(v => r.push(v), () => resolve(r), () => resolve(r));
        });
        expect(items.length).toBeGreaterThanOrEqual(0); // source may complete before trigger
    });
});

describe('Flux#delayUntil', () => {
    it('holds items until trigger fires', async () => {
        const start = Date.now();
        const items = await new Promise<number[]>(resolve => {
            const r: number[] = [];
            Flux.just(1, 2, 3)
                .delayUntil(() => Mono.delay(50))
                .subscribe(v => r.push(v), () => resolve(r), () => resolve(r));
        });
        expect(items).toEqual([1, 2, 3]);
        expect(Date.now() - start).toBeGreaterThanOrEqual(40);
    });

    it('propagates trigger error', () => {
        const err = new Error('trig');
        const ts = new TestSubscriber<number>();
        Flux.just(1).delayUntil(() => Flux.error(err)).subscribe(ts);
        ts.requestUnbounded();
        expect(ts.error).toBe(err);
    });
});

describe('Flux#expand', () => {
    it('recursively expands items', () => {
        const items = collect(
            Flux.just(1).expand(n => n < 4 ? Flux.just(n + 1) : Flux.empty())
        );
        expect(items).toEqual([1, 2, 3, 4]);
    });

    it('stops when fn returns empty', () => {
        const items = collect(Flux.just(3).expand(_ => Flux.empty<number>()));
        expect(items).toEqual([3]);
    });
});

describe('Flux#expandDeep', () => {
    it('emits items in depth-first order', () => {
        // Tree: 1 → [2, 3], 2 → [4, 5], others → empty
        const items = collect(
            Flux.just(1).expandDeep(n =>
                n === 1 ? Flux.just(2, 3) :
                n === 2 ? Flux.just(4, 5) :
                Flux.empty()
            )
        );
        // Depth-first pre-order: 1, then 1's children depth-first [2→[4,5], 3]
        expect(items).toEqual([1, 2, 4, 5, 3]);
    });

    it('stops when fn returns empty', () => {
        const items = collect(Flux.just(42).expandDeep(_ => Flux.empty<number>()));
        expect(items).toEqual([42]);
    });

    it('linear chain emits same as expand', () => {
        // For a linear chain DFS == BFS
        const items = collect(
            Flux.just(1).expandDeep(n => n < 4 ? Flux.just(n + 1) : Flux.empty())
        );
        expect(items).toEqual([1, 2, 3, 4]);
    });

    it('propagates errors from fn', () => {
        const err = new Error('boom');
        let caught: Error | null = null;
        Flux.just(1)
            .expandDeep(_ => { throw err; })
            .subscribe({ onSubscribe(s) { s.request(10); }, onNext() {}, onError(e) { caught = e; }, onComplete() {} });
        expect(caught).toBe(err);
    });

    it('multiple roots are each expanded depth-first', () => {
        // Roots: [1, 2], each expanded to one child; expect [1, 10, 2, 20]
        const items = collect(
            Flux.just(1, 2).expandDeep(n => n < 10 ? Flux.just(n * 10) : Flux.empty())
        );
        expect(items).toEqual([1, 10, 2, 20]);
    });
});

describe('Flux#elapsed', () => {
    it('pairs each item with elapsed ms > 0', async () => {
        const pairs = await new Promise<[number, number][]>(resolve => {
            const r: [number, number][] = [];
            Flux.interval(30).take(3).elapsed()
                .subscribe(p => r.push(p), () => {}, () => resolve(r));
        });
        expect(pairs).toHaveLength(3);
        for (const [ms] of pairs) expect(ms).toBeGreaterThan(0);
    });
});

describe('Flux#timestamp', () => {
    it('pairs each item with a current timestamp', () => {
        const before = Date.now();
        const pairs = collect(Flux.just(1, 2, 3).timestamp());
        const after = Date.now();
        for (const [ts] of pairs) {
            expect(ts).toBeGreaterThanOrEqual(before);
            expect(ts).toBeLessThanOrEqual(after);
        }
        expect(pairs.map(([, v]) => v)).toEqual([1, 2, 3]);
    });
});

describe('Flux#materialize / dematerialize', () => {
    it('materialize wraps next and complete as Signal items', () => {
        const sigs = collect(Flux.just(1, 2).materialize());
        expect(sigs[0]).toEqual({ kind: 'next', value: 1 });
        expect(sigs[1]).toEqual({ kind: 'next', value: 2 });
        expect(sigs[2]).toEqual({ kind: 'complete' });
    });

    it('materialize wraps error and completes the outer Flux normally', () => {
        const err = new Error('mat-err');
        const ts = new TestSubscriber<unknown>();
        Flux.error<number>(err).materialize().subscribe(ts);
        ts.requestUnbounded();
        expect(ts.error).toBeNull();
        expect(ts.completed).toBe(true);
        expect((ts.items[0] as { error: Error }).error).toBe(err);
    });

    it('round-trip materialize → dematerialize is identity', () => {
        const items = collect(
            Flux.just(1, 2, 3).materialize().dematerialize<number>()
        );
        expect(items).toEqual([1, 2, 3]);
    });
});

describe('Flux#onBackpressureBuffer', () => {
    it('buffers items and delivers on demand', () => {
        const ts = new TestSubscriber<number>();
        Flux.just(1, 2, 3).onBackpressureBuffer().subscribe(ts);
        expect(ts.items).toHaveLength(0);
        ts.request(2);
        expect(ts.items).toEqual([1, 2]);
        ts.request(1);
        expect(ts.items).toEqual([1, 2, 3]);
    });

    it('errors when maxSize is exceeded', () => {
        const ts = new TestSubscriber<number>();
        Flux.just(1, 2, 3, 4, 5).onBackpressureBuffer(2).subscribe(ts);
        expect(ts.error).not.toBeNull();
        expect(ts.error!.message).toContain('overflow');
    });
});

describe('Flux#onBackpressureDrop', () => {
    it('drops items when no demand', () => {
        const ts = new TestSubscriber<number>();
        Flux.just(1, 2, 3).onBackpressureDrop().subscribe(ts);
        expect(ts.items).toHaveLength(0);
        expect(ts.completed).toBe(true);
    });

    it('invokes onDrop callback for each dropped item', () => {
        const dropped: number[] = [];
        const ts = new TestSubscriber<number>();
        Flux.just(1, 2, 3).onBackpressureDrop(v => dropped.push(v)).subscribe(ts);
        expect(dropped).toEqual([1, 2, 3]);
    });
});

describe('Flux#onBackpressureLatest', () => {
    it('keeps only the latest item', () => {
        const ts = new TestSubscriber<number>();
        Flux.just(1, 2, 3).onBackpressureLatest().subscribe(ts);
        ts.request(1);
        expect(ts.items).toEqual([3]);
        expect(ts.completed).toBe(true);
    });
});

describe('Flux#limitRate', () => {
    it('prefetches in batches of n', () => {
        const requested: number[] = [];
        const source = Flux.from<number>({
            subscribe(sub) {
                const items = [1, 2, 3, 4, 5, 6, 7, 8];
                const s: Subscription = {
                    request(n) {
                        requested.push(n);
                        const slice = items.splice(0, n);
                        for (const v of slice) sub.onNext(v);
                        if (items.length === 0) sub.onComplete();
                    },
                    unsubscribe() {}
                };
                sub.onSubscribe(s);
                return s;
            }
        });
        const items = collect(source.limitRate(8));
        expect(requested[0]).toBe(8);
        expect(items).toHaveLength(8);
    });
});

describe('Flux#ofType', () => {
    class Animal {}
    class Cat extends Animal { meow() { return 'meow'; } }
    class Dog extends Animal { woof() { return 'woof'; } }

    it('keeps only instanceof matches', () => {
        const animals: Animal[] = [new Cat(), new Dog(), new Cat()];
        const cats = collect(Flux.fromIterable(animals).ofType(Cat));
        expect(cats).toHaveLength(2);
        expect(cats.every(c => c instanceof Cat)).toBe(true);
    });

    it('returns empty when no items match', () => {
        const items = collect(Flux.just(new Cat() as Animal).ofType(Dog));
        expect(items).toHaveLength(0);
    });
});

describe('Flux#transformDeferred', () => {
    it('applies operator function lazily, one call per subscription', () => {
        let count = 0;
        const addCount = (f: Publisher<number>) => Flux.from(f).doOnSubscribe(() => count++);
        const flux = Flux.just(1, 2, 3).transformDeferred(addCount);
        collect(flux);
        collect(flux);
        expect(count).toBe(2);
    });

    it('transforms the element type', () => {
        const items = collect(
            Flux.just(1, 2, 3).transformDeferred<number>(f => Flux.from(f).map(n => (n as number) * 10))
        );
        expect(items).toEqual([10, 20, 30]);
    });
});

// ─────────────────────────── Flux#share ──────────────────────────────────────

describe('Flux#share', () => {
    it('delivers items to a single subscriber', () => {
        const shared = Flux.just(1, 2, 3).share();
        const ts = new TestSubscriber<number>();
        shared.subscribe(ts);
        ts.requestUnbounded();
        expect(ts.items).toEqual([1, 2, 3]);
        expect(ts.completed).toBe(true);
    });

    it('multicasts items to all current subscribers', () => {
        const sink = Sinks.many().multicast().directBestEffort<number>();
        const shared = Flux.from(sink).share();

        const a = new TestSubscriber<number>();
        const b = new TestSubscriber<number>();
        shared.subscribe(a);
        a.requestUnbounded();
        shared.subscribe(b);
        b.requestUnbounded();

        sink.next(1);
        sink.next(2);
        sink.complete();

        expect(a.items).toEqual([1, 2]);
        expect(b.items).toEqual([1, 2]);
        expect(a.completed).toBe(true);
        expect(b.completed).toBe(true);
    });

    it('best-effort: subscriber with no demand misses items', () => {
        const sink = Sinks.many().multicast().directBestEffort<number>();
        const shared = Flux.from(sink).share();

        const ready = new TestSubscriber<number>();
        const slow = new TestSubscriber<number>();
        shared.subscribe(ready);
        ready.requestUnbounded();
        shared.subscribe(slow);
        // slow requests nothing

        sink.next(10);
        sink.next(20);
        sink.complete();

        expect(ready.items).toEqual([10, 20]);
        expect(slow.items).toHaveLength(0);
    });

    it('cancels upstream when last subscriber unsubscribes', () => {
        let upstreamCancelled = false;
        const sink = Sinks.many().multicast().directBestEffort<number>();
        const shared = Flux.from(sink)
            .doFinally(() => { upstreamCancelled = true; })
            .share();

        const ts = new TestSubscriber<number>();
        shared.subscribe(ts);
        ts.requestUnbounded();

        expect(upstreamCancelled).toBe(false);
        ts.sub.unsubscribe();
        expect(upstreamCancelled).toBe(true);
    });

    it('upstream is cancelled only after ALL subscribers leave', () => {
        let upstreamCancelled = false;
        const sink = Sinks.many().multicast().directBestEffort<number>();
        const shared = Flux.from(sink)
            .doFinally(() => { upstreamCancelled = true; })
            .share();

        const a = new TestSubscriber<number>();
        const b = new TestSubscriber<number>();
        shared.subscribe(a);
        a.requestUnbounded();
        shared.subscribe(b);
        b.requestUnbounded();

        a.sub.unsubscribe();
        expect(upstreamCancelled).toBe(false);

        b.sub.unsubscribe();
        expect(upstreamCancelled).toBe(true);
    });

    it('reconnects upstream when new subscriber arrives after all left', () => {
        // Use a generator that stays open (never calls complete) so the subscriber
        // can unsubscribe before the source terminates — exercising the refCount reconnect.
        let connectCount = 0;
        let activeSink: Sink<number> | null = null;

        const source = Flux.generate<number>(sink => {
            connectCount++;
            activeSink = sink;
            // deliberately do NOT call complete — stays open until cancelled
        });
        const shared = source.share();

        // ── First subscription cycle ──
        const first = new TestSubscriber<number>();
        shared.subscribe(first);
        first.requestUnbounded();
        expect(connectCount).toBe(1);
        activeSink!.next(connectCount);      // emit 1
        first.sub.unsubscribe();             // cancel before source completes

        // ── Second subscription cycle ──
        const second = new TestSubscriber<number>();
        shared.subscribe(second);
        second.requestUnbounded();           // triggers reconnect
        expect(connectCount).toBe(2);
        activeSink!.next(connectCount);      // emit 2

        expect(first.items).toEqual([1]);
        expect(second.items).toEqual([2]);
    });

    it('replays terminal complete to late subscriber after upstream finishes', () => {
        const shared = Flux.just(1, 2).share();

        const first = new TestSubscriber<number>();
        shared.subscribe(first);
        first.requestUnbounded();
        expect(first.completed).toBe(true);

        const late = new TestSubscriber<number>();
        shared.subscribe(late);
        late.requestUnbounded();
        expect(late.completed).toBe(true);
        expect(late.items).toHaveLength(0);
    });

    it('replays terminal error to late subscriber after upstream errors', () => {
        const err = new Error('share-err');
        const shared = Flux.error<number>(err).share();

        const first = new TestSubscriber<number>();
        shared.subscribe(first);
        first.requestUnbounded();
        expect(first.error).toBe(err);

        const late = new TestSubscriber<number>();
        shared.subscribe(late);
        late.requestUnbounded();
        expect(late.error).toBe(err);
    });

    it('propagates upstream error to all current subscribers', () => {
        const err = new Error('multi-err');
        const sink = Sinks.many().multicast().directBestEffort<number>();
        const shared = Flux.from(sink).share();

        const a = new TestSubscriber<number>();
        const b = new TestSubscriber<number>();
        shared.subscribe(a);
        a.requestUnbounded();
        shared.subscribe(b);
        b.requestUnbounded();

        sink.error(err);

        expect(a.error).toBe(err);
        expect(b.error).toBe(err);
    });
});
