import {Flux, Mono, Publisher, Subscriber, Subscription} from '@/.';

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

class TestSubscriber<T> implements Subscriber<T> {
    readonly values: T[] = [];
    readonly errors: Error[] = [];
    completed = false;
    private sub: Subscription | null = null;

    onSubscribe(sub: Subscription): void { this.sub = sub; }
    onNext(v: T): void { this.values.push(v); }
    onError(e: Error): void { this.errors.push(e); }
    onComplete(): void { this.completed = true; }

    subscribeTo(publisher: { subscribe(s: Subscriber<T>): Subscription }, demand = Number.MAX_SAFE_INTEGER): this {
        this.sub = publisher.subscribe(this);
        if (demand > 0) this.sub.request(demand);
        return this;
    }

    request(n: number): void { this.sub?.request(n); }
    cancel(): void { this.sub?.unsubscribe(); }
}

// ---------------------------------------------------------------------------
// Mono.just
// ---------------------------------------------------------------------------

describe('Mono.just', () => {
    test('emits value then completes', () => {
        const sub = new TestSubscriber<number>().subscribeTo(Mono.just(42));
        expect(sub.values).toEqual([42]);
        expect(sub.completed).toBe(true);
    });

    test('does not emit without demand', () => {
        const sub = new TestSubscriber<number>().subscribeTo(Mono.just(1), 0);
        expect(sub.values).toHaveLength(0);
        sub.request(1);
        expect(sub.values).toEqual([1]);
    });

    test('value is delivered only once even if requested more than 1', () => {
        const sub = new TestSubscriber<number>().subscribeTo(Mono.just(5), 10);
        expect(sub.values).toEqual([5]);
        expect(sub.completed).toBe(true);
    });
});

// ---------------------------------------------------------------------------
// Mono.justOrEmpty
// ---------------------------------------------------------------------------

describe('Mono.justOrEmpty', () => {
    test('emits value when present', () => {
        const sub = new TestSubscriber<number>().subscribeTo(Mono.justOrEmpty(7));
        expect(sub.values).toEqual([7]);
    });

    test('completes empty for null', () => {
        const sub = new TestSubscriber<number>().subscribeTo(Mono.justOrEmpty<number>(null));
        expect(sub.values).toHaveLength(0);
        expect(sub.completed).toBe(true);
    });

    test('completes empty for undefined', () => {
        const sub = new TestSubscriber<number>().subscribeTo(Mono.justOrEmpty<number>(undefined));
        expect(sub.values).toHaveLength(0);
        expect(sub.completed).toBe(true);
    });
});

// ---------------------------------------------------------------------------
// Mono.empty
// ---------------------------------------------------------------------------

describe('Mono.empty', () => {
    test('completes immediately without emitting', () => {
        const sub = new TestSubscriber<number>().subscribeTo(Mono.empty());
        expect(sub.values).toHaveLength(0);
        expect(sub.completed).toBe(true);
    });
});

// ---------------------------------------------------------------------------
// Mono.error
// ---------------------------------------------------------------------------

describe('Mono.error', () => {
    test('signals error immediately', () => {
        const err = new Error('boom');
        const sub = new TestSubscriber<number>().subscribeTo(Mono.error(err));
        expect(sub.errors).toEqual([err]);
        expect(sub.completed).toBe(false);
    });

    test('wraps non-Error in Error', () => {
        const sub = new TestSubscriber<number>().subscribeTo(Mono.error('oops'));
        expect(sub.errors[0]).toBeInstanceOf(Error);
        expect(sub.errors[0].message).toBe('oops');
    });
});

// ---------------------------------------------------------------------------
// Mono.generate
// ---------------------------------------------------------------------------

describe('Mono.generate', () => {
    test('sink.next emits value and auto-completes the Mono', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.generate<number>(sink => sink.next(10))
        );
        expect(sub.values).toEqual([10]);
        expect(sub.completed).toBe(true);
    });

    test('sink.complete emits nothing and completes', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.generate<number>(sink => sink.complete())
        );
        expect(sub.values).toHaveLength(0);
        expect(sub.completed).toBe(true);
    });

    test('sink.error terminates with error', () => {
        const err = new Error('gen-err');
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.generate<number>(sink => sink.error(err))
        );
        expect(sub.errors).toEqual([err]);
    });

    test('second sink.next is ignored', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.generate<number>(sink => { sink.next(1); sink.next(2); })
        );
        expect(sub.values).toEqual([1]);
    });

    test('defers delivery until request()', () => {
        const mono = Mono.generate<number>(sink => sink.next(99));
        const sub = new TestSubscriber<number>().subscribeTo(mono, 0);
        expect(sub.values).toHaveLength(0);
        sub.request(1);
        expect(sub.values).toEqual([99]);
    });

    test('thrown exception in generator signals onError', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.generate<number>(_sink => { throw new Error('thrown'); })
        );
        expect(sub.errors[0].message).toBe('thrown');
    });
});

// ---------------------------------------------------------------------------
// Mono.fromPromise
// ---------------------------------------------------------------------------

describe('Mono.fromPromise', () => {
    test('resolved promise emits value', async () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.fromPromise(Promise.resolve(42))
        );
        await Promise.resolve(); // flush microtasks
        expect(sub.values).toEqual([42]);
        expect(sub.completed).toBe(true);
    });

    test('rejected promise signals onError', async () => {
        const err = new Error('rejected');
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.fromPromise(Promise.reject(err))
        );
        await Promise.resolve();
        await Promise.resolve(); // second flush: .then(x).catch(handler) needs 2 microtask ticks
        expect(sub.errors).toEqual([err]);
    });
});

// ---------------------------------------------------------------------------
// Mono.defer
// ---------------------------------------------------------------------------

describe('Mono.defer', () => {
    test('factory is called on each subscribe', () => {
        let calls = 0;
        const mono = Mono.defer(() => { calls++; return Mono.just(calls); });
        new TestSubscriber<number>().subscribeTo(mono);
        new TestSubscriber<number>().subscribeTo(mono);
        expect(calls).toBe(2);
    });

    test('deferred mono emits correct value', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.defer(() => Mono.just(7))
        );
        expect(sub.values).toEqual([7]);
    });
});

// ---------------------------------------------------------------------------
// Mono.from
// ---------------------------------------------------------------------------

describe('Mono.from', () => {
    test('wraps an existing publisher', () => {
        const publisher = Mono.just(3);
        const sub = new TestSubscriber<number>().subscribeTo(Mono.from(publisher));
        expect(sub.values).toEqual([3]);
    });
});

// ---------------------------------------------------------------------------
// map
// ---------------------------------------------------------------------------

describe('Mono.map', () => {
    test('transforms the value', () => {
        const sub = new TestSubscriber<string>().subscribeTo(
            Mono.just(5).map(v => `value:${v}`)
        );
        expect(sub.values).toEqual(['value:5']);
    });

    test('propagates errors from the source', () => {
        const err = new Error('src-err');
        const sub = new TestSubscriber<string>().subscribeTo(
            Mono.error<number>(err).map(v => String(v))
        );
        expect(sub.errors).toEqual([err]);
    });

    test('catches mapper exceptions and signals onError', () => {
        const sub = new TestSubscriber<string>().subscribeTo(
            Mono.just(1).map(_v => { throw new Error('mapper-err'); })
        );
        expect(sub.errors[0].message).toBe('mapper-err');
    });

    test('chained maps compose correctly', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just(2).map(v => v * 3).map(v => v + 1)
        );
        expect(sub.values).toEqual([7]);
    });
});

// ---------------------------------------------------------------------------
// mapNotNull
// ---------------------------------------------------------------------------

describe('Mono.mapNotNull', () => {
    test('emits mapped value when non-null', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just('hello').mapNotNull(v => v.length)
        );
        expect(sub.values).toEqual([5]);
    });

    test('completes empty when mapper returns null', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just('x').mapNotNull(() => null)
        );
        expect(sub.values).toHaveLength(0);
        expect(sub.completed).toBe(true);
    });

    test('completes empty when mapper returns undefined', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just('x').mapNotNull(() => undefined)
        );
        expect(sub.values).toHaveLength(0);
        expect(sub.completed).toBe(true);
    });
});

// ---------------------------------------------------------------------------
// flatMap
// ---------------------------------------------------------------------------

describe('Mono.flatMap', () => {
    test('maps value to inner Mono and flattens', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just(3).flatMap(v => Mono.just(v * 2))
        );
        expect(sub.values).toEqual([6]);
        expect(sub.completed).toBe(true);
    });

    test('empty source results in empty mono', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.empty<number>().flatMap(v => Mono.just(v))
        );
        expect(sub.values).toHaveLength(0);
        expect(sub.completed).toBe(true);
    });

    test('inner error propagates', () => {
        const err = new Error('inner-err');
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just(1).flatMap(() => Mono.error(err))
        );
        expect(sub.errors).toEqual([err]);
    });

    test('outer error propagates', () => {
        const err = new Error('outer-err');
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.error<number>(err).flatMap(v => Mono.just(v))
        );
        expect(sub.errors).toEqual([err]);
    });

    test('mapper exception signals onError', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just(1).flatMap(() => { throw new Error('mapper-throw'); })
        );
        expect(sub.errors[0].message).toBe('mapper-throw');
    });
});

// ---------------------------------------------------------------------------
// filter
// ---------------------------------------------------------------------------

describe('Mono.filter', () => {
    test('passes value when predicate true', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just(5).filter(v => v > 3)
        );
        expect(sub.values).toEqual([5]);
        expect(sub.completed).toBe(true);
    });

    test('becomes empty when predicate false', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just(1).filter(v => v > 3)
        );
        expect(sub.values).toHaveLength(0);
        expect(sub.completed).toBe(true);
    });
});

// ---------------------------------------------------------------------------
// filterWhen
// ---------------------------------------------------------------------------

describe('Mono.filterWhen', () => {
    test('passes value when predicate publisher emits true', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just(5).filterWhen(() => Mono.just(true))
        );
        expect(sub.values).toEqual([5]);
    });

    test('becomes empty when predicate publisher emits false', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just(5).filterWhen(() => Mono.just(false))
        );
        expect(sub.values).toHaveLength(0);
        expect(sub.completed).toBe(true);
    });
});

// ---------------------------------------------------------------------------
// cast
// ---------------------------------------------------------------------------

describe('Mono.cast', () => {
    test('reinterprets type without transformation', () => {
        const sub = new TestSubscriber<unknown>().subscribeTo(
            Mono.just<unknown>(42).cast<number>().map(v => v + 1)
        );
        expect(sub.values).toEqual([43]);
    });
});

// ---------------------------------------------------------------------------
// switchIfEmpty
// ---------------------------------------------------------------------------

describe('Mono.switchIfEmpty', () => {
    test('subscribes to alternative when source is empty', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.empty<number>().switchIfEmpty(Mono.just(99))
        );
        expect(sub.values).toEqual([99]);
    });

    test('uses source value when source is not empty', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just(1).switchIfEmpty(Mono.just(99))
        );
        expect(sub.values).toEqual([1]);
    });
});

// ---------------------------------------------------------------------------
// onErrorReturn
// ---------------------------------------------------------------------------

describe('Mono.onErrorReturn', () => {
    test('switches to replacement on error', () => {
        const err = new Error('oops');
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.error<number>(err).onErrorReturn(Mono.just(0))
        );
        expect(sub.values).toEqual([0]);
        expect(sub.errors).toHaveLength(0);
    });

    test('does not activate replacement on success', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just(7).onErrorReturn(Mono.just(0))
        );
        expect(sub.values).toEqual([7]);
    });
});

// ---------------------------------------------------------------------------
// onErrorContinue
// ---------------------------------------------------------------------------

describe('Mono.onErrorContinue', () => {
    test('swallows error and completes empty when predicate returns true', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.error<number>(new Error('handled')).onErrorContinue(() => true)
        );
        expect(sub.errors).toHaveLength(0);
        expect(sub.completed).toBe(true);
    });

    test('re-signals error when predicate returns false', () => {
        const err = new Error('unhandled');
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.error<number>(err).onErrorContinue(() => false)
        );
        expect(sub.errors).toEqual([err]);
    });
});

// ---------------------------------------------------------------------------
// doOnNext / doOnError / doFinally / doFirst / doOnSubscribe
// ---------------------------------------------------------------------------

describe('Mono side-effect operators', () => {
    test('doOnNext runs side-effect without affecting value', () => {
        const seen: number[] = [];
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just(42).doOnNext(v => seen.push(v))
        );
        expect(seen).toEqual([42]);
        expect(sub.values).toEqual([42]);
    });

    test('doOnError runs side-effect on error', () => {
        const seen: Error[] = [];
        const err = new Error('err');
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.error<number>(err).doOnError(e => seen.push(e))
        );
        expect(seen).toEqual([err]);
        expect(sub.errors).toEqual([err]);
    });

    test('doFinally runs on complete', () => {
        let ran = false;
        new TestSubscriber<number>().subscribeTo(
            Mono.just(1).doFinally(() => { ran = true; })
        );
        expect(ran).toBe(true);
    });

    test('doFinally runs on error', () => {
        let ran = false;
        new TestSubscriber<number>().subscribeTo(
            Mono.error(new Error('e')).doFinally(() => { ran = true; })
        );
        expect(ran).toBe(true);
    });

    test('doFirst runs before first value is forwarded', () => {
        const order: string[] = [];
        new TestSubscriber<number>().subscribeTo(
            Mono.just(1).doFirst(() => order.push('first')).doOnNext(() => order.push('next'))
        );
        expect(order).toEqual(['first', 'next']);
    });

    test('doOnSubscribe receives the subscription object', () => {
        let received: Subscription | null = null;
        new TestSubscriber<number>().subscribeTo(
            Mono.just(1).doOnSubscribe(sub => { received = sub; })
        );
        expect(received).not.toBeNull();
    });
});

// ---------------------------------------------------------------------------
// publishOn / subscribeOn
// ---------------------------------------------------------------------------

describe('Mono.publishOn', () => {
    test('delivers signals via the provided scheduler', done => {
        const order: string[] = [];
        Mono.just(1)
            .publishOn({ schedule: task => setTimeout(task, 0) })
            .subscribe({
                onSubscribe(_s) {},
                onNext(v) { order.push(`next:${v}`); },
                onError() {},
                onComplete() {
                    order.push('complete');
                    expect(order).toEqual(['next:1', 'complete']);
                    done();
                }
            })
            .request(1);
        expect(order).toHaveLength(0); // nothing synchronously
    });
});

describe('Mono.subscribeOn', () => {
    test('performs subscription on the provided scheduler', done => {
        let subscribeTime = 0;
        Mono.generate<number>(sink => {
            subscribeTime = Date.now();
            sink.next(1);
        })
            .subscribeOn({ schedule: task => setTimeout(task, 10) })
            .subscribe({
                onSubscribe(_s) {},
                onNext(_v) {},
                onError() {},
                onComplete() {
                    expect(subscribeTime).toBeGreaterThan(0);
                    done();
                }
            })
            .request(1);
        expect(subscribeTime).toBe(0); // not yet subscribed synchronously
    });
});

// ---------------------------------------------------------------------------
// flatMapMany
// ---------------------------------------------------------------------------

describe('Mono.flatMapMany', () => {
    test('maps single value to a Flux', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just(3).flatMapMany(v => Flux.range(1, v))
        );
        expect(sub.values).toEqual([1, 2, 3]);
        expect(sub.completed).toBe(true);
    });

    test('empty source produces empty Flux', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.empty<number>().flatMapMany(v => Flux.range(1, v))
        );
        expect(sub.values).toHaveLength(0);
        expect(sub.completed).toBe(true);
    });

    test('inner error propagates to subscriber', () => {
        const err = new Error('inner');
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just(1).flatMapMany(() => Mono.error<number>(err))
        );
        expect(sub.errors).toEqual([err]);
    });
});

// ---------------------------------------------------------------------------
// zipWith
// ---------------------------------------------------------------------------

describe('Mono.zipWith', () => {
    test('combines two Monos into a tuple', () => {
        const sub = new TestSubscriber<[number, string]>().subscribeTo(
            Mono.just(1).zipWith(Mono.just('a'))
        );
        expect(sub.values).toEqual([[1, 'a']]);
        expect(sub.completed).toBe(true);
    });

    test('empty left source yields empty result', () => {
        const sub = new TestSubscriber<[number, string]>().subscribeTo(
            Mono.empty<number>().zipWith(Mono.just('a'))
        );
        expect(sub.values).toHaveLength(0);
        expect(sub.completed).toBe(true);
    });

    test('empty right source yields empty result', () => {
        const sub = new TestSubscriber<[number, string]>().subscribeTo(
            Mono.just(1).zipWith(Mono.empty<string>())
        );
        expect(sub.values).toHaveLength(0);
        expect(sub.completed).toBe(true);
    });

    test('error in left source propagates', () => {
        const err = new Error('left-err');
        const sub = new TestSubscriber<[number, string]>().subscribeTo(
            Mono.error<number>(err).zipWith(Mono.just('a'))
        );
        expect(sub.errors).toEqual([err]);
    });
});

// ---------------------------------------------------------------------------
// zipWhen
// ---------------------------------------------------------------------------

describe('Mono.zipWhen', () => {
    test('combines source value with derived Mono', () => {
        const sub = new TestSubscriber<[number, number]>().subscribeTo(
            Mono.just(5).zipWhen(v => Mono.just(v * 2))
        );
        expect(sub.values).toEqual([[5, 10]]);
    });

    test('empty source yields empty result', () => {
        const sub = new TestSubscriber<[number, number]>().subscribeTo(
            Mono.empty<number>().zipWhen(v => Mono.just(v * 2))
        );
        expect(sub.values).toHaveLength(0);
        expect(sub.completed).toBe(true);
    });

    test('error in derived Mono propagates', () => {
        const err = new Error('zip-when-err');
        const sub = new TestSubscriber<[number, number]>().subscribeTo(
            Mono.just(1).zipWhen(() => Mono.error<number>(err))
        );
        expect(sub.errors).toEqual([err]);
    });
});

// ---------------------------------------------------------------------------
// hasElement
// ---------------------------------------------------------------------------

describe('Mono.hasElement', () => {
    test('returns true when source emits a value', () => {
        const sub = new TestSubscriber<boolean>().subscribeTo(
            Mono.just(42).hasElement()
        );
        expect(sub.values).toEqual([true]);
    });

    test('returns false when source is empty', () => {
        const sub = new TestSubscriber<boolean>().subscribeTo(
            Mono.empty().hasElement()
        );
        expect(sub.values).toEqual([false]);
    });

    test('propagates errors', () => {
        const err = new Error('has-err');
        const sub = new TestSubscriber<boolean>().subscribeTo(
            Mono.error(err).hasElement()
        );
        expect(sub.errors).toEqual([err]);
    });
});

// ---------------------------------------------------------------------------
// toPromise
// ---------------------------------------------------------------------------

describe('Mono.toPromise', () => {
    test('resolves to value', async () => {
        expect(await Mono.just(42).toPromise()).toBe(42);
    });

    test('resolves to null for empty Mono', async () => {
        expect(await Mono.empty().toPromise()).toBeNull();
    });

    test('rejects on error', async () => {
        const err = new Error('promise-err');
        await expect(Mono.error(err).toPromise()).rejects.toThrow('promise-err');
    });
});

// ---------------------------------------------------------------------------
// Operator chaining
// ---------------------------------------------------------------------------

describe('Mono operator chaining', () => {
    test('map → filter → flatMap → hasElement', () => {
        const sub = new TestSubscriber<boolean>().subscribeTo(
            Mono.just(10)
                .map(v => v * 2)
                .filter(v => v > 15)
                .flatMap(v => Mono.just(v - 5))
                .hasElement()
        );
        expect(sub.values).toEqual([true]);
    });

    test('map → filter (false) → hasElement returns false', () => {
        const sub = new TestSubscriber<boolean>().subscribeTo(
            Mono.just(1).map(v => v * 2).filter(v => v > 100).hasElement()
        );
        expect(sub.values).toEqual([false]);
    });

    test('switchIfEmpty after filter', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just(1).filter(v => v > 100).switchIfEmpty(Mono.just(-1))
        );
        expect(sub.values).toEqual([-1]);
    });

    test('doOnNext → map → doFinally', () => {
        const events: string[] = [];
        new TestSubscriber<number>().subscribeTo(
            Mono.just(3)
                .doOnNext(() => events.push('peek'))
                .map(v => v * 2)
                .doFinally(() => events.push('finally'))
        );
        expect(events).toEqual(['peek', 'finally']);
    });

    test('toPromise on a chain', async () => {
        const result = await Mono.just(4)
            .map(v => v + 1)
            .filter(v => v > 0)
            .toPromise();
        expect(result).toBe(5);
    });
});

// ---------------------------------------------------------------------------
// subscribe() convenience overloads
// ---------------------------------------------------------------------------

describe('Mono subscribe convenience overloads', () => {
    test('subscribe() with no args — completes silently without throwing', () => {
        expect(() => Mono.just(42).subscribe()).not.toThrow();
    });

    test('subscribe() on empty Mono — completes silently', () => {
        expect(() => Mono.empty<number>().subscribe()).not.toThrow();
    });

    test('subscribe(onNext) — receives the value, auto-requests 1', () => {
        const received: number[] = [];
        Mono.just(42).subscribe(v => received.push(v));
        expect(received).toEqual([42]);
    });

    test('subscribe(onNext) — Mono emits at most 1 value', () => {
        const received: number[] = [];
        Mono.just(99).subscribe(v => received.push(v));
        expect(received).toHaveLength(1);
    });

    test('subscribe(onNext) — empty Mono calls no onNext', () => {
        const received: number[] = [];
        Mono.empty<number>().subscribe(v => received.push(v));
        expect(received).toHaveLength(0);
    });

    test('subscribe(onNext, onError) — onError fires on failure', () => {
        const errors: Error[] = [];
        Mono.error<number>(new Error('boom')).subscribe(
            () => {},
            e => errors.push(e),
        );
        expect(errors).toHaveLength(1);
        expect(errors[0].message).toBe('boom');
    });

    test('subscribe(onNext, onError, onComplete) — all three callbacks fire', () => {
        const events: string[] = [];
        Mono.just(7).subscribe(
            v => events.push(`next:${v}`),
            _e => events.push('error'),
            () => events.push('complete'),
        );
        expect(events).toEqual(['next:7', 'complete']);
    });

    test('subscribe(onNext, onError, onComplete) — empty Mono fires only onComplete', () => {
        const events: string[] = [];
        Mono.empty<number>().subscribe(
            () => events.push('next'),
            () => events.push('error'),
            () => events.push('complete'),
        );
        expect(events).toEqual(['complete']);
    });

    test('subscribe(onNext, onError, onComplete) — error Mono fires only onError', () => {
        const events: string[] = [];
        Mono.error<number>(new Error('x')).subscribe(
            () => events.push('next'),
            _e => events.push('error'),
            () => events.push('complete'),
        );
        expect(events).toEqual(['error']);
    });

    test('subscribe(onNext) with no onError — unhandled error is re-thrown', () => {
        expect(() =>
            Mono.error<number>(new Error('unhandled')).subscribe(() => {}),
        ).toThrow('unhandled');
    });

    test('full Subscriber object — caller controls demand manually', () => {
        const received: number[] = [];
        let completed = false;
        const sub = Mono.just(5).subscribe({
            onSubscribe(s) { /* no auto-request */ },
            onNext(v) { received.push(v); },
            onError() {},
            onComplete() { completed = true; },
        });
        expect(received).toHaveLength(0); // no demand yet
        sub.request(1);
        expect(received).toEqual([5]);
        expect(completed).toBe(true);
    });
});

// ---------------------------------------------------------------------------
// flatMap with schedulers
// ---------------------------------------------------------------------------

describe('Mono.flatMap with schedulers', () => {
    const asyncScheduler = { schedule: (fn: () => void) => setTimeout(fn, 0) };

    test('flatMap: inner Mono.publishOn delivers value asynchronously', async () => {
        const result = await new Promise<number>(resolve => {
            Mono.just(3).flatMap(v =>
                Mono.just(v * 10).publishOn(asyncScheduler),
            ).subscribe(resolve);
        });
        expect(result).toBe(30);
    });

    test('flatMap: source subscribed on scheduler, inner runs synchronously', async () => {
        const events: string[] = [];
        await new Promise<void>(resolve => {
            Mono.just(2)
                .subscribeOn(asyncScheduler)
                .flatMap(v => Mono.just(v * 5))
                .subscribe(
                    v => events.push(`next:${v}`),
                    _e => {},
                    () => { events.push('complete'); resolve(); },
                );
        });
        expect(events).toEqual(['next:10', 'complete']);
    });

    test('flatMap: inner publishOn + outer subscribeOn both async', async () => {
        const received: number[] = [];
        await new Promise<void>(resolve => {
            Mono.just(4)
                .subscribeOn(asyncScheduler)
                .flatMap(v => Mono.just(v + 1).publishOn(asyncScheduler))
                .subscribe(
                    v => received.push(v),
                    _e => {},
                    resolve,
                );
        });
        expect(received).toEqual([5]);
    });

    test('flatMapMany: expands Mono to Flux via inner publishOn scheduler', async () => {
        const received: number[] = [];
        await new Promise<void>(resolve => {
            Mono.just(3).flatMapMany(n =>
                Flux.range(1, n).publishOn(asyncScheduler),
            ).subscribe(
                v => received.push(v),
                _e => {},
                resolve,
            );
        });
        expect(received).toEqual([1, 2, 3]);
    });

    test('flatMapMany: subscribeOn delays source, inner is sync', async () => {
        const received: number[] = [];
        await new Promise<void>(resolve => {
            Mono.just(3)
                .subscribeOn(asyncScheduler)
                .flatMapMany(n => Flux.range(0, n))
                .subscribe(
                    v => received.push(v),
                    _e => {},
                    resolve,
                );
        });
        expect(received).toEqual([0, 1, 2]);
    });
});

// ---------------------------------------------------------------------------
// Mono.create
// ---------------------------------------------------------------------------

describe('Mono.create', () => {
    test('is an alias for Mono.generate — emits value', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.create<number>(sink => sink.next(7))
        );
        expect(sub.values).toEqual([7]);
        expect(sub.completed).toBe(true);
    });

    test('is an alias for Mono.generate — completes empty', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.create<number>(sink => sink.complete())
        );
        expect(sub.values).toHaveLength(0);
        expect(sub.completed).toBe(true);
    });
});

// ---------------------------------------------------------------------------
// Mono.delay
// ---------------------------------------------------------------------------

describe('Mono.delay', () => {
    test('does not emit synchronously', () => {
        const sub = new TestSubscriber<number>().subscribeTo(Mono.delay(50), 1);
        expect(sub.values).toHaveLength(0);
    });

    test('emits 0 after the given delay', async () => {
        const result = await Mono.delay(50).toPromise();
        expect(result).toBe(0);
    });
});

// ---------------------------------------------------------------------------
// Mono.fromCallable
// ---------------------------------------------------------------------------

describe('Mono.fromCallable', () => {
    test('calls factory at subscription time and emits result', () => {
        let calls = 0;
        const mono = Mono.fromCallable(() => { calls++; return 42; });
        expect(calls).toBe(0);
        const sub = new TestSubscriber<number>().subscribeTo(mono);
        expect(calls).toBe(1);
        expect(sub.values).toEqual([42]);
    });

    test('wraps thrown exceptions as onError', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.fromCallable<number>(() => { throw new Error('sync-throw'); })
        );
        expect(sub.errors[0].message).toBe('sync-throw');
    });

    test('each subscription calls factory independently', () => {
        let calls = 0;
        const mono = Mono.fromCallable(() => ++calls);
        new TestSubscriber<number>().subscribeTo(mono);
        new TestSubscriber<number>().subscribeTo(mono);
        expect(calls).toBe(2);
    });
});

// ---------------------------------------------------------------------------
// Mono.firstWithValue
// ---------------------------------------------------------------------------

describe('Mono.firstWithValue', () => {
    test('emits value from first resolving Mono', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.firstWithValue(Mono.just(1), Mono.just(2))
        );
        expect(sub.values[0]).toBe(1);
    });

    test('skips empty Mono and takes value from next', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.firstWithValue(Mono.empty<number>(), Mono.just(42))
        );
        expect(sub.values).toEqual([42]);
    });

    test('completes empty when all sources are empty', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.firstWithValue(Mono.empty<number>(), Mono.empty<number>())
        );
        expect(sub.values).toHaveLength(0);
        expect(sub.completed).toBe(true);
    });

    test('propagates error from first erroring source', () => {
        const err = new Error('race-err');
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.firstWithValue(Mono.error<number>(err), Mono.just(5))
        );
        expect(sub.errors).toEqual([err]);
    });

    test('returns empty Mono for zero sources', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.firstWithValue<number>()
        );
        expect(sub.values).toHaveLength(0);
        expect(sub.completed).toBe(true);
    });
});

// ---------------------------------------------------------------------------
// Mono#doOnSuccess
// ---------------------------------------------------------------------------

describe('Mono#doOnSuccess', () => {
    test('side effect fires with emitted value', () => {
        const seen: number[] = [];
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just(42).doOnSuccess(v => seen.push(v))
        );
        expect(seen).toEqual([42]);
        expect(sub.values).toEqual([42]);
    });

    test('does not fire when Mono is empty', () => {
        const seen: number[] = [];
        new TestSubscriber<number>().subscribeTo(
            Mono.empty<number>().doOnSuccess(v => seen.push(v))
        );
        expect(seen).toHaveLength(0);
    });

    test('exception in fn propagates as onError', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just(1).doOnSuccess(() => { throw new Error('fn-throw'); })
        );
        expect(sub.errors[0].message).toBe('fn-throw');
        expect(sub.values).toHaveLength(0);
    });
});

// ---------------------------------------------------------------------------
// Mono#onErrorComplete
// ---------------------------------------------------------------------------

describe('Mono#onErrorComplete', () => {
    test('converts error to completion (no predicate)', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.error<number>(new Error('x')).onErrorComplete()
        );
        expect(sub.errors).toHaveLength(0);
        expect(sub.completed).toBe(true);
    });

    test('predicate true — converts to completion', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.error<number>(new Error('match')).onErrorComplete(e => e.message === 'match')
        );
        expect(sub.completed).toBe(true);
        expect(sub.errors).toHaveLength(0);
    });

    test('predicate false — re-propagates error', () => {
        const err = new Error('no-match');
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.error<number>(err).onErrorComplete(e => e.message === 'other')
        );
        expect(sub.errors).toEqual([err]);
        expect(sub.completed).toBe(false);
    });

    test('does not affect normal emission', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just(5).onErrorComplete()
        );
        expect(sub.values).toEqual([5]);
        expect(sub.completed).toBe(true);
    });
});

// ---------------------------------------------------------------------------
// Mono#thenReturn
// ---------------------------------------------------------------------------

describe('Mono#thenReturn', () => {
    test('replaces emitted value with the given value', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just('ignored').thenReturn(99)
        );
        expect(sub.values).toEqual([99]);
        expect(sub.completed).toBe(true);
    });

    test('works when source is empty — still emits the value', () => {
        const sub = new TestSubscriber<string>().subscribeTo(
            Mono.empty<void>().thenReturn('hello')
        );
        expect(sub.values).toEqual(['hello']);
    });

    test('propagates error without emitting the value', () => {
        const err = new Error('src-err');
        const sub = new TestSubscriber<string>().subscribeTo(
            Mono.error<void>(err).thenReturn('x')
        );
        expect(sub.errors).toEqual([err]);
        expect(sub.values).toHaveLength(0);
    });
});

// ---------------------------------------------------------------------------
// Mono#delayElement
// ---------------------------------------------------------------------------

describe('Mono#delayElement', () => {
    test('delays the emitted value', async () => {
        const start = Date.now();
        const result = await Mono.just('hi').delayElement(80).toPromise();
        expect(result).toBe('hi');
        expect(Date.now() - start).toBeGreaterThanOrEqual(60);
    });

    test('does not emit synchronously', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just(1).delayElement(200), 1
        );
        expect(sub.values).toHaveLength(0);
    });

    test('propagates error without delay', () => {
        const err = new Error('early-err');
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.error<number>(err).delayElement(200)
        );
        expect(sub.errors).toEqual([err]);
    });
});

// ---------------------------------------------------------------------------
// Mono#delayUntil
// ---------------------------------------------------------------------------

describe('Mono#delayUntil', () => {
    test('holds value until trigger emits', async () => {
        const result = await Mono.just(7)
            .delayUntil(() => Mono.delay(50))
            .toPromise();
        expect(result).toBe(7);
    });

    test('propagates trigger error and discards the value', () => {
        const err = new Error('trigger-fail');
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just(1).delayUntil(() => Mono.error(err))
        );
        expect(sub.errors).toEqual([err]);
        expect(sub.values).toHaveLength(0);
    });

    test('does not invoke triggerFn when source is empty', () => {
        let called = false;
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.empty<number>().delayUntil(() => { called = true; return Mono.just(0); })
        );
        expect(called).toBe(false);
        expect(sub.completed).toBe(true);
    });
});

// ---------------------------------------------------------------------------
// Mono#or
// ---------------------------------------------------------------------------

describe('Mono#or', () => {
    test('uses primary value when source emits', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just(1).or(Mono.just(99))
        );
        expect(sub.values).toEqual([1]);
    });

    test('falls back to other when source is empty', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.empty<number>().or(Mono.just(99))
        );
        expect(sub.values).toEqual([99]);
        expect(sub.completed).toBe(true);
    });

    test('propagates source error without subscribing to other', () => {
        let otherSubscribed = false;
        const err = new Error('src');
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.error<number>(err).or(Mono.defer(() => { otherSubscribed = true; return Mono.just(0); }))
        );
        expect(sub.errors).toEqual([err]);
        expect(otherSubscribed).toBe(false);
    });

    test('other is empty — result is empty', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.empty<number>().or(Mono.empty<number>())
        );
        expect(sub.values).toHaveLength(0);
        expect(sub.completed).toBe(true);
    });
});

// ---------------------------------------------------------------------------
// Mono#retryWhen
// ---------------------------------------------------------------------------

describe('Mono#retryWhen', () => {
    test('retries and eventually succeeds', () => {
        let attempt = 0;
        const source = Mono.defer(() => {
            attempt++;
            return attempt < 3 ? Mono.error<number>(new Error('fail')) : Mono.just(42);
        });
        const sub = new TestSubscriber<number>().subscribeTo(
            source.retryWhen(errors => errors.take(3))
        );
        expect(sub.values).toEqual([42]);
        expect(attempt).toBe(3);
    });

    test('stops retrying when control stream completes', () => {
        const err = new Error('always-fail');
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.error<number>(err).retryWhen(errors => errors.take(0))
        );
        expect(sub.completed).toBe(true);
        expect(sub.values).toHaveLength(0);
    });
});

// ---------------------------------------------------------------------------
// Mono#toFuture
// ---------------------------------------------------------------------------

describe('Mono#toFuture', () => {
    test('resolves with the emitted value', async () => {
        const result = await Mono.just(123).toFuture();
        expect(result).toBe(123);
    });

    test('resolves with null when Mono is empty', async () => {
        const result = await Mono.empty<number>().toFuture();
        expect(result).toBeNull();
    });

    test('rejects on error', async () => {
        await expect(Mono.error(new Error('rej')).toFuture()).rejects.toThrow('rej');
    });
});

// ---------------------------------------------------------------------------
// Mono.when
// ---------------------------------------------------------------------------

describe('Mono.when', () => {
    test('completes immediately with no sources', () => {
        const sub = new TestSubscriber<void>().subscribeTo(Mono.when());
        expect(sub.completed).toBe(true);
        expect(sub.errors).toHaveLength(0);
    });

    test('completes when single source completes', () => {
        const sub = new TestSubscriber<void>().subscribeTo(
            Mono.when(Mono.just(42))
        );
        expect(sub.completed).toBe(true);
        expect(sub.values).toHaveLength(0);
    });

    test('ignores emitted values from sources', () => {
        const sub = new TestSubscriber<void>().subscribeTo(
            Mono.when(Mono.just(1), Mono.just(2), Mono.just(3))
        );
        expect(sub.values).toHaveLength(0);
        expect(sub.completed).toBe(true);
    });

    test('completes only when all sources complete', () => {
        let completeA: (() => void) | null = null;
        let completeB: (() => void) | null = null;

        const sourceA = Mono.generate<number>(sink => { completeA = () => sink.next(1); });
        const sourceB = Mono.generate<number>(sink => { completeB = () => sink.next(2); });

        const sub = new TestSubscriber<void>().subscribeTo(Mono.when(sourceA, sourceB));
        expect(sub.completed).toBe(false);

        completeA!();
        expect(sub.completed).toBe(false);

        completeB!();
        expect(sub.completed).toBe(true);
    });

    test('propagates error from first failing source and cancels the rest', () => {
        const err = new Error('when-err');
        let completeB: (() => void) | null = null;
        let bCompleted = false;

        const sourceA = Mono.error<number>(err);
        const sourceB = Mono.generate<number>(sink => {
            completeB = () => { sink.next(99); bCompleted = true; };
        });

        const sub = new TestSubscriber<void>().subscribeTo(Mono.when(sourceA, sourceB));
        expect(sub.errors).toEqual([err]);
        expect(sub.completed).toBe(false);

        // completing B after error must be a no-op
        completeB!();
        expect(bCompleted).toBe(true);
        expect(sub.errors).toHaveLength(1);
        expect(sub.completed).toBe(false);
    });

    test('first error wins even if other sources emit later', () => {
        const err = new Error('first-err');
        const sub = new TestSubscriber<void>().subscribeTo(
            Mono.when(Mono.error<number>(err), Mono.just(5))
        );
        expect(sub.errors).toEqual([err]);
        expect(sub.completed).toBe(false);
    });

    test('works with a single empty source', () => {
        const sub = new TestSubscriber<void>().subscribeTo(
            Mono.when(Mono.empty<number>())
        );
        expect(sub.completed).toBe(true);
    });

    test('completes when all sources are empty', () => {
        const sub = new TestSubscriber<void>().subscribeTo(
            Mono.when(Mono.empty<number>(), Mono.empty<string>())
        );
        expect(sub.completed).toBe(true);
        expect(sub.values).toHaveLength(0);
    });
});

// ---------------------------------------------------------------------------
// Mono#then
// ---------------------------------------------------------------------------

describe('Mono#then', () => {
    describe('then() — no argument', () => {
        test('completes when source completes', () => {
            const sub = new TestSubscriber<void>().subscribeTo(
                Mono.just(42).then()
            );
            expect(sub.completed).toBe(true);
            expect(sub.values).toHaveLength(0);
        });

        test('ignores the emitted value', () => {
            const sub = new TestSubscriber<void>().subscribeTo(
                Mono.just('should be ignored').then()
            );
            expect(sub.values).toHaveLength(0);
            expect(sub.completed).toBe(true);
        });

        test('completes when source is empty', () => {
            const sub = new TestSubscriber<void>().subscribeTo(
                Mono.empty<number>().then()
            );
            expect(sub.completed).toBe(true);
        });

        test('propagates source error', () => {
            const err = new Error('then-err');
            const sub = new TestSubscriber<void>().subscribeTo(
                Mono.error<number>(err).then()
            );
            expect(sub.errors).toEqual([err]);
            expect(sub.completed).toBe(false);
        });

        test('returns Mono<void> — does not emit any value', () => {
            const values: void[] = [];
            new TestSubscriber<void>().subscribeTo(
                Mono.just(99).then().doOnSuccess(v => values.push(v))
            );
            expect(values).toHaveLength(0);
        });
    });

    describe('then(other) — with argument', () => {
        test('subscribes to other after source completes and returns other value', () => {
            const sub = new TestSubscriber<number>().subscribeTo(
                Mono.just('ignored').then(Mono.just(42))
            );
            expect(sub.values).toEqual([42]);
            expect(sub.completed).toBe(true);
        });

        test('ignores source value, returns other value', () => {
            const sub = new TestSubscriber<string>().subscribeTo(
                Mono.just(999).then(Mono.just('hello'))
            );
            expect(sub.values).toEqual(['hello']);
        });

        test('source error stops chain — other is never subscribed', () => {
            const err = new Error('chain-err');
            let otherSubscribed = false;
            const other = Mono.defer<number>(() => {
                otherSubscribed = true;
                return Mono.just(1);
            });
            const sub = new TestSubscriber<number>().subscribeTo(
                Mono.error<string>(err).then(other)
            );
            expect(sub.errors).toEqual([err]);
            expect(otherSubscribed).toBe(false);
        });

        test('empty source — other is still subscribed', () => {
            const sub = new TestSubscriber<number>().subscribeTo(
                Mono.empty<string>().then(Mono.just(7))
            );
            expect(sub.values).toEqual([7]);
            expect(sub.completed).toBe(true);
        });

        test('other error is propagated downstream', () => {
            const err = new Error('other-err');
            const sub = new TestSubscriber<number>().subscribeTo(
                Mono.just('ok').then(Mono.error<number>(err))
            );
            expect(sub.errors).toEqual([err]);
            expect(sub.completed).toBe(false);
        });

        test('other empty — completes without value', () => {
            const sub = new TestSubscriber<number>().subscribeTo(
                Mono.just('ok').then(Mono.empty<number>())
            );
            expect(sub.values).toHaveLength(0);
            expect(sub.completed).toBe(true);
        });

        test('sequencing: chain multiple then()', () => {
            const steps: string[] = [];
            const sub = new TestSubscriber<number>().subscribeTo(
                Mono.fromCallable(() => { steps.push('A'); return 1; })
                    .then(Mono.fromCallable(() => { steps.push('B'); return 2; }))
                    .then(Mono.fromCallable(() => { steps.push('C'); return 3; }))
            );
            expect(steps).toEqual(['A', 'B', 'C']);
            expect(sub.values).toEqual([3]);
        });
    });
});

// ---------------------------------------------------------------------------
// Mono#thenMany
// ---------------------------------------------------------------------------

describe('Mono#thenMany', () => {
    test('emits all items from the other publisher after source completes', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just('ignored').thenMany(Flux.just(1, 2, 3))
        );
        expect(sub.values).toEqual([1, 2, 3]);
        expect(sub.completed).toBe(true);
    });

    test('ignores the value emitted by the source', () => {
        const received: string[] = [];
        new TestSubscriber<string>().subscribeTo(
            Mono.just('should be dropped').thenMany(Flux.just('a', 'b'))
        );
        // source value never reaches downstream
        expect(received).toHaveLength(0);
    });

    test('works when source is empty — still subscribes to other', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.empty<void>().thenMany(Flux.just(10, 20))
        );
        expect(sub.values).toEqual([10, 20]);
        expect(sub.completed).toBe(true);
    });

    test('propagates source error without subscribing to other', () => {
        const err = new Error('src-err');
        let otherSubscribed = false;
        const other = Flux.defer(() => { otherSubscribed = true; return Flux.just(1); });
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.error<void>(err).thenMany(other)
        );
        expect(sub.errors).toEqual([err]);
        expect(otherSubscribed).toBe(false);
    });

    test('propagates error from the other publisher', () => {
        const err = new Error('other-err');
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just('x').thenMany(Flux.error<number>(err))
        );
        expect(sub.errors).toEqual([err]);
        expect(sub.completed).toBe(false);
    });

    test('returns Flux<V> — all Flux operators compose correctly', () => {
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.just('trigger')
                .thenMany(Flux.just(1, 2, 3, 4, 5))
                .filter(n => n % 2 === 0)
        );
        expect(sub.values).toEqual([2, 4]);
    });

    test('respects backpressure — honours demand from downstream', () => {
        const received: number[] = [];
        let sub!: Subscription;
        const flux = Mono.just('x').thenMany(Flux.just(1, 2, 3));
        flux.subscribe({
            onSubscribe(s) { sub = s; },
            onNext(v) { received.push(v); },
            onError() {},
            onComplete() {}
        });
        expect(received).toHaveLength(0);
        sub.request(2);
        expect(received).toEqual([1, 2]);
    });

    test('unsubscribe before source completes cancels upstream', () => {
        let cancelled = false;
        const slowMono: Publisher<void> = {
            subscribe(subscriber) {
                const s: Subscription = {
                    request() {},
                    unsubscribe() { cancelled = true; }
                };
                subscriber.onSubscribe(s);
                return s;
            }
        };
        let sub!: Subscription;
        Mono.from(slowMono).thenMany(Flux.just(1)).subscribe({
            onSubscribe(s) { sub = s; },
            onNext() {},
            onError() {},
            onComplete() {}
        });
        sub.request(1);
        sub.unsubscribe();
        expect(cancelled).toBe(true);
    });

    test('unsubscribe after source completes cancels inner subscription', () => {
        let innerCancelled = false;
        const inner = Flux.just(1, 2, 3).doOnCancel(() => { innerCancelled = true; });
        const received: number[] = [];
        let sub!: Subscription;
        Mono.just('x').thenMany(inner).subscribe({
            onSubscribe(s) { sub = s; },
            onNext(v) { received.push(v); },
            onError() {},
            onComplete() {}
        });
        sub.request(1);
        expect(received).toEqual([1]);
        sub.unsubscribe();
        expect(innerCancelled).toBe(true);
    });

    test('works with Mono as other publisher', () => {
        const sub = new TestSubscriber<string>().subscribeTo(
            Mono.just(1).thenMany(Mono.just('hello'))
        );
        expect(sub.values).toEqual(['hello']);
        expect(sub.completed).toBe(true);
    });

    test('chaining: then().thenMany() sequences correctly', () => {
        const steps: string[] = [];
        const sub = new TestSubscriber<number>().subscribeTo(
            Mono.fromCallable(() => { steps.push('A'); return 'a'; })
                .then()
                .thenMany(Flux.defer(() => { steps.push('B'); return Flux.just(1, 2); }))
        );
        expect(steps).toEqual(['A', 'B']);
        expect(sub.values).toEqual([1, 2]);
    });
});
