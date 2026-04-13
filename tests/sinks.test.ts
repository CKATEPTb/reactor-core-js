import {Sinks} from '@/.';
import {Publisher, Subscriber, Subscription} from "@/.";

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

class TestSubscriber<T> implements Subscriber<T> {
    readonly values: T[] = [];
    readonly errors: Error[] = [];
    completed = false;
    private sub: Subscription | null = null;

    onSubscribe(sub: Subscription): void { this.sub = sub; }
    onNext(value: T): void { this.values.push(value); }
    onError(error: Error): void { this.errors.push(error); }
    onComplete(): void { this.completed = true; }

    /** Subscribe and immediately request `demand` items (default: unbounded). */
    subscribeTo(publisher: Publisher<T>, demand = Number.MAX_SAFE_INTEGER): this {
        this.sub = publisher.subscribe(this);
        if (demand > 0) this.sub.request(demand);
        return this;
    }

    request(n: number): void { this.sub?.request(n); }
    cancel(): void { this.sub?.unsubscribe(); }
}

// ---------------------------------------------------------------------------
// Sinks.empty() — EmptySink
// ---------------------------------------------------------------------------

describe('Sinks.empty() / EmptySink', () => {
    test('complete() delivers onComplete to subscribed subscriber', () => {
        const sink = Sinks.empty<number>();
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        sink.complete();
        expect(sub.completed).toBe(true);
        expect(sub.values).toHaveLength(0);
        expect(sub.errors).toHaveLength(0);
    });

    test('error() delivers onError to subscribed subscriber', () => {
        const sink = Sinks.empty<number>();
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        const err = new Error('boom');
        sink.error(err);
        expect(sub.errors).toEqual([err]);
        expect(sub.completed).toBe(false);
    });

    test('next() is ignored — no values delivered', () => {
        const sink = Sinks.empty<number>();
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        sink.next(42);
        sink.complete();
        expect(sub.values).toHaveLength(0);
        expect(sub.completed).toBe(true);
    });

    test('late subscriber gets immediate onComplete after sink already completed', () => {
        const sink = Sinks.empty<number>();
        sink.complete();
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        expect(sub.completed).toBe(true);
    });

    test('late subscriber gets immediate onError after sink errored', () => {
        const sink = Sinks.empty<number>();
        const err = new Error('pre-error');
        sink.error(err);
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        expect(sub.errors).toEqual([err]);
    });

    test('multiple subscribers all receive the terminal signal', () => {
        const sink = Sinks.empty<number>();
        const s1 = new TestSubscriber<number>().subscribeTo(sink);
        const s2 = new TestSubscriber<number>().subscribeTo(sink);
        sink.complete();
        expect(s1.completed).toBe(true);
        expect(s2.completed).toBe(true);
    });

    test('complete() is idempotent — second call is ignored', () => {
        const sink = Sinks.empty<number>();
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        sink.complete();
        sink.complete(); // must not throw or call onComplete twice
        expect(sub.completed).toBe(true);
    });

    test('error() after complete() is ignored', () => {
        const sink = Sinks.empty<number>();
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        sink.complete();
        sink.error(new Error('late'));
        expect(sub.errors).toHaveLength(0);
        expect(sub.completed).toBe(true);
    });

    test('unsubscribe() before complete() prevents delivery', () => {
        const sink = Sinks.empty<number>();
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        sub.cancel();
        sink.complete();
        expect(sub.completed).toBe(false);
    });

});

// ---------------------------------------------------------------------------
// Sinks.one() — OneSink
// ---------------------------------------------------------------------------

describe('Sinks.one() / OneSink', () => {
    test('next(v) + complete() delivers value then onComplete', () => {
        const sink = Sinks.one<number>();
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        sink.next(42);
        sink.complete();
        expect(sub.values).toEqual([42]);
        expect(sub.completed).toBe(true);
    });

    test('complete() without next() delivers only onComplete', () => {
        const sink = Sinks.one<number>();
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        sink.complete();
        expect(sub.values).toHaveLength(0);
        expect(sub.completed).toBe(true);
    });

    test('error() delivers onError', () => {
        const sink = Sinks.one<number>();
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        const err = new Error('fail');
        sink.error(err);
        expect(sub.errors).toEqual([err]);
        expect(sub.completed).toBe(false);
    });

    test('second next() is ignored — only first value is emitted', () => {
        const sink = Sinks.one<number>();
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        sink.next(1);
        sink.next(99); // ignored
        sink.complete();
        expect(sub.values).toEqual([1]);
    });

    test('late subscriber replays value after complete()', () => {
        const sink = Sinks.one<number>();
        sink.next(7);
        sink.complete();
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        expect(sub.values).toEqual([7]);
        expect(sub.completed).toBe(true);
    });

    test('late subscriber gets onComplete without value when completed empty', () => {
        const sink = Sinks.one<number>();
        sink.complete();
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        expect(sub.values).toHaveLength(0);
        expect(sub.completed).toBe(true);
    });

    test('late subscriber gets onError after error()', () => {
        const sink = Sinks.one<number>();
        const err = new Error('late-err');
        sink.error(err);
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        expect(sub.errors).toEqual([err]);
    });

    test('multiple subscribers all receive value + onComplete', () => {
        const sink = Sinks.one<number>();
        const s1 = new TestSubscriber<number>().subscribeTo(sink);
        const s2 = new TestSubscriber<number>().subscribeTo(sink);
        sink.next(5);
        sink.complete();
        expect(s1.values).toEqual([5]);
        expect(s2.values).toEqual([5]);
        expect(s1.completed).toBe(true);
        expect(s2.completed).toBe(true);
    });

    test('complete() is idempotent', () => {
        const sink = Sinks.one<number>();
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        sink.next(1);
        sink.complete();
        sink.complete();
        expect(sub.values).toHaveLength(1);
    });

    test('next() after complete() is ignored', () => {
        const sink = Sinks.one<number>();
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        sink.complete();
        sink.next(99);
        expect(sub.values).toHaveLength(0);
    });

});

// ---------------------------------------------------------------------------
// Sinks.many().unicast().onBackpressureBuffer()
// ---------------------------------------------------------------------------

describe('Sinks.many().unicast().onBackpressureBuffer()', () => {
    test('delivers elements in order with unbounded demand', () => {
        const sink = Sinks.many().unicast().onBackpressureBuffer<number>();
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        sink.next(1); sink.next(2); sink.next(3);
        sink.complete();
        expect(sub.values).toEqual([1, 2, 3]);
        expect(sub.completed).toBe(true);
    });

    test('buffers elements emitted before subscribe', () => {
        const sink = Sinks.many().unicast().onBackpressureBuffer<number>();
        sink.next(1); sink.next(2);
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        expect(sub.values).toEqual([1, 2]);
    });

    test('respects backpressure: delivers only requested count', () => {
        const sink = Sinks.many().unicast().onBackpressureBuffer<number>();
        sink.next(1); sink.next(2); sink.next(3);
        const sub = new TestSubscriber<number>().subscribeTo(sink, 2);
        expect(sub.values).toEqual([1, 2]);
        sub.request(1);
        expect(sub.values).toEqual([1, 2, 3]);
    });

    test('second subscriber receives error', () => {
        const sink = Sinks.many().unicast().onBackpressureBuffer<number>();
        new TestSubscriber<number>().subscribeTo(sink);
        const sub2 = new TestSubscriber<number>().subscribeTo(sink);
        expect(sub2.errors).toHaveLength(1);
        expect(sub2.errors[0].message).toMatch(/one subscriber/i);
    });

    test('complete() flushes buffer then signals onComplete', () => {
        const sink = Sinks.many().unicast().onBackpressureBuffer<number>();
        sink.next(10); sink.next(20);
        sink.complete();
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        expect(sub.values).toEqual([10, 20]);
        expect(sub.completed).toBe(true);
    });

    test('error() flushes buffer then signals onError', () => {
        const sink = Sinks.many().unicast().onBackpressureBuffer<number>();
        sink.next(1);
        const err = new Error('e');
        sink.error(err);
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        expect(sub.values).toEqual([1]);
        expect(sub.errors).toEqual([err]);
    });

    test('cancel() stops delivery and clears buffer', () => {
        const sink = Sinks.many().unicast().onBackpressureBuffer<number>();
        const sub = new TestSubscriber<number>().subscribeTo(sink, 0);
        sink.next(1); sink.next(2);
        sub.cancel();
        sub.request(10);
        expect(sub.values).toHaveLength(0);
    });

});

// ---------------------------------------------------------------------------
// Sinks.many().unicast().onBackpressureError()
// ---------------------------------------------------------------------------

describe('Sinks.many().unicast().onBackpressureError()', () => {
    test('delivers element when subscriber has demand', () => {
        const sink = Sinks.many().unicast().onBackpressureError<number>();
        const sub = new TestSubscriber<number>().subscribeTo(sink, 3);
        sink.next(1); sink.next(2);
        expect(sub.values).toEqual([1, 2]);
    });

    test('errors subscriber when next() is called with no demand', () => {
        const sink = Sinks.many().unicast().onBackpressureError<number>();
        const sub = new TestSubscriber<number>().subscribeTo(sink, 0);
        sink.next(1);
        expect(sub.errors).toHaveLength(1);
        expect(sub.errors[0].message).toMatch(/demand/i);
    });

    test('complete() signals onComplete', () => {
        const sink = Sinks.many().unicast().onBackpressureError<number>();
        const sub = new TestSubscriber<number>().subscribeTo(sink, 1);
        sink.next(1);
        sink.complete();
        expect(sub.completed).toBe(true);
    });

    test('second subscriber gets error', () => {
        const sink = Sinks.many().unicast().onBackpressureError<number>();
        new TestSubscriber<number>().subscribeTo(sink);
        const sub2 = new TestSubscriber<number>().subscribeTo(sink);
        expect(sub2.errors).toHaveLength(1);
        expect(sub2.errors[0].message).toMatch(/one subscriber/i);
    });

    test('already terminated sink replays terminal to late subscriber', () => {
        const sink = Sinks.many().unicast().onBackpressureError<number>();
        sink.complete();
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        expect(sub.completed).toBe(true);
    });

});

// ---------------------------------------------------------------------------
// Sinks.many().multicast().directAllOrNothing()
// ---------------------------------------------------------------------------

describe('Sinks.many().multicast().directAllOrNothing()', () => {
    test('emits to all subscribers when all have demand', () => {
        const sink = Sinks.many().multicast().directAllOrNothing<number>();
        const s1 = new TestSubscriber<number>().subscribeTo(sink, 1);
        const s2 = new TestSubscriber<number>().subscribeTo(sink, 1);
        sink.next(42);
        expect(s1.values).toEqual([42]);
        expect(s2.values).toEqual([42]);
    });

    test('drops emission if any subscriber has no demand', () => {
        const sink = Sinks.many().multicast().directAllOrNothing<number>();
        const s1 = new TestSubscriber<number>().subscribeTo(sink, 1);
        const s2 = new TestSubscriber<number>().subscribeTo(sink, 0); // no demand
        sink.next(42);
        expect(s1.values).toHaveLength(0); // dropped for ALL
        expect(s2.values).toHaveLength(0);
    });

    test('complete() signals all subscribers', () => {
        const sink = Sinks.many().multicast().directAllOrNothing<number>();
        const s1 = new TestSubscriber<number>().subscribeTo(sink);
        const s2 = new TestSubscriber<number>().subscribeTo(sink);
        sink.complete();
        expect(s1.completed).toBe(true);
        expect(s2.completed).toBe(true);
    });

    test('error() signals all subscribers', () => {
        const sink = Sinks.many().multicast().directAllOrNothing<number>();
        const s1 = new TestSubscriber<number>().subscribeTo(sink);
        const s2 = new TestSubscriber<number>().subscribeTo(sink);
        const err = new Error('all fail');
        sink.error(err);
        expect(s1.errors).toEqual([err]);
        expect(s2.errors).toEqual([err]);
    });

    test('no subscribers — emission is silently dropped', () => {
        const sink = Sinks.many().multicast().directAllOrNothing<number>();
        expect(() => sink.next(1)).not.toThrow();
    });

    test('late subscriber gets terminal signal if already completed', () => {
        const sink = Sinks.many().multicast().directAllOrNothing<number>();
        sink.complete();
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        expect(sub.completed).toBe(true);
    });

});

// ---------------------------------------------------------------------------
// Sinks.many().multicast().directBestEffort()
// ---------------------------------------------------------------------------

describe('Sinks.many().multicast().directBestEffort()', () => {
    test('delivers only to subscribers with demand', () => {
        const sink = Sinks.many().multicast().directBestEffort<number>();
        const s1 = new TestSubscriber<number>().subscribeTo(sink, 1);
        const s2 = new TestSubscriber<number>().subscribeTo(sink, 0);
        sink.next(7);
        expect(s1.values).toEqual([7]);
        expect(s2.values).toHaveLength(0);
    });

    test('all receive value when all have demand', () => {
        const sink = Sinks.many().multicast().directBestEffort<number>();
        const s1 = new TestSubscriber<number>().subscribeTo(sink, 5);
        const s2 = new TestSubscriber<number>().subscribeTo(sink, 5);
        sink.next(1); sink.next(2);
        expect(s1.values).toEqual([1, 2]);
        expect(s2.values).toEqual([1, 2]);
    });

    test('complete() reaches all subscribers', () => {
        const sink = Sinks.many().multicast().directBestEffort<number>();
        const s1 = new TestSubscriber<number>().subscribeTo(sink);
        const s2 = new TestSubscriber<number>().subscribeTo(sink);
        sink.complete();
        expect(s1.completed).toBe(true);
        expect(s2.completed).toBe(true);
    });

    test('error() reaches all subscribers', () => {
        const sink = Sinks.many().multicast().directBestEffort<number>();
        const s1 = new TestSubscriber<number>().subscribeTo(sink);
        const s2 = new TestSubscriber<number>().subscribeTo(sink);
        const err = new Error('err');
        sink.error(err);
        expect(s1.errors).toEqual([err]);
        expect(s2.errors).toEqual([err]);
    });

    test('cancelled subscriber is removed and does not receive further emissions', () => {
        const sink = Sinks.many().multicast().directBestEffort<number>();
        const s1 = new TestSubscriber<number>().subscribeTo(sink, 5);
        const s2 = new TestSubscriber<number>().subscribeTo(sink, 5);
        sink.next(1);
        s1.cancel();
        sink.next(2);
        expect(s1.values).toEqual([1]);
        expect(s2.values).toEqual([1, 2]);
    });

});

// ---------------------------------------------------------------------------
// Sinks.many().multicast().onBackpressureBuffer()
// ---------------------------------------------------------------------------

describe('Sinks.many().multicast().onBackpressureBuffer()', () => {
    test('each subscriber receives all elements via their own buffer', () => {
        const sink = Sinks.many().multicast().onBackpressureBuffer<number>();
        const s1 = new TestSubscriber<number>().subscribeTo(sink, 0);
        const s2 = new TestSubscriber<number>().subscribeTo(sink, 5);
        sink.next(1); sink.next(2); sink.next(3);
        s1.request(3);
        expect(s1.values).toEqual([1, 2, 3]);
        expect(s2.values).toEqual([1, 2, 3]);
    });

    test('buffer overflow cancels slow subscriber with error', () => {
        const sink = Sinks.many().multicast().onBackpressureBuffer<number>(2);
        const slow = new TestSubscriber<number>().subscribeTo(sink, 0);
        sink.next(1); sink.next(2); sink.next(3); // 3rd overflows buffer of 2
        expect(slow.errors).toHaveLength(1);
        expect(slow.errors[0].message).toMatch(/overflow/i);
    });

    test('complete() delivers buffered elements then onComplete', () => {
        const sink = Sinks.many().multicast().onBackpressureBuffer<number>();
        const sub = new TestSubscriber<number>().subscribeTo(sink, 0);
        sink.next(1); sink.next(2);
        sink.complete();
        sub.request(2);
        expect(sub.values).toEqual([1, 2]);
        expect(sub.completed).toBe(true);
    });

    test('error() propagates immediately to all subscribers', () => {
        const sink = Sinks.many().multicast().onBackpressureBuffer<number>();
        const s1 = new TestSubscriber<number>().subscribeTo(sink);
        const s2 = new TestSubscriber<number>().subscribeTo(sink);
        const err = new Error('e');
        sink.error(err);
        expect(s1.errors).toEqual([err]);
        expect(s2.errors).toEqual([err]);
    });

    test('autoCancel=true: terminates sink when last subscriber cancels', () => {
        const sink = Sinks.many().multicast().onBackpressureBuffer<number>(256, true);
        const s1 = new TestSubscriber<number>().subscribeTo(sink);
        const s2 = new TestSubscriber<number>().subscribeTo(sink);
        s1.cancel();
        s2.cancel();
        // After autoCancel, new emissions should be no-ops
        expect(() => sink.next(1)).not.toThrow();
    });

});

// ---------------------------------------------------------------------------
// Sinks.many().replay().all()
// ---------------------------------------------------------------------------

describe('Sinks.many().replay().all()', () => {
    test('new subscriber receives full history', () => {
        const sink = Sinks.many().replay().all<number>();
        sink.next(1); sink.next(2); sink.next(3);
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        expect(sub.values).toEqual([1, 2, 3]);
    });

    test('history is replayed before live emissions', () => {
        const sink = Sinks.many().replay().all<number>();
        sink.next(1); sink.next(2);
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        sink.next(3);
        expect(sub.values).toEqual([1, 2, 3]);
    });

    test('backpressure: replay respects request count', () => {
        const sink = Sinks.many().replay().all<number>();
        sink.next(1); sink.next(2); sink.next(3);
        const sub = new TestSubscriber<number>().subscribeTo(sink, 2);
        expect(sub.values).toEqual([1, 2]);
        sub.request(1);
        expect(sub.values).toEqual([1, 2, 3]);
    });

    test('complete() is replayed — late subscriber gets history then onComplete', () => {
        const sink = Sinks.many().replay().all<number>();
        sink.next(10); sink.next(20);
        sink.complete();
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        expect(sub.values).toEqual([10, 20]);
        expect(sub.completed).toBe(true);
    });

    test('error() is replayed — late subscriber gets history then onError', () => {
        const sink = Sinks.many().replay().all<number>();
        sink.next(1);
        const err = new Error('replay-err');
        sink.error(err);
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        expect(sub.values).toEqual([1]);
        expect(sub.errors).toEqual([err]);
    });

    test('empty completed sink — late subscriber gets only onComplete', () => {
        const sink = Sinks.many().replay().all<number>();
        sink.complete();
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        expect(sub.values).toHaveLength(0);
        expect(sub.completed).toBe(true);
    });

    test('multiple live subscribers all receive elements', () => {
        const sink = Sinks.many().replay().all<number>();
        const s1 = new TestSubscriber<number>().subscribeTo(sink);
        sink.next(1);
        const s2 = new TestSubscriber<number>().subscribeTo(sink); // s2 gets replay of [1]
        sink.next(2);
        expect(s1.values).toEqual([1, 2]);
        expect(s2.values).toEqual([1, 2]);
    });

});

// ---------------------------------------------------------------------------
// Sinks.many().replay().latest(n) and .limit(n)
// ---------------------------------------------------------------------------

describe('Sinks.many().replay().latest(n) / .limit(n)', () => {
    test('new subscriber receives last N elements', () => {
        const sink = Sinks.many().replay().latest<number>(2);
        sink.next(1); sink.next(2); sink.next(3);
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        expect(sub.values).toEqual([2, 3]);
    });

    test('history does not grow beyond limit', () => {
        const sink = Sinks.many().replay().limit<number>(3);
        for (let i = 1; i <= 6; i++) sink.next(i);
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        expect(sub.values).toEqual([4, 5, 6]);
    });

    test('complete() is replayed after bounded history', () => {
        const sink = Sinks.many().replay().latest<number>(2);
        sink.next(1); sink.next(2); sink.next(3);
        sink.complete();
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        expect(sub.values).toEqual([2, 3]);
        expect(sub.completed).toBe(true);
    });

    test('error() is replayed after bounded history', () => {
        const sink = Sinks.many().replay().limit<number>(1);
        sink.next(9);
        const err = new Error('e');
        sink.error(err);
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        expect(sub.values).toEqual([9]);
        expect(sub.errors).toEqual([err]);
    });

    test('live subscribers receive all elements, not just last N', () => {
        const sink = Sinks.many().replay().latest<number>(2);
        const live = new TestSubscriber<number>().subscribeTo(sink);
        sink.next(1); sink.next(2); sink.next(3);
        expect(live.values).toEqual([1, 2, 3]);
    });

});

// ---------------------------------------------------------------------------
// Sinks.many().replay().latestOrDefault(value)
// ---------------------------------------------------------------------------

describe('Sinks.many().replay().latestOrDefault(value)', () => {
    test('new subscriber receives default value when nothing was emitted', () => {
        const sink = Sinks.many().replay().latestOrDefault<number>(0);
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        expect(sub.values).toEqual([0]);
    });

    test('new subscriber receives last emitted value instead of default', () => {
        const sink = Sinks.many().replay().latestOrDefault<number>(0);
        sink.next(5); sink.next(10);
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        expect(sub.values).toEqual([10]);
    });

    test('live subscriber receives each emitted value', () => {
        const sink = Sinks.many().replay().latestOrDefault<number>(-1);
        const sub = new TestSubscriber<number>().subscribeTo(sink, 0); // starts with no demand
        sub.request(1); // drains default
        sink.next(1);
        sub.request(1);
        sink.next(2);
        sub.request(1);
        expect(sub.values).toEqual([-1, 1, 2]);
    });

    test('complete() after emission is replayed to late subscriber', () => {
        const sink = Sinks.many().replay().latestOrDefault<string>('none');
        sink.next('hello');
        sink.complete();
        const sub = new TestSubscriber<string>().subscribeTo(sink);
        expect(sub.values).toEqual(['hello']);
        expect(sub.completed).toBe(true);
    });

    test('error() is signalled to late subscriber', () => {
        const sink = Sinks.many().replay().latestOrDefault<number>(0);
        const err = new Error('err');
        sink.error(err);
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        expect(sub.errors).toEqual([err]);
    });

});

// ---------------------------------------------------------------------------
// Reactive Streams Specification Compliance
// ---------------------------------------------------------------------------

describe('Reactive Streams Spec — Rule 1.1: onNext count ≤ requested', () => {
    test('OneSink does not deliver value before demand is signalled', () => {
        const sink = Sinks.one<number>();
        // Subscribe but do NOT request
        let received: number[] = [];
        let completed = false;
        const sub = sink.subscribe({
            onSubscribe(_s) {},
            onNext(v) { received.push(v); },
            onError() {},
            onComplete() { completed = true; }
        });
        sink.next(42);
        sink.complete();
        // No demand yet — value must not have been delivered
        expect(received).toHaveLength(0);
        expect(completed).toBe(false);
        // Now request — value must be delivered
        sub.request(1);
        expect(received).toEqual([42]);
        expect(completed).toBe(true);
    });

    test('UnicastOnBackpressureBuffer does not deliver before demand', () => {
        const sink = Sinks.many().unicast().onBackpressureBuffer<number>();
        const sub = new TestSubscriber<number>().subscribeTo(sink, 0);
        sink.next(1); sink.next(2);
        expect(sub.values).toHaveLength(0);
        sub.request(1);
        expect(sub.values).toHaveLength(1);
    });

    test('ReplayAll does not deliver before demand', () => {
        const sink = Sinks.many().replay().all<number>();
        sink.next(1); sink.next(2);
        const sub = new TestSubscriber<number>().subscribeTo(sink, 0);
        expect(sub.values).toHaveLength(0);
        sub.request(2);
        expect(sub.values).toEqual([1, 2]);
    });
});

describe('Reactive Streams Spec — Rule 3.9: request(n ≤ 0) signals onError', () => {
    const sinksUnderTest: Array<[string, () => Publisher<number> & { subscribe: any }]> = [
        ['EmptySink', () => Sinks.empty<number>()],
        ['OneSink', () => Sinks.one<number>()],
        ['UnicastOnBackpressureBuffer', () => Sinks.many().unicast().onBackpressureBuffer<number>()],
        ['UnicastOnBackpressureError', () => Sinks.many().unicast().onBackpressureError<number>()],
        ['MulticastDirectAllOrNothing', () => Sinks.many().multicast().directAllOrNothing<number>()],
        ['MulticastDirectBestEffort', () => Sinks.many().multicast().directBestEffort<number>()],
        ['MulticastOnBackpressureBuffer', () => Sinks.many().multicast().onBackpressureBuffer<number>()],
        ['ReplayAll', () => Sinks.many().replay().all<number>()],
        ['ReplayLatest', () => Sinks.many().replay().latest<number>(2)],
        ['ReplayLatestOrDefault', () => Sinks.many().replay().latestOrDefault<number>(0)],
    ];

    test.each(sinksUnderTest)('%s: request(0) signals onError', (_name, factory) => {
        const sink = factory();
        const errors: Error[] = [];
        const sub = sink.subscribe({ onSubscribe(_s) {}, onNext() {}, onError(e: any) { errors.push(e); }, onComplete() {} });
        sub.request(0);
        expect(errors).toHaveLength(1);
        expect(errors[0].message).toMatch(/must be > 0/);
    });

    test.each(sinksUnderTest)('%s: request(-1) signals onError', (_name, factory) => {
        const sink = factory();
        const errors: Error[] = [];
        const sub = sink.subscribe({ onSubscribe(_s) {}, onNext() {}, onError(e: any) { errors.push(e); }, onComplete() {} });
        sub.request(-1);
        expect(errors).toHaveLength(1);
        expect(errors[0].message).toMatch(/must be > 0/);
    });

    test.each(sinksUnderTest)('%s: after request(0) error, further request() is NOP', (_name, factory) => {
        const sink = factory();
        const events: string[] = [];
        const sub = sink.subscribe({
            onSubscribe(_s) {},
            onNext() { events.push('next'); },
            onError() { events.push('error'); },
            onComplete() { events.push('complete'); }
        });
        sub.request(0);
        sub.request(1); // must be NOP after terminal error
        expect(events).toEqual(['error']);
    });
});

describe('Reactive Streams Spec — Rule 3.7: cancel() is idempotent', () => {
    test('UnicastOnBackpressureBuffer: cancel() twice does not throw', () => {
        const sink = Sinks.many().unicast().onBackpressureBuffer<number>();
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        expect(() => { sub.cancel(); sub.cancel(); }).not.toThrow();
    });

    test('MulticastDirectBestEffort: cancel() twice does not throw', () => {
        const sink = Sinks.many().multicast().directBestEffort<number>();
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        expect(() => { sub.cancel(); sub.cancel(); }).not.toThrow();
    });

    test('ReplayAll: cancel() twice does not throw', () => {
        const sink = Sinks.many().replay().all<number>();
        const sub = new TestSubscriber<number>().subscribeTo(sink);
        expect(() => { sub.cancel(); sub.cancel(); }).not.toThrow();
    });
});

describe('Reactive Streams Spec — Rule 3.6: after cancel, request() is NOP', () => {
    test('UnicastOnBackpressureBuffer: request() after cancel delivers nothing', () => {
        const sink = Sinks.many().unicast().onBackpressureBuffer<number>();
        sink.next(1); sink.next(2);
        const sub = new TestSubscriber<number>().subscribeTo(sink, 0);
        sub.cancel();
        sub.request(10);
        expect(sub.values).toHaveLength(0);
        expect(sub.errors).toHaveLength(0);
    });

    test('ReplayAll: request() after cancel delivers nothing', () => {
        const sink = Sinks.many().replay().all<number>();
        sink.next(1); sink.next(2);
        const sub = new TestSubscriber<number>().subscribeTo(sink, 0);
        sub.cancel();
        sub.request(10);
        expect(sub.values).toHaveLength(0);
    });
});

describe('Reactive Streams Spec — Rule 3.16: no unbounded recursion (request in onNext)', () => {
    test('UnicastOnBackpressureBuffer: 10 000 items with request(1) in onNext — no stack overflow', () => {
        const N = 10_000;
        const sink = Sinks.many().unicast().onBackpressureBuffer<number>();
        for (let i = 0; i < N; i++) sink.next(i);
        sink.complete();

        const received: number[] = [];
        let completed = false;
        let sub: Subscription;
        sub = sink.subscribe({
            onSubscribe(_s) {},
            onNext(v) {
                received.push(v);
                sub.request(1); // re-entrant request from within onNext
            },
            onError() {},
            onComplete() { completed = true; }
        });
        sub.request(1); // kick off
        expect(received).toHaveLength(N);
        expect(completed).toBe(true);
    });

    test('ReplayAll: 10 000 items with request(1) in onNext — no stack overflow', () => {
        const N = 10_000;
        const sink = Sinks.many().replay().all<number>();
        for (let i = 0; i < N; i++) sink.next(i);
        sink.complete();

        const received: number[] = [];
        let sub: Subscription;
        sub = sink.subscribe({
            onSubscribe(_s) {},
            onNext(v) {
                received.push(v);
                sub.request(1);
            },
            onError() {},
            onComplete() {}
        });
        sub.request(1);
        expect(received).toHaveLength(N);
    });

    test('ReplayLatest: re-entrant request in onNext — no stack overflow', () => {
        const N = 10_000;
        const sink = Sinks.many().replay().latest<number>(N);
        for (let i = 0; i < N; i++) sink.next(i);
        sink.complete();

        const received: number[] = [];
        let sub: Subscription;
        sub = sink.subscribe({
            onSubscribe(_s) {},
            onNext(v) { received.push(v); sub.request(1); },
            onError() {},
            onComplete() {}
        });
        sub.request(1);
        expect(received).toHaveLength(N);
    });

    test('MulticastOnBackpressureBuffer: re-entrant request in onNext — no stack overflow', () => {
        // MulticastOnBackpressureBuffer does NOT replay history to late subscribers,
        // so subscribe first, then emit.
        const N = 1_000;
        const sink = Sinks.many().multicast().onBackpressureBuffer<number>(N + 1);

        const received: number[] = [];
        let sub: Subscription;
        sub = sink.subscribe({
            onSubscribe(_s) {},
            onNext(v) { received.push(v); sub.request(1); },
            onError() {},
            onComplete() {}
        });
        sub.request(1); // initial demand

        for (let i = 0; i < N; i++) sink.next(i);
        sink.complete();

        expect(received).toHaveLength(N);
    });
});
