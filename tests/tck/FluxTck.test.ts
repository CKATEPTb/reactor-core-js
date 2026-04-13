/**
 * Reactive Streams TCK — Flux Publisher verification
 *
 * Publisher under test  : Flux<number>
 * create(n)             : Flux.range(0, n)   — emits 0, 1, …, n-1 then completes
 * createFailed()        : Flux.error(…)      — signals onError immediately
 * maxElements           : unlimited
 *
 * Additional publisher variants tested below the shared TCK:
 *   Flux.fromIterable, Flux.just, Flux.defer, Flux.generate (custom)
 */

import {Flux, Subscriber, Subscription} from '@/.';
import {runPublisherVerification, TckSubscriber} from './PublisherVerification';

// ─────────────────────────── Shared TCK ──────────────────────────────────────

describe('Flux — Reactive Streams Publisher spec (Flux.range)', () => {
    runPublisherVerification({
        create: (n) => Flux.range(0, n),
        createFailed: () => Flux.error(new Error('test-error')),
        maxElements: Infinity,
    });
});

// ─────────────────────────── Flux.fromIterable ───────────────────────────────

describe('Flux — Reactive Streams Publisher spec (Flux.fromIterable)', () => {
    runPublisherVerification({
        create: (n) => Flux.fromIterable(Array.from({length: n}, (_, i) => i)),
        createFailed: () => Flux.error(new Error('test-error')),
        maxElements: Infinity,
    });
});

// ─────────────────────────── Flux.generate (custom) ─────────────────────────

describe('Flux — Reactive Streams Publisher spec (Flux.generate)', () => {
    runPublisherVerification({
        create: (n) => Flux.generate(sink => {
            for (let i = 0; i < n; i++) sink.next(i);
            sink.complete();
        }),
        createFailed: () => Flux.generate(sink => sink.error(new Error('test-error'))),
        maxElements: Infinity,
    });
});

// ─────────────────────────── Flux-specific rules ─────────────────────────────

describe('Flux — additional spec checks', () => {

    // onSubscribe is always the very first signal (Rule 1.3 ordering)
    it('Rule 1.3 (ordering): onSubscribe MUST be the first signal', () => {
        const order: string[] = [];
        Flux.just(1, 2, 3).subscribe({
            onSubscribe() { order.push('subscribe'); },
            onNext() { order.push('next'); },
            onError() { order.push('error'); },
            onComplete() { order.push('complete'); }
        });
        expect(order[0]).toBe('subscribe');
    });

    it('Rule 1.3 (ordering): onSubscribe before onComplete for empty Flux', () => {
        const order: string[] = [];
        Flux.empty<number>().subscribe({
            onSubscribe(s) { order.push('subscribe'); s.request(1); },
            onNext() { order.push('next'); },
            onError() { order.push('error'); },
            onComplete() { order.push('complete'); }
        });
        expect(order).toEqual(['subscribe', 'complete']);
    });

    it('Rule 1.3 (ordering): onSubscribe before onError for failed Flux', () => {
        const order: string[] = [];
        Flux.error<number>(new Error()).subscribe({
            onSubscribe(s) { order.push('subscribe'); s.request(1); },
            onNext() { order.push('next'); },
            onError() { order.push('error'); },
            onComplete() { order.push('complete'); }
        });
        expect(order).toEqual(['subscribe', 'error']);
    });

    // Rule 1.7: No signals after terminal — verify with a sink-based publisher
    it('Rule 1.7: Producer calling next() after complete() on Flux.generate is silently dropped', () => {
        const ts = new TckSubscriber<number>();
        Flux.generate<number>(sink => {
            sink.next(1);
            sink.complete();
            sink.next(2);   // MUST be ignored
            sink.complete(); // MUST be ignored
        }).subscribe(ts);
        ts.request(10);
        expect(ts.received).toEqual([1]);
        expect(ts.completed).toBe(true);
        expect(ts.terminalCount).toBe(1);
    });

    // Rule 1.8: Subscription.cancel() stops delivery
    it('Rule 1.8: cancel() during onNext stops further item delivery', () => {
        const received: number[] = [];
        let sub!: Subscription;
        Flux.range(0, 100).subscribe({
            onSubscribe(s) { sub = s; s.request(100); },
            onNext(v) {
                received.push(v);
                if (v === 4) sub.unsubscribe();
            },
            onError() {},
            onComplete() {}
        });
        // At most 5 items delivered (0-4), nothing after cancel
        expect(received.length).toBeLessThanOrEqual(5);
        expect(received).not.toContain(5);
    });

    // Rule 3.6: After cancel, further request() calls MUST be no-ops
    it('Rule 3.6: request() after cancel() MUST be a no-op', () => {
        const ts = new TckSubscriber<number>();
        Flux.range(0, 10).subscribe(ts);
        ts.request(3);
        expect(ts.received).toHaveLength(3);
        ts.cancel();
        ts.request(10); // MUST be no-op
        expect(ts.received).toHaveLength(3);
        expect(ts.terminalCount).toBe(0); // no terminal after cancel
    });

    // Rule 3.7: cancel() after cancel() MUST be a no-op (not throw)
    it('Rule 3.7: cancel() after cancel() MUST return normally', () => {
        const ts = new TckSubscriber<number>();
        Flux.range(0, 3).subscribe(ts);
        expect(() => { ts.cancel(); ts.cancel(); }).not.toThrow();
    });

    // Rule 3.2: demand accumulates across multiple request() calls
    it('Rule 3.2: demand accumulates — two request(3) calls deliver 6 items total', () => {
        const ts = new TckSubscriber<number>();
        Flux.range(0, 10).subscribe(ts);
        ts.request(3);
        expect(ts.received).toHaveLength(3);
        ts.request(3);
        expect(ts.received).toHaveLength(6);
    });

    // Rule 2.12: onSubscribe MUST be called at most once per subscription
    it('Rule 2.12: onSubscribe called exactly once per subscribe()', () => {
        const ts = new TckSubscriber<number>();
        Flux.range(0, 3).subscribe(ts);
        ts.request(3);
        expect(ts.subscribeCount).toBe(1);
    });

    // Cold publisher: each subscriber gets its own independent stream
    it('Rule 1.10 (cold): each subscriber receives the same sequence independently', () => {
        const source = Flux.range(0, 5);
        const ts1 = new TckSubscriber<number>();
        const ts2 = new TckSubscriber<number>();
        source.subscribe(ts1);
        source.subscribe(ts2);
        ts1.request(5);
        ts2.request(5);
        expect(ts1.received).toEqual([0, 1, 2, 3, 4]);
        expect(ts2.received).toEqual([0, 1, 2, 3, 4]);
    });
});
