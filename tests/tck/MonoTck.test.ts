/**
 * Reactive Streams TCK — Mono Publisher verification
 *
 * Publisher under test  : Mono<number>
 * create(0)             : Mono.empty()   — completes without emitting
 * create(1)             : Mono.just(0)   — emits one element then completes
 * create(n > 1)         : SKIPPED — Mono is a 0-or-1 publisher
 * createFailed()        : Mono.error(…) — signals onError immediately
 * maxElements           : 1
 *
 * KNOWN DEVIATION (Mono-specific)
 *   Mono is not required to support n > 1 elements. All TCK tests that
 *   need more than 1 element are automatically skipped via `maxElements: 1`.
 *   This is the correct behaviour per the Reactive Streams spec — the spec
 *   applies to Publishers of any cardinality, and a 0/1 publisher is valid.
 */

import {Mono, Subscriber, Subscription} from '@/.';
import {runPublisherVerification, TckSubscriber} from './PublisherVerification';

// ─────────────────────────── Shared TCK ──────────────────────────────────────

describe('Mono — Reactive Streams Publisher spec (Mono.just / Mono.empty)', () => {
    runPublisherVerification({
        create: (n) => n === 0 ? Mono.empty<number>() : Mono.just(0),
        createFailed: () => Mono.error<number>(new Error('test-error')),
        maxElements: 1,  // Mono emits at most 1 element — multi-element tests are skipped
    });
});

describe('Mono — Reactive Streams Publisher spec (Mono.generate)', () => {
    runPublisherVerification({
        create: (n) => n === 0
            ? Mono.generate<number>(sink => sink.complete())
            : Mono.generate<number>(sink => sink.next(0)),
        createFailed: () => Mono.generate<number>(sink => sink.error(new Error('test-error'))),
        maxElements: 1,
    });
});

// ─────────────────────────── Mono-specific rules ─────────────────────────────
// NOTE: Mono.fromPromise is intentionally excluded from runPublisherVerification.
// Promise resolution is asynchronous (microtask queue), but the TCK is synchronous.
// Async Mono.fromPromise behaviour is verified separately below with async/await.

describe('Mono — additional spec checks', () => {

    // Rule 1.3 (ordering): onSubscribe always first
    it('Rule 1.3 (ordering): onSubscribe MUST be the first signal', () => {
        const order: string[] = [];
        Mono.just(42).subscribe({
            onSubscribe() { order.push('subscribe'); },
            onNext() { order.push('next'); },
            onError() { order.push('error'); },
            onComplete() { order.push('complete'); }
        });
        expect(order[0]).toBe('subscribe');
    });

    it('Rule 1.3 (ordering): onSubscribe before onComplete for empty Mono', () => {
        const order: string[] = [];
        Mono.empty<number>().subscribe({
            onSubscribe(s) { order.push('subscribe'); s.request(1); },
            onNext() { order.push('next'); },
            onError() { order.push('error'); },
            onComplete() { order.push('complete'); }
        });
        expect(order).toEqual(['subscribe', 'complete']);
    });

    it('Rule 1.3 (ordering): onSubscribe before onError for failed Mono', () => {
        const order: string[] = [];
        Mono.error<number>(new Error()).subscribe({
            onSubscribe(s) { order.push('subscribe'); s.request(1); },
            onNext() { order.push('next'); },
            onError() { order.push('error'); },
            onComplete() { order.push('complete'); }
        });
        expect(order).toEqual(['subscribe', 'error']);
    });

    // Rule 1.7: No signals after terminal — via Mono.generate
    it('Rule 1.7: Producer calling next() after next() on Mono.generate — second is silently dropped', () => {
        const ts = new TckSubscriber<number>();
        Mono.generate<number>(sink => {
            sink.next(1);
            sink.next(2);   // MUST be ignored — Mono already terminated
            sink.error(new Error('ignored'));
        }).subscribe(ts);
        ts.request(10);
        expect(ts.received).toEqual([1]);
        expect(ts.completed).toBe(true);
        expect(ts.terminalCount).toBe(1);
    });

    it('Rule 1.7: error() after complete() on Mono.generate is silently dropped', () => {
        const ts = new TckSubscriber<number>();
        Mono.generate<number>(sink => {
            sink.complete();
            sink.error(new Error('should be dropped'));
        }).subscribe(ts);
        ts.request(1);
        expect(ts.received).toHaveLength(0);
        expect(ts.completed).toBe(true);
        expect(ts.errors).toHaveLength(0);
        expect(ts.terminalCount).toBe(1);
    });

    // Rule 1.8: cancel stops delivery
    it('Rule 1.8: cancel() before request() prevents item delivery', () => {
        const ts = new TckSubscriber<number>();
        Mono.just(1).subscribe(ts);
        ts.cancel();
        ts.request(1); // should be no-op after cancel
        expect(ts.received).toHaveLength(0);
        expect(ts.terminalCount).toBe(0);
    });

    // Rule 3.6: request() after cancel() is no-op
    it('Rule 3.6: request() after cancel() MUST be a no-op', () => {
        const ts = new TckSubscriber<number>();
        Mono.just(42).subscribe(ts);
        ts.cancel();
        expect(() => ts.request(1)).not.toThrow();
        expect(ts.received).toHaveLength(0);
    });

    // Rule 2.12: onSubscribe called exactly once
    it('Rule 2.12: onSubscribe called exactly once per subscribe()', () => {
        const ts = new TckSubscriber<number>();
        Mono.just(1).subscribe(ts);
        ts.request(1);
        expect(ts.subscribeCount).toBe(1);
    });

    // Cold publisher: each subscriber gets its own independent stream
    it('Rule 1.10 (cold): each subscriber receives the same value independently', () => {
        const source = Mono.just(99);
        const ts1 = new TckSubscriber<number>();
        const ts2 = new TckSubscriber<number>();
        source.subscribe(ts1);
        source.subscribe(ts2);
        ts1.request(1);
        ts2.request(1);
        expect(ts1.received).toEqual([99]);
        expect(ts2.received).toEqual([99]);
        expect(ts1.completed).toBe(true);
        expect(ts2.completed).toBe(true);
    });

    // Mono.generate — delivery deferred until demand
    it('Mono.generate defers delivery until request()', () => {
        const ts = new TckSubscriber<number>();
        Mono.generate<number>(sink => sink.next(7)).subscribe(ts);
        expect(ts.received).toHaveLength(0); // no demand yet
        ts.request(1);
        expect(ts.received).toEqual([7]);
        expect(ts.completed).toBe(true);
    });

    // Mono.fromPromise — async delivery
    it('Mono.fromPromise resolves as a valid Publisher', async () => {
        const ts = new TckSubscriber<number>();
        Mono.fromPromise(Promise.resolve(42)).subscribe(ts);
        ts.request(1);
        await Promise.resolve(); // flush microtask queue
        expect(ts.received).toEqual([42]);
        expect(ts.completed).toBe(true);
    });

    it('Mono.fromPromise rejection signals onError', async () => {
        const ts = new TckSubscriber<number>();
        Mono.fromPromise<number>(Promise.reject(new Error('boom'))).subscribe(ts);
        ts.request(1);
        // Promise.reject().then().catch() takes 2 microtask ticks:
        // tick 1 — rejected .then() propagates rejection to intermediate promise
        // tick 2 — .catch() handler fires
        await Promise.resolve();
        await Promise.resolve();
        expect(ts.errors).toHaveLength(1);
        expect(ts.errors[0].message).toBe('boom');
    });
});
