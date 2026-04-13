/**
 * Reactive Streams Publisher Specification — TypeScript TCK
 *
 * Ported from the official Java TCK:
 * https://github.com/reactive-streams/reactive-streams-jvm/tree/master/tck
 * https://github.com/reactive-streams/reactive-streams-jvm/blob/master/README.md
 *
 * LEGEND
 *   required_  — the rule is MUST; the test MUST pass for spec compliance
 *   optional_  — the rule is SHOULD/MAY; deviation is acceptable with justification
 *   skipped_   — rule not testable in the JS/TS single-threaded synchronous environment,
 *                or enforced statically by TypeScript's type system
 *
 * KNOWN ACCEPTABLE DEVIATIONS (shared across all implementations)
 *   • Rule 1.9  — subscribe(null) MUST throw NullPointerException.
 *                 TypeScript prevents null Subscribers at compile time;
 *                 no runtime guard needed.
 *   • Rule 2.7  — all calls on Subscription MUST come from the same thread.
 *                 JavaScript is single-threaded; trivially satisfied, not testable.
 *   • Rule 3.1  — request/cancel MUST only be called from within its Subscriber.
 *                 Callee-side enforcement is impossible at library level.
 *   • Rule 3.4/3.5 — request/cancel SHOULD return in a timely manner.
 *                 "Timely" is subjective; no deterministic assertion possible.
 *   • Rule 3.13 (Long.MAX_VALUE) — adapted to Number.MAX_SAFE_INTEGER (2^53−1).
 *                 JS has no 64-bit integers; MAX_SAFE_INTEGER is the largest exact integer.
 */

import {Publisher, Subscriber, Subscription} from '@/.';

// ─────────────────────────── TCK Subscriber ──────────────────────────────────

/**
 * Minimal test subscriber that records every signal in arrival order
 * and exposes helpers for assertions.
 */
export class TckSubscriber<T> implements Subscriber<T> {

    private _sub!: Subscription;
    readonly received: T[] = [];
    readonly errors: Error[] = [];
    completed = false;
    subscribeCount = 0;

    get sub(): Subscription { return this._sub; }

    onSubscribe(s: Subscription): void { this.subscribeCount++; this._sub = s; }
    onNext(v: T): void { this.received.push(v); }
    onError(e: Error): void { this.errors.push(e); }
    onComplete(): void { this.completed = true; }

    /** Cumulative terminal signals (errors + complete). MUST be ≤ 1 by Rule 1.7. */
    get terminalCount(): number { return this.errors.length + (this.completed ? 1 : 0); }

    request(n: number): void { this._sub.request(n); }
    cancel(): void { this._sub.unsubscribe(); }
}

// ─────────────────────────── Verification factory ────────────────────────────

/**
 * Options for `runPublisherVerification`.
 *
 * @param create         Factory: `create(n)` must produce a Publisher that emits
 *                       exactly `n` elements and then completes.
 *                       `create(0)` must complete without emitting.
 * @param createFailed   Factory for a Publisher that terminates with onError immediately
 *                       (before any onNext). Pass `null` if the publisher cannot fail.
 * @param maxElements    Maximum number of elements the publisher can produce in one
 *                       subscription. Tests requiring more elements are skipped.
 *                       Defaults to Infinity (unlimited).
 */
export interface VerificationOptions {
    create: (elements: number) => Publisher<number>;
    createFailed: (() => Publisher<number>) | null;
    maxElements?: number;
}

/**
 * Registers all Reactive Streams Publisher spec tests in the current Jest describe scope.
 * Call this inside a `describe` block.
 *
 * @example
 * describe('Flux — Reactive Streams TCK', () => {
 *   runPublisherVerification({
 *     create: n => Flux.range(0, n),
 *     createFailed: () => Flux.error(new Error('test')),
 *   });
 * });
 */
export function runPublisherVerification(opts: VerificationOptions): void {
    const {create, createFailed} = opts;
    const maxElements = opts.maxElements ?? Infinity;

    /**
     * Skip a test if the publisher cannot produce enough elements.
     * Mirrors the Java TCK's `publisherUnableToSignalOnComplete(n)` guard.
     */
    const itRequiring = (n: number, name: string, fn: () => void) => {
        if (n > maxElements) {
            it.skip(`${name} [skipped: publisher produces at most ${maxElements} element(s)]`, () => {});
        } else {
            it(name, fn);
        }
    };

    // ─── Baseline sanity ───────────────────────────────────────────────────

    itRequiring(1, 'baseline: create(1) produces exactly 1 element then completes', () => {
        const ts = new TckSubscriber<number>();
        create(1).subscribe(ts);
        ts.request(1);
        expect(ts.received).toHaveLength(1);
        expect(ts.completed).toBe(true);
        expect(ts.errors).toHaveLength(0);
    });

    itRequiring(3, 'baseline: create(3) produces exactly 3 elements then completes', () => {
        const ts = new TckSubscriber<number>();
        create(3).subscribe(ts);
        ts.request(3);
        expect(ts.received).toHaveLength(3);
        expect(ts.completed).toBe(true);
    });

    it('baseline: create(0) completes immediately without emitting', () => {
        const ts = new TckSubscriber<number>();
        create(0).subscribe(ts);
        ts.request(1);
        expect(ts.received).toHaveLength(0);
        expect(ts.completed).toBe(true);
    });

    // ─── Rule 1.1 ──────────────────────────────────────────────────────────
    // The total number of onNext signals sent by a Publisher to a Subscriber
    // MUST be less than or equal to the total number of elements requested by
    // that Subscriber's Subscription at all times.

    itRequiring(10, 'required_spec101: onNext count MUST NOT exceed cumulative demand', () => {
        const ts = new TckSubscriber<number>();
        create(10).subscribe(ts);
        ts.request(3);
        expect(ts.received.length).toBeLessThanOrEqual(3);
    });

    itRequiring(10, 'required_spec101: exactly n onNext when demand = n and n elements available', () => {
        const ts = new TckSubscriber<number>();
        create(10).subscribe(ts);
        ts.request(5);
        expect(ts.received).toHaveLength(5);
    });

    // ─── Rule 1.2 ──────────────────────────────────────────────────────────
    // A Publisher MAY signal fewer onNext than requested and terminate the
    // Subscription by calling onComplete or onError.

    itRequiring(3, 'required_spec102: Publisher MAY emit fewer than requested and complete', () => {
        const ts = new TckSubscriber<number>();
        create(3).subscribe(ts);
        ts.request(100); // request more than available
        expect(ts.received).toHaveLength(3);
        expect(ts.completed).toBe(true);
    });

    // ─── Rule 1.4 ──────────────────────────────────────────────────────────
    // If a Publisher fails it MUST signal an onError.

    if (createFailed !== null) {
        it('required_spec104: failed Publisher MUST signal onError', () => {
            const ts = new TckSubscriber<number>();
            createFailed!().subscribe(ts);
            ts.request(1);
            expect(ts.errors).toHaveLength(1);
            expect(ts.completed).toBe(false);
            expect(ts.received).toHaveLength(0);
        });
    } else {
        it.skip('required_spec104: failed publisher not provided by this factory', () => {});
    }

    // ─── Rule 1.5 ──────────────────────────────────────────────────────────
    // If a Publisher terminates successfully (finishes publishing) it MUST
    // signal an onComplete.

    itRequiring(3, 'required_spec105: exhausted Publisher MUST signal onComplete', () => {
        const ts = new TckSubscriber<number>();
        create(3).subscribe(ts);
        ts.request(3);
        expect(ts.completed).toBe(true);
        expect(ts.errors).toHaveLength(0);
    });

    // ─── Rule 1.7 ──────────────────────────────────────────────────────────
    // Once a terminal state has been signalled (onError / onComplete) it
    // MUST NOT signal any further signals.

    itRequiring(3, 'required_spec107: no onNext/onError/onComplete after onComplete', () => {
        const ts = new TckSubscriber<number>();
        create(3).subscribe(ts);
        ts.request(3);
        expect(ts.completed).toBe(true);
        const snapshot = { received: ts.received.length, terminals: ts.terminalCount };
        ts.request(10); // extra request after terminal
        expect(ts.received).toHaveLength(snapshot.received);
        expect(ts.terminalCount).toBe(1); // exactly one terminal signal total
    });

    if (createFailed !== null) {
        it('required_spec107: no signals after onError', () => {
            const ts = new TckSubscriber<number>();
            createFailed!().subscribe(ts);
            ts.request(1);
            expect(ts.errors).toHaveLength(1);
            const snapshot = { received: ts.received.length, terminals: ts.terminalCount };
            ts.request(10);
            expect(ts.received).toHaveLength(snapshot.received);
            expect(ts.terminalCount).toBe(snapshot.terminals); // no new terminal
        });
    }

    // ─── Rule 1.8 ──────────────────────────────────────────────────────────
    // If a Subscription is cancelled its Subscriber MUST eventually stop
    // being signalled.

    itRequiring(10, 'required_spec108: no signals after Subscription.cancel()', () => {
        const received: number[] = [];
        let sub!: Subscription;
        let cancelled = false;
        let straySignals = 0;

        create(10).subscribe({
            onSubscribe(s) { sub = s; s.request(10); },
            onNext(v) {
                if (cancelled) { straySignals++; return; }
                received.push(v);
                if (received.length === 3) {
                    cancelled = true;
                    sub.unsubscribe();
                }
            },
            onError() { if (cancelled) straySignals++; },
            onComplete() { if (cancelled) straySignals++; }
        });

        // For synchronous publishers: cancel stops delivery immediately.
        // The Spec allows "eventually", so we only require stray = 0 for sync.
        expect(straySignals).toBe(0);
        expect(received.length).toBeLessThan(10);
    });

    // ─── Rule 1.9 ──────────────────────────────────────────────────────────
    // Calling Publisher.subscribe(null) MUST throw NullPointerException.
    // DEVIATION: TypeScript's type system prevents null Subscribers at compile
    // time. No runtime guard is implemented or needed.

    it.skip(
        'skipped_spec109: subscribe(null) — TypeScript prevents null Subscribers statically; ' +
        'no runtime check needed',
        () => {}
    );

    // ─── Rule 1.10 ─────────────────────────────────────────────────────────
    // Publisher.subscribe() MAY be called as many times as wanted but with a
    // different Subscriber each time. (Cold publishers must replay independently.)

    itRequiring(3, 'required_spec110: subscribe called multiple times creates independent streams', () => {
        const ts1 = new TckSubscriber<number>();
        const ts2 = new TckSubscriber<number>();
        const publisher = create(3);

        publisher.subscribe(ts1);
        publisher.subscribe(ts2);
        ts1.request(3);
        ts2.request(3);

        expect(ts1.received).toHaveLength(3);
        expect(ts2.received).toHaveLength(3);
        expect(ts1.received).toEqual(ts2.received);
        expect(ts1.completed).toBe(true);
        expect(ts2.completed).toBe(true);
    });

    // ─── Rule 2.9 ──────────────────────────────────────────────────────────
    // A Subscriber MUST be prepared to receive onComplete even if no
    // Subscription.request has been made.
    // Verified from Publisher side: empty() sends onComplete before any request.

    it('required_spec209 (Publisher side): create(0) MUST eventually signal onComplete', () => {
        const order: string[] = [];
        create(0).subscribe({
            onSubscribe(s) { order.push('onSubscribe'); s.request(1); },
            onNext() { order.push('onNext'); },
            onError() { order.push('onError'); },
            onComplete() { order.push('onComplete'); }
        });
        expect(order[0]).toBe('onSubscribe');
        expect(order).toContain('onComplete');
        expect(order).not.toContain('onNext');
        expect(order).not.toContain('onError');
    });

    if (createFailed !== null) {
        it('required_spec210 (Publisher side): createFailed() MUST eventually signal onError', () => {
            const order: string[] = [];
            createFailed!().subscribe({
                onSubscribe(s) { order.push('onSubscribe'); s.request(1); },
                onNext() { order.push('onNext'); },
                onError() { order.push('onError'); },
                onComplete() { order.push('onComplete'); }
            });
            expect(order[0]).toBe('onSubscribe');
            expect(order).toContain('onError');
            expect(order).not.toContain('onNext');
            expect(order).not.toContain('onComplete');
        });
    }

    // ─── Rule 3.2 ──────────────────────────────────────────────────────────
    // Subscription.request MUST place a limit on possible synchronous
    // recursion between Publisher and Subscriber.
    // Here: verify that incremental requests accumulate demand correctly.

    itRequiring(6, 'required_spec302: incremental request calls accumulate demand', () => {
        const ts = new TckSubscriber<number>();
        create(6).subscribe(ts);
        ts.request(2);
        expect(ts.received).toHaveLength(2);
        ts.request(2);
        expect(ts.received).toHaveLength(4);
        ts.request(2);
        expect(ts.received).toHaveLength(6);
        expect(ts.completed).toBe(true);
    });

    // ─── Rule 3.3 / 3.16 ───────────────────────────────────────────────────
    // Subscription.request MUST NOT cause Subscriber.onNext to be called
    // recursively from within Subscriber.onNext (must be trampoline-safe).
    // Rule 3.16: re-entrant request() from onNext MUST NOT stack-overflow.

    itRequiring(10, 'required_spec303_and_spec316: request() from onNext MUST NOT cause unbounded recursion', () => {
        let depth = 0;
        let maxDepth = 0;
        const received: number[] = [];
        let sub!: Subscription;

        create(10).subscribe({
            onSubscribe(s) { sub = s; s.request(1); },
            onNext(v) {
                depth++;
                maxDepth = Math.max(maxDepth, depth);
                received.push(v);
                if (received.length < 10) sub.request(1);
                depth--;
            },
            onError() {},
            onComplete() {}
        });

        expect(received).toHaveLength(10);
        // Depth must be bounded. Publishers using a draining flag achieve depth ≤ 1;
        // trampoline-based publishers achieve depth = 1 (no re-entry).
        // We allow ≤ 2 to accommodate one level of legitimate re-entry.
        expect(maxDepth).toBeLessThanOrEqual(2);
    });

    // ─── Rule 3.9 ──────────────────────────────────────────────────────────
    // While the Subscription is not cancelled, Subscription.request(n ≤ 0)
    // MUST signal onError with java.lang.IllegalArgumentException.

    it('required_spec309: request(0) MUST signal onError (IllegalArgumentException equivalent)', () => {
        const ts = new TckSubscriber<number>();
        create(1).subscribe(ts);
        ts.request(0);
        expect(ts.errors).toHaveLength(1);
        expect(ts.errors[0].message).toMatch(/0/);
        expect(ts.completed).toBe(false);
    });

    it('required_spec309: request(-1) MUST signal onError (IllegalArgumentException equivalent)', () => {
        const ts = new TckSubscriber<number>();
        create(1).subscribe(ts);
        ts.request(-1);
        expect(ts.errors).toHaveLength(1);
        expect(ts.errors[0].message).toMatch(/-1/);
    });

    // ─── Rule 3.12 ─────────────────────────────────────────────────────────
    // Calling Subscription.cancel() MUST return normally (must not throw).

    it('required_spec312: Subscription.cancel() MUST return normally', () => {
        const ts = new TckSubscriber<number>();
        create(1).subscribe(ts);
        expect(() => ts.cancel()).not.toThrow();
    });

    it('required_spec312: Subscription.cancel() called twice MUST return normally', () => {
        const ts = new TckSubscriber<number>();
        create(1).subscribe(ts);
        expect(() => { ts.cancel(); ts.cancel(); }).not.toThrow();
    });

    // ─── Rule 3.13 ─────────────────────────────────────────────────────────
    // Calling Subscription.request() MUST return normally.

    it('required_spec313: Subscription.request() MUST return normally', () => {
        const ts = new TckSubscriber<number>();
        create(1).subscribe(ts);
        expect(() => ts.request(1)).not.toThrow();
    });

    // ─── Rule 3.17 (adapted) ───────────────────────────────────────────────
    // Calling Subscription.request() with Long.MAX_VALUE must not overflow.
    // ADAPTATION: JavaScript has no 64-bit integers.
    //   We use Number.MAX_SAFE_INTEGER (2^53−1) as the upper bound.
    //   Publishers MUST accumulate demand correctly at this limit.

    itRequiring(3, 'required_spec317 (adapted): demand of Number.MAX_SAFE_INTEGER MUST work', () => {
        const ts = new TckSubscriber<number>();
        create(3).subscribe(ts);
        ts.request(Number.MAX_SAFE_INTEGER);
        expect(ts.received).toHaveLength(3);
        expect(ts.completed).toBe(true);
    });

    // ─── Optional / context-dependent ──────────────────────────────────────

    it.skip(
        'skipped_spec207: all Subscription calls from same thread — ' +
        'JavaScript is single-threaded; trivially satisfied, not testable',
        () => {}
    );

    it.skip(
        'skipped_spec301: request/cancel only from Subscriber — ' +
        'callee-side enforcement is impossible at library level',
        () => {}
    );

    it.skip(
        'skipped_spec304_305: request/cancel return in timely manner — ' +
        '"timely" is not deterministically assertable in a synchronous environment',
        () => {}
    );

    it.skip(
        'skipped_spec313_drop_reference: cancel drops Subscriber reference — ' +
        'GC behaviour is not observable via the public API',
        () => {}
    );
}
