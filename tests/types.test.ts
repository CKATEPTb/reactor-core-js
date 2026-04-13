/**
 * Generic type inference tests for Flux and Mono operators.
 *
 * These tests verify that TypeScript correctly infers generic type parameters
 * throughout operator chains. The primary check is *compilation*: if a test
 * body fails to compile, the type system has regressed.
 *
 * Runtime assertions are secondary and kept minimal.
 */

import {Flux, Mono, Subscription} from '@/.';

// ─────────────────────────── Flux generics ───────────────────────────────────

describe('Flux — generic type inference', () => {

    // ── factories ─────────────────────────────────────────────────────────────

    it('Flux.just<T> infers element type', () => {
        const f: Flux<number> = Flux.just(1, 2, 3);
        f.subscribe((n: number) => n.toFixed(2));
    });

    it('Flux.range produces Flux<number>', () => {
        const f: Flux<number> = Flux.range(0, 5);
        f.subscribe((n: number) => n + 1);
    });

    it('Flux.fromIterable<string> infers element type', () => {
        const f: Flux<string> = Flux.fromIterable(['a', 'b']);
        f.subscribe((s: string) => s.toUpperCase());
    });

    it('Flux.generate<boolean> infers element type', () => {
        const f: Flux<boolean> = Flux.generate<boolean>(sink => { sink.next(true); sink.complete(); });
        f.subscribe((b: boolean) => b && true);
    });

    // ── map / mapNotNull ───────────────────────────────────────────────────────

    it('map<R> produces Flux<R>', () => {
        const f: Flux<string> = Flux.just(1, 2, 3).map(n => n.toString());
        f.subscribe((s: string) => s.length);
    });

    it('map chain preserves types at each step', () => {
        const f: Flux<boolean> = Flux.just('hello', 'world')
            .map(s => s.length)         // Flux<number>
            .map(n => n > 3);           // Flux<boolean>
        f.subscribe((b: boolean) => !b);
    });

    it('mapNotNull<R> produces Flux<NonNullable<R>>', () => {
        const f: Flux<number> = Flux.just('1', '', '2')
            .mapNotNull(s => s.length > 0 ? parseInt(s) : null);
        f.subscribe((n: number) => n * 2);
    });

    // ── flatMap / concatMap / switchMap ────────────────────────────────────────

    it('flatMap<R> with Flux inner produces Flux<R>', () => {
        const f: Flux<string> = Flux.just(1, 2, 3)
            .flatMap(n => Flux.just(n.toString()));
        f.subscribe((s: string) => s.toLowerCase());
    });

    it('flatMap<R> with Mono inner produces Flux<R>', () => {
        const f: Flux<string> = Flux.just(1, 2, 3)
            .flatMap(n => Mono.just(n.toString()));
        f.subscribe((s: string) => s.length);
    });

    it('flatMap chain: Flux<number> → flatMap → Flux<string> → map → Flux<boolean>', () => {
        const f: Flux<boolean> = Flux.just(1, 2, 3)
            .flatMap(n => Flux.just(n.toString()))  // Flux<string>
            .map(s => s.length > 0);                 // Flux<boolean>
        f.subscribe((b: boolean) => !b);
    });

    it('concatMap<R> produces Flux<R>', () => {
        const f: Flux<string> = Flux.just(1, 2, 3)
            .concatMap(n => Mono.just(`item-${n}`));
        f.subscribe((s: string) => s.startsWith('item'));
    });

    it('switchMap<R> produces Flux<R>', () => {
        const f: Flux<boolean> = Flux.just(1, 2, 3)
            .switchMap(n => Flux.just(n % 2 === 0));
        f.subscribe((b: boolean) => !b);
    });

    // ── filter / filterWhen ────────────────────────────────────────────────────

    it('filter preserves Flux<T>', () => {
        const f: Flux<number> = Flux.just(1, 2, 3, 4).filter(n => n % 2 === 0);
        f.subscribe((n: number) => n.toFixed());
    });

    it('filter after flatMap preserves correct type', () => {
        const f: Flux<string> = Flux.just(1, 2, 3)
            .flatMap(n => Flux.just(n.toString()))
            .filter(s => s.length > 0);
        f.subscribe((s: string) => s.toUpperCase());
    });

    // ── side-effect operators return correct this type ─────────────────────────

    it('doOnNext returns Flux<T>', () => {
        const f: Flux<number> = Flux.just(1, 2, 3).doOnNext(n => n.toFixed());
        f.subscribe((n: number) => n + 1);
    });

    it('publishOn returns Flux<T>', () => {
        const f: Flux<number> = Flux.just(1).publishOn({ schedule: fn => fn() });
        f.subscribe((n: number) => n.toFixed());
    });

    it('doOnNext → flatMap → filter: full chain types are preserved', () => {
        const f: Flux<boolean> = Flux.just(1, 2, 3)
            .doOnNext(n => n.toFixed())             // Flux<number>
            .flatMap(n => Flux.just(n % 2 === 0))  // Flux<boolean>
            .filter(b => b !== undefined);           // Flux<boolean>
        f.subscribe((b: boolean) => !b);
    });

    // ── cast<R> ────────────────────────────────────────────────────────────────

    it('cast<R> produces Flux<R>', () => {
        const source: Flux<unknown> = Flux.just(1, 2, 3);
        const f: Flux<number> = source.cast<number>();
        f.subscribe((n: number) => n.toFixed());
    });

    // ── aggregation / reduction operators ─────────────────────────────────────

    it('collect() produces Mono<T[]>', () => {
        const m: Mono<number[]> = Flux.just(1, 2, 3).collect();
        m.subscribe((arr: number[]) => arr.map(n => n * 2));
    });

    it('reduce produces Mono<T>', () => {
        const m: Mono<number> = Flux.just(1, 2, 3).reduce((acc, n) => acc + n);
        m.subscribe((sum: number) => sum.toFixed());
    });

    it('reduceWith<A> produces Mono<A>', () => {
        const m: Mono<string> = Flux.just(1, 2, 3)
            .reduceWith(() => '', (acc, n) => acc + n.toString());
        m.subscribe((s: string) => s.length);
    });

    it('then() produces Mono<void>', () => {
        const m: Mono<void> = Flux.just(1, 2, 3).then();
        m.subscribe(() => {});
    });

    it('thenEmpty produces Mono<void>', () => {
        const m: Mono<void> = Flux.just(1, 2, 3).thenEmpty(Flux.empty());
        m.subscribe(() => {});
    });

    it('count() produces Mono<number>', () => {
        const m: Mono<number> = Flux.just(1, 2, 3).count();
        m.subscribe((n: number) => n.toFixed());
    });

    it('hasElements() produces Mono<boolean>', () => {
        const m: Mono<boolean> = Flux.just(1).hasElements();
        m.subscribe((b: boolean) => !b);
    });

    it('first() produces Mono<T>', () => {
        const m: Mono<number> = Flux.just(1, 2, 3).first();
        m.subscribe((n: number) => n.toFixed());
    });

    it('last() produces Mono<T>', () => {
        const m: Mono<number> = Flux.just(1, 2, 3).last();
        m.subscribe((n: number) => n.toFixed());
    });

    it('elementAt(i) produces Mono<T>', () => {
        const m: Mono<number> = Flux.just(1, 2, 3).elementAt(1);
        m.subscribe((n: number) => n.toFixed());
    });

    // ── subscribe overloads return Subscription ────────────────────────────────

    it('subscribe(fullSubscriber) returns Subscription', () => {
        const sub: Subscription = Flux.just(1).subscribe({
            onSubscribe(s) { s.request(1); },
            onNext(_v) {},
            onError(_e) {},
            onComplete() {}
        });
        sub.request(0); // only checking the shape
        sub.unsubscribe();
    });

    it('subscribe(onNext) returns Subscription', () => {
        const sub: Subscription = Flux.just(1, 2, 3).subscribe(_n => {});
        sub.unsubscribe();
    });

    it('subscribe(onNext, onError, onComplete) returns Subscription', () => {
        const sub: Subscription = Flux.just(1).subscribe(
            (_n: number) => {},
            (_e: Error) => {},
            () => {},
        );
        sub.unsubscribe();
    });
});

// ─────────────────────────── Mono generics ───────────────────────────────────

describe('Mono — generic type inference', () => {

    // ── factories ─────────────────────────────────────────────────────────────

    it('Mono.just<T> infers element type', () => {
        const m: Mono<number> = Mono.just(42);
        m.subscribe((n: number) => n.toFixed());
    });

    it('Mono.fromPromise<T> infers element type', async () => {
        const m: Mono<string> = Mono.fromPromise(Promise.resolve('hello'));
        await m.toPromise().then((s: string | null) => s?.toUpperCase());
    });

    it('Mono.generate<T> infers element type', () => {
        const m: Mono<boolean> = Mono.generate<boolean>(sink => sink.next(true));
        m.subscribe((b: boolean) => !b);
    });

    // ── map / mapNotNull ───────────────────────────────────────────────────────

    it('map<R> produces Mono<R>', () => {
        const m: Mono<string> = Mono.just(42).map(n => n.toString());
        m.subscribe((s: string) => s.length);
    });

    it('map chain preserves types at each step', () => {
        const m: Mono<boolean> = Mono.just('hello')
            .map(s => s.length)     // Mono<number>
            .map(n => n > 3);       // Mono<boolean>
        m.subscribe((b: boolean) => !b);
    });

    it('mapNotNull<R> produces Mono<NonNullable<R>>', () => {
        const m: Mono<number> = Mono.just('42')
            .mapNotNull(s => s.length > 0 ? parseInt(s) : null);
        m.subscribe((n: number) => n * 2);
    });

    // ── flatMap / flatMapMany ──────────────────────────────────────────────────

    it('flatMap<R> with Mono inner produces Mono<R>', () => {
        const m: Mono<string> = Mono.just(42).flatMap(n => Mono.just(n.toString()));
        m.subscribe((s: string) => s.length);
    });

    it('flatMap<R> with Flux inner produces Mono<R>', () => {
        const m: Mono<string> = Mono.just(42)
            .flatMap(n => Flux.just(n.toString()).first());
        m.subscribe((s: string) => s.length);
    });

    it('flatMap chain: Mono<number> → flatMap → Mono<string> → map → Mono<boolean>', () => {
        const m: Mono<boolean> = Mono.just(5)
            .flatMap(n => Mono.just(n.toString()))  // Mono<string>
            .map(s => s.length > 0);                 // Mono<boolean>
        m.subscribe((b: boolean) => !b);
    });

    it('flatMapMany<R> produces Flux<R>', () => {
        const f: Flux<string> = Mono.just(3)
            .flatMapMany(n => Flux.range(0, n).map(i => `item-${i}`));
        f.subscribe((s: string) => s.startsWith('item'));
    });

    // ── filter ────────────────────────────────────────────────────────────────

    it('filter preserves Mono<T>', () => {
        const m: Mono<number> = Mono.just(42).filter(n => n > 0);
        m.subscribe((n: number) => n.toFixed());
    });

    it('filter after flatMap preserves correct type', () => {
        const m: Mono<string> = Mono.just(5)
            .flatMap(n => Mono.just(n.toString()))
            .filter(s => s.length > 0);
        m.subscribe((s: string) => s.toUpperCase());
    });

    // ── side-effect operators return correct this type ─────────────────────────

    it('doOnNext returns Mono<T>', () => {
        const m: Mono<number> = Mono.just(1).doOnNext(n => n.toFixed());
        m.subscribe((n: number) => n + 1);
    });

    it('publishOn returns Mono<T>', () => {
        const m: Mono<number> = Mono.just(1).publishOn({ schedule: fn => fn() });
        m.subscribe((n: number) => n.toFixed());
    });

    it('doOnNext → flatMap → filter: full chain types preserved', () => {
        const m: Mono<boolean> = Mono.just(5)
            .doOnNext(n => n.toFixed())              // Mono<number>
            .flatMap(n => Mono.just(n % 2 === 0))   // Mono<boolean>
            .filter(b => b !== undefined);            // Mono<boolean>
        m.subscribe((b: boolean) => !b);
    });

    // ── cast / pipe ────────────────────────────────────────────────────────────

    it('cast<R> produces Mono<R>', () => {
        const source: Mono<unknown> = Mono.just(42);
        const m: Mono<number> = source.cast<number>();
        m.subscribe((n: number) => n.toFixed());
    });

    it('pipe<R> produces Mono<R> via custom producer', () => {
        // pipe is a low-level factory: (onNext, onError, onComplete, request, unsubscribe) => Mono<R>
        const m: Mono<string> = Mono.just(42).pipe<string>(
            (onNext, _onError, onComplete) => { onNext('hello'); onComplete(); },
            _n => {},
            () => {},
        );
        m.subscribe((s: string) => s.length);
    });

    // ── zipWith / zipWhen ──────────────────────────────────────────────────────

    it('zipWith<R> produces Mono<[T, R]>', () => {
        const m: Mono<[number, string]> = Mono.just(1).zipWith(Mono.just('a'));
        m.subscribe(([n, s]: [number, string]) => `${n}-${s}`);
    });

    it('zipWhen<R> produces Mono<[T, R]>', () => {
        const m: Mono<[number, string]> = Mono.just(1).zipWhen(n => Mono.just(n.toString()));
        m.subscribe(([n, s]: [number, string]) => `${n}-${s}`);
    });

    // ── hasElement / toPromise ─────────────────────────────────────────────────

    it('hasElement() produces Mono<boolean>', () => {
        const m: Mono<boolean> = Mono.just(42).hasElement();
        m.subscribe((b: boolean) => !b);
    });

    it('toPromise() produces Promise<T | null>', async () => {
        const p: Promise<number | null> = Mono.just(42).toPromise();
        const result: number | null = await p;
        expect(result).toBe(42);
    });

    // ── subscribe overloads return Subscription ────────────────────────────────

    it('subscribe(onNext) returns Subscription with correct T', () => {
        const sub: Subscription = Mono.just(42).subscribe((_n: number) => {});
        sub.unsubscribe();
    });

    it('subscribe(onNext, onError, onComplete) all callbacks typed correctly', () => {
        const sub: Subscription = Mono.just('hello').subscribe(
            (s: string) => s.toUpperCase(),
            (_e: Error) => {},
            () => {},
        );
        sub.unsubscribe();
    });

    it('subscribe(fullSubscriber) onNext typed correctly', () => {
        const sub: Subscription = Mono.just(42).subscribe({
            onSubscribe(s) { s.request(1); },
            onNext(n: number) { n.toFixed(); },
            onError(_e) {},
            onComplete() {}
        });
        sub.unsubscribe();
    });
});
