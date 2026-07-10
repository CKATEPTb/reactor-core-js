/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import {AsyncQueue} from "@/internal/index.js";
import {Flux} from "@/publishers/flux.js";
import type {PublisherInput} from "@/publishers/types.js";

declare module "@/publishers/flux.js" {
    /** Flattening and source-combining operators added to Flux. */
    interface Flux<T> {
        /** Reactor flatMap operator. */
        flatMap<R>(mapper: (value: T) => PublisherInput<R>, concurrency?: number): Flux<R>;

        /** Reactor concatMap operator. */
        concatMap<R>(mapper: (value: T) => PublisherInput<R>): Flux<R>;

        /** Reactor switchMap operator. */
        switchMap<R>(mapper: (value: T) => PublisherInput<R>): Flux<R>;

        /** Reactor mergeWith operator. */
        mergeWith(...others: readonly PublisherInput<T>[]): Flux<T>;

        /** Reactor concatWith operator. */
        concatWith(...others: readonly PublisherInput<T>[]): Flux<T>;

        /** Reactor startWith operator. */
        startWith(...values: readonly T[]): Flux<T>;
    }
}

Flux.prototype.flatMap = function flatMap<T, R>(
    this: Flux<T>,
    mapper: (value: T) => PublisherInput<R>,
    concurrency = Number.POSITIVE_INFINITY
): Flux<R> {
    if (concurrency <= 1) {
        return this.concatMap(mapper);
    }
    const source = this;
    return new Flux((signal, context) => {
        const queue = new AsyncQueue<R>(signal);
        const running = new Set<Promise<void>>();
        let failed = false;
        void (async () => {
            try {
                for await (const value of source.iterate(signal, context)) {
                    while (running.size >= concurrency && !signal.aborted) {
                        await Promise.race(running);
                    }
                    if (signal.aborted || failed) {
                        return;
                    }
                    let task: Promise<void> = Promise.resolve();
                    task = (async () => {
                        try {
                            for await (const inner of Flux.from(mapper(value)).iterate(signal, context)) {
                                queue.push(inner);
                            }
                        } catch (error) {
                            failed = true;
                            queue.error(error);
                        } finally {
                            running.delete(task);
                        }
                    })();
                    running.add(task);
                }
                await Promise.all(running);
                if (!failed) {
                    queue.complete();
                }
            } catch (error) {
                queue.error(error);
            }
        })();
        return queue;
    });
};

Flux.prototype.concatMap = function concatMap<T, R>(
    this: Flux<T>,
    mapper: (value: T) => PublisherInput<R>
): Flux<R> {
    const source = this;
    return new Flux(async function* (signal, context) {
        for await (const value of source.iterate(signal, context)) {
            for await (const inner of Flux.from(mapper(value)).iterate(signal, context)) {
                yield inner;
            }
        }
    });
};

Flux.prototype.switchMap = function switchMap<T, R>(
    this: Flux<T>,
    mapper: (value: T) => PublisherInput<R>
): Flux<R> {
    const source = this;
    return new Flux((signal, context) => {
        const queue = new AsyncQueue<R>(signal);
        let active = 0;
        let outerDone = false;
        let failed = false;
        const completeIfDone = () => {
            if (outerDone && active === 0 && !failed) {
                queue.complete();
            }
        };
        void (async () => {
            try {
                let version = 0;
                for await (const value of source.iterate(signal, context)) {
                    version += 1;
                    const current = version;
                    active += 1;
                    void (async () => {
                        try {
                            for await (const inner of Flux.from(mapper(value)).iterate(signal, context)) {
                                if (current === version) {
                                    queue.push(inner);
                                }
                            }
                        } catch (error) {
                            failed = true;
                            queue.error(error);
                        } finally {
                            active -= 1;
                            completeIfDone();
                        }
                    })();
                }
                outerDone = true;
                completeIfDone();
            } catch (error) {
                failed = true;
                queue.error(error);
            }
        })();
        return queue;
    });
};

Flux.prototype.mergeWith = function mergeWith<T>(this: Flux<T>, ...others: readonly PublisherInput<T>[]): Flux<T> {
    return Flux.merge(this, ...others);
};

Flux.prototype.concatWith = function concatWith<T>(this: Flux<T>, ...others: readonly PublisherInput<T>[]): Flux<T> {
    return Flux.concat(this, ...others);
};

Flux.prototype.startWith = function startWith<T>(this: Flux<T>, ...values: readonly T[]): Flux<T> {
    return Flux.concat(Flux.just(...values), this);
};
