/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import {TimeoutError} from "@/errors/index.js";
import {AsyncQueue, toAsyncIterator} from "@/internal/index.js";
import {type DurationInput, type Scheduler, Schedulers} from "@/scheduler/index.js";
import {Flux} from "@/publishers/flux.js";
import {raceIteratorWithTimeout, scheduleDelay, TIMEOUT} from "@/publishers/helpers.js";
import type {PublisherInput} from "@/publishers/types.js";

declare module "@/publishers/flux.js" {
    /** Time, scheduling and timeout operators added to Flux. */
    interface Flux<T> {
        /** Reactor delayElements operator. */
        delayElements(delay: DurationInput, scheduler?: Scheduler): Flux<T>;

        /** Reactor timeout operator. */
        timeout(timeout: DurationInput, fallback?: PublisherInput<T>, scheduler?: Scheduler): Flux<T>;

        /** Reactor publishOn operator. */
        publishOn(scheduler: Scheduler): Flux<T>;

        /** Reactor subscribeOn operator. */
        subscribeOn(scheduler: Scheduler): Flux<T>;

        /** Reactor timestamp operator. */
        timestamp(scheduler?: Scheduler): Flux<readonly [number, T]>;

        /** Reactor elapsed operator. */
        elapsed(scheduler?: Scheduler): Flux<readonly [number, T]>;
    }
}

Flux.prototype.delayElements = function delayElements<T>(
    this: Flux<T>,
    delay: DurationInput,
    scheduler: Scheduler = Schedulers.timeout()
): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        for await (const value of source.iterate(signal, context)) {
            await scheduleDelay(scheduler, delay, signal);
            yield value;
        }
    });
};

Flux.prototype.timeout = function timeout<T>(
    this: Flux<T>,
    timeout: DurationInput,
    fallback?: PublisherInput<T>,
    scheduler: Scheduler = Schedulers.timeout()
): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        const controller = new AbortController();
        const abort = () => controller.abort();
        signal.addEventListener("abort", abort, {once: true});
        const iterator = toAsyncIterator(source.iterate(controller.signal, context));
        try {
            while (!signal.aborted) {
                const result = await raceIteratorWithTimeout(iterator, timeout, scheduler, signal);
                if (result === TIMEOUT) {
                    controller.abort();
                    if (fallback) {
                        for await (const value of Flux.from(fallback).iterate(signal, context)) {
                            yield value;
                        }
                        return;
                    }
                    throw new TimeoutError();
                }
                if (result.done) {
                    return;
                }
                yield result.value;
            }
        } finally {
            signal.removeEventListener("abort", abort);
            controller.abort();
            void iterator.return?.();
        }
    });
};

Flux.prototype.publishOn = function publishOn<T>(this: Flux<T>, scheduler: Scheduler): Flux<T> {
    const source = this;
    return new Flux(async function* (signal, context) {
        for await (const value of source.iterate(signal, context)) {
            await scheduleDelay(scheduler, 0, signal);
            yield value;
        }
    });
};

Flux.prototype.subscribeOn = function subscribeOn<T>(this: Flux<T>, scheduler: Scheduler): Flux<T> {
    const source = this;
    return new Flux((signal, context) => {
        const queue = new AsyncQueue<T>(signal);
        const disposable = scheduler.schedule(() => {
            void (async () => {
                try {
                    for await (const value of source.iterate(signal, context)) {
                        queue.push(value);
                    }
                    queue.complete();
                } catch (error) {
                    queue.error(error);
                }
            })();
        });
        signal.addEventListener("abort", () => disposable.dispose(), {once: true});
        return queue;
    });
};

Flux.prototype.timestamp = function timestamp<T>(
    this: Flux<T>,
    scheduler: Scheduler = Schedulers.timeout()
): Flux<readonly [number, T]> {
    return this.map(value => [scheduler.now(), value] as const);
};

Flux.prototype.elapsed = function elapsed<T>(
    this: Flux<T>,
    scheduler: Scheduler = Schedulers.timeout()
): Flux<readonly [number, T]> {
    const source = this;
    return new Flux(async function* (signal, context) {
        let last = scheduler.now();
        for await (const value of source.iterate(signal, context)) {
            const now = scheduler.now();
            yield [now - last, value] as const;
            last = now;
        }
    });
};
