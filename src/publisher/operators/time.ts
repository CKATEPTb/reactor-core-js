/**
 * @packageDocumentation
 * Flux, Mono and operator implementation modules.
 */
import {TimeoutError} from "@/errors/classes.js";
import {AsyncQueue} from "@/internal/async-queue.js";
import {toAsyncIterator} from "@/internal/iterable.js";
import {Flux} from "@/publisher/flux.js";
import {raceIteratorWithTimeout, scheduleDelay, TIMEOUT} from "@/publisher/helpers.js";
import type {PublisherInput} from "@/publisher/types.js";
import {Schedulers} from "@/schedulers/schedulers.js";
import type {DurationInput, Scheduler} from "@/schedulers/types.js";

declare module "@/publisher/flux.js" {
    /** Time, scheduling and timeout operators added to Flux. */
    interface Flux<T> {
        /** Delays each source value on the provided scheduler before emitting it. */
        delayElements(delay: DurationInput, scheduler?: Scheduler): Flux<T>;

        /** Fails or switches to a fallback when the source does not signal in time. */
        timeout(timeout: DurationInput, fallback?: PublisherInput<T>, scheduler?: Scheduler): Flux<T>;

        /** Reschedules value delivery on the provided scheduler. */
        publishOn(scheduler: Scheduler): Flux<T>;

        /** Schedules subscription work on the provided scheduler. */
        subscribeOn(scheduler: Scheduler): Flux<T>;

        /** Emits each value together with the scheduler timestamp. */
        timestamp(scheduler?: Scheduler): Flux<readonly [number, T]>;

        /** Emits each value together with elapsed scheduler time since the previous value. */
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
