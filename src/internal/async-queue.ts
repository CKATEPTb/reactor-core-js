/**
 * @packageDocumentation
 * Internal async iteration and cancellation utilities.
 */
import {CancelledError} from "@/errors/index.js";

/** Iterable queue used to bridge push-based producers to async iteration. */
export class AsyncQueue<T> implements AsyncIterable<T> {
    /** Buffered values waiting for an async iterator consumer. */
    private readonly values: T[] = [];
    /** Index of the next buffered value to read. */
    private valueHead = 0;
    /** Pending iterator requests waiting for a value or terminal signal. */
    private readonly waiters: Array<{
        /** Resolves the pending iterator request. */
        resolve: (result: IteratorResult<T>) => void;
        /** Rejects the pending iterator request. */
        reject: (error: unknown) => void;
    }> = [];
    /** Index of the next pending waiter to consume. */
    private waiterHead = 0;
    /** Tracks whether the queue has already reached a terminal state. */
    private closed = false;
    /** Tracks whether the queue has failed, including undefined failures. */
    private failed = false;
    /** Terminal failure delivered to current and future consumers. */
    private failure: unknown;

    /** Creates a queue that is optionally cancelled by an abort signal. */
    public constructor(private readonly signal?: AbortSignal) {
        if (signal) {
            signal.addEventListener(
                "abort",
                () => {
                    this.error(new CancelledError());
                },
                {once: true}
            );
        }
    }

    /** Pushes a value to the queue or a pending consumer. */
    public push(value: T): boolean {
        if (this.closed || this.failed) {
            return false;
        }
        const waiter = this.nextWaiter();
        if (waiter) {
            waiter.resolve({done: false, value});
        } else {
            this.values.push(value);
        }
        return true;
    }

    /** Completes the queue and wakes pending consumers. */
    public complete(): void {
        if (this.closed || this.failed) {
            return;
        }
        this.closed = true;
        for (let index = this.waiterHead; index < this.waiters.length; index += 1) {
            const waiter = this.waiters[index]!;
            waiter.resolve({done: true, value: undefined as T});
        }
        this.clearWaiters();
    }

    /** Fails the queue and wakes pending consumers. */
    public error(error: unknown): void {
        if (this.closed || this.failed) {
            return;
        }
        this.failed = true;
        this.failure = error;
        for (let index = this.waiterHead; index < this.waiters.length; index += 1) {
            const waiter = this.waiters[index]!;
            waiter.reject(this.failure);
        }
        this.clearWaiters();
    }

    /** Returns the number of buffered values. */
    public size(): number {
        return this.values.length - this.valueHead;
    }

    /** Returns this queue as an async iterator. */
    public [Symbol.asyncIterator](): AsyncIterator<T> {
        return this;
    }

    /** Reads the next value or terminal signal. */
    public next(): Promise<IteratorResult<T>> {
        if (this.valueHead < this.values.length) {
            const value = this.values[this.valueHead] as T;
            this.valueHead += 1;
            this.compactValues();
            return Promise.resolve({done: false, value});
        }
        if (this.failed) {
            return Promise.reject(this.failure);
        }
        if (this.closed) {
            return Promise.resolve({done: true, value: undefined as T});
        }
        if (this.signal?.aborted) {
            return Promise.reject(new CancelledError());
        }
        return new Promise((resolve, reject) => {
            this.waiters.push({resolve, reject});
        });
    }

    /** Completes the queue from an async iterator return call. */
    public return(): Promise<IteratorResult<T>> {
        this.complete();
        return Promise.resolve({done: true, value: undefined as T});
    }

    /** Returns the next pending waiter without shifting the backing array. */
    private nextWaiter():
        | {
        /** Resolves the pending iterator request. */
        resolve: (result: IteratorResult<T>) => void;
        /** Rejects the pending iterator request. */
        reject: (error: unknown) => void;
    }
        | undefined {
        if (this.waiterHead >= this.waiters.length) {
            return undefined;
        }
        const waiter = this.waiters[this.waiterHead]!;
        this.waiterHead += 1;
        this.compactWaiters();
        return waiter;
    }

    /** Compacts consumed buffered values after enough reads have accumulated. */
    private compactValues(): void {
        if (this.valueHead === this.values.length) {
            this.values.length = 0;
            this.valueHead = 0;
        } else if (this.valueHead > 1024 && this.valueHead * 2 > this.values.length) {
            this.values.copyWithin(0, this.valueHead);
            this.values.length -= this.valueHead;
            this.valueHead = 0;
        }
    }

    /** Compacts consumed waiters after enough resolved requests have accumulated. */
    private compactWaiters(): void {
        if (this.waiterHead === this.waiters.length) {
            this.clearWaiters();
        } else if (this.waiterHead > 1024 && this.waiterHead * 2 > this.waiters.length) {
            this.waiters.copyWithin(0, this.waiterHead);
            this.waiters.length -= this.waiterHead;
            this.waiterHead = 0;
        }
    }

    /** Clears all pending waiters and resets waiter indexing. */
    private clearWaiters(): void {
        this.waiters.length = 0;
        this.waiterHead = 0;
    }
}
