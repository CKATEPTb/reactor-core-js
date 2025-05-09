import {Publisher} from "@/publishers/Publisher";
import {Sink} from "@/sinks/Sink";
import {Subscriber} from "@/subscriptions/Subscriber";
import {Subscription} from "@/subscriptions/Subscription";

/**
 * Represents a backpressure control structure.
 * Stores the subscriber, buffered data, and the number of requested items.
 *
 * @template T - The type of data being processed.
 */
type Backpressure<T> = {
    subscriber: Subscriber<T>
    data: EmitAction<T>[]
    requested: number;
}

/**
 * Represents an action emitted by the sink.
 * Contains the type of emission (next, error, complete) and the associated data.
 *
 * @template T - The type of data being emitted.
 */
export type EmitAction<T> = {
    emit: 'next' | 'error' | 'complete'
    data?: T | Error
}

/**
 * An abstract sink that supports backpressure handling.
 * Manages the flow of data to multiple subscribers while respecting backpressure demands.
 *
 * @template T - The type of data being emitted.
 */
export abstract class BackpressureSink<T> implements Sink<T>, Publisher<T> {
    protected readonly subscribers = new Set<Backpressure<T>>()
    protected completed = false

    /**
     * Subscribes a subscriber to the sink with backpressure handling.
     *
     * @param {Subscriber<T>} subscriber - The subscriber to add.
     * @returns {Subscription} The subscription object for managing the subscriber's lifecycle.
     */
    public subscribe(subscriber: Subscriber<T>): Subscription {
        // if (this.completed) throw new Error('The completed sink is not accepting new subscribers.')
        const backpressure: Backpressure<T> = {
            subscriber: subscriber,
            data: [],
            requested: 0,
        }

        this.subscribers.add(backpressure)

        return {
            /**
             * Requests a specific number of items from the sink.
             * Increments the requested count and triggers data flushing.
             * @param {number} count - The number of items to request.
             */
            request: (count: number) => {
                backpressure.requested += count
                this.flush(backpressure)
            },
            /**
             * Unsubscribes from the sink, clearing the buffered data.
             */
            unsubscribe: () => {
                backpressure.data = []
                this.subscribers.delete(backpressure)
            }
        }
    }

    /**
     * Emits the next value to all subscribers.
     * Validates emission and triggers flushing of buffered data.
     *
     * @param {T} value - The value to emit.
     * @throws {Error} If the sink has already completed.
     */
    public next(value: T): void {
        this.validateEmit()
        for (const subscriber of this.subscribers) {
            subscriber.data.push({emit: 'next', data: value})
            this.flush(subscriber)
        }
    }

    /**
     * Emits an error to all subscribers and marks the sink as completed.
     *
     * @param {Error} error - The error to emit.
     * @throws {Error} If the sink has already completed.
     */
    public error(error: Error): void {
        this.validateEmit()
        for (const subscriber of this.subscribers) {
            subscriber.data.push({emit: 'error', data: error})
            this.flush(subscriber)
        }
    }

    /**
     * Completes the sink, notifying all subscribers.
     * Prevents further emissions and clears the subscriber set.
     */
    public complete(): void {
        if (this.completed) return
        this.completed = true;
        for (const subscriber of this.subscribers) {
            subscriber.data.push({emit: 'complete'})
            this.flush(subscriber)
        }
        this.subscribers.clear()
    }

    /**
     * Emits an action to a specific subscriber.
     * Handles 'next', 'error', and 'complete' emissions.
     *
     * @protected
     * @param {EmitAction<T>} action - The action to be emitted.
     * @param {Subscriber<T>} subscriber - The subscriber to receive the action.
     */
    protected emit(action: EmitAction<T>, subscriber: Subscriber<T>) {
        switch (action.emit) {
            case "next": {
                try {
                    subscriber.onNext(action.data as T)
                } catch (error) {
                    subscriber.onError(error as Error)
                }
                break;
            }
            case "error": {
                subscriber.onError(action.data as Error)
                break;
            }
            case "complete": {
                subscriber.onComplete()
            }
        }
    }

    /**
     * Validates whether the sink can emit new data.
     * Throws an error if the sink has already completed.
     *
     * @protected
     * @throws {Error} If the sink has already completed.
     */
    protected validateEmit() {
        if (this.completed) throw new Error('The completed sink is not accepting new emits.')
    }

    /**
     * Flushes buffered data to the subscriber based on the requested count.
     * Ensures that only the requested number of items are sent.
     *
     * @private
     * @param {Backpressure<T>} backpressure - The backpressure object for the subscriber.
     */
    private flush(backpressure: Backpressure<T>) {
        const data = backpressure.data;
        while (backpressure.requested > 0 && data.length > 0) {
            backpressure.requested--
            this.emit(data.shift() as EmitAction<T>, backpressure.subscriber)
        }
        if (data.length > 0 && data[0].emit == "complete") {
            backpressure.requested++
            this.flush(backpressure)
        }
    }
}