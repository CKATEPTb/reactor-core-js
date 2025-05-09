import ManySink from "@/sinks/ManySink";
import {EmitAction} from "@/sinks/BackpressureSink";
import {Subscriber} from "@/subscriptions/Subscriber";
import {Subscription} from "@/subscriptions/Subscription";

/**
 * A sink that stores emitted values and replays them to new subscribers.
 * Extends the `ManySink` to support replaying previously emitted events.
 * Useful in scenarios where late subscribers need to receive historical data.
 *
 * @template T - The type of data being emitted.
 */
export abstract class ReplaySink<T> extends ManySink<T> {
    readonly buffer: EmitAction<T>[] = []

    /**
     * Emits a value to all current subscribers and stores it in the buffer for future replay.
     * @param {T} value - The value to emit.
     */
    public override next(value: T): void {
        super.next(value)
        this.store('next', value)
    }

    /**
     * Emits an error to all current subscribers and stores it in the buffer for future replay.
     * @param {Error} error - The error to emit.
     */
    public override error(error: Error) {
        super.error(error);
        this.store('error', error)
    }

    /**
     * Signals the completion of the data stream to all current subscribers.
     * Stores the completion event in the buffer for future replay.
     */
    public override complete() {
        super.complete();
        this.store("complete")
    }

    /**
     * Subscribes a subscriber to the sink.
     * Replays all buffered emits to the new subscriber upon subscription.
     * @param {Subscriber<T>} subscriber - The subscriber to add.
     * @returns {Subscription} The subscription object for managing the subscriber's lifecycle.
     */
    public override subscribe(subscriber: Subscriber<T>): Subscription {
        let left = this.buffer.length
        if(left == 0) return super.subscribe(subscriber)
        const replay = new ManySink<T>()
        const i = replay.subscribe(subscriber)
        for (const action of this.buffer) replay[action.emit](action.data as any)
        const o = super.subscribe({
            onNext: value => {
                replay.next(value)
            },
            onError: error => {
                replay.error(error)
            },
            onComplete: () => {
                replay.complete()
            }
        })
        return {
            request(count: number) {
                i.request(count)
                left = left - count
                if(left < 0) {
                    o.request(left * -1)
                    left = 0
                }
            },
            unsubscribe() {
                i.unsubscribe()
                o.unsubscribe()
            }
        }
    }

    /**
     * Stores an emitted action (next, error, complete) in the buffer.
     * @protected
     * @param {'next' | 'error' | 'complete'} emit - The type of emission.
     * @param {T | Error} [data] - The data associated with the emission, if any.
     */
    protected store(emit: 'next' | 'error' | 'complete', data?: T | Error) {
        this.buffer.push({emit, data})
    }
}