import ManySink from "@/sinks/ManySink";
import {EmitAction} from "@/sinks/BackpressureSink";
import {Subscriber} from "@/subscriptions/Subscriber";
import {Subscription} from "@/subscriptions/Subscription";

export abstract class ReplaySink<T> extends ManySink<T> {
    readonly buffer: EmitAction<T>[] = []

    public override next(value: T): void {
        super.next(value)
        this.store('next', value)
    }

    public override error(error: Error) {
        super.error(error);
        this.store('error', error)
    }

    public override complete() {
        super.complete();
        this.store("complete")
    }

    public override subscribe(subscriber: Subscriber<T>): Subscription {
        const subscription = super.subscribe(subscriber)
        this.replay(subscriber)
        return subscription
    }

    protected replay(subscriber: Subscriber<T>) {
        for (const action of this.buffer) {
            this.emit(action, subscriber)
        }
    }

    protected store(emit: 'next' | 'error' | 'complete', data?: T | Error) {
        this.buffer.push({emit, data})
    }
}