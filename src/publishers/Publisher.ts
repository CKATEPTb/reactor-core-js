import {EmitAction} from "@/sinks/BackpressureSink";
import {Subscriber} from "@/subscriptions/Subscriber";
import {Subscription} from "@/subscriptions/Subscription";

export interface Publisher<T> {
    subscribe(subscriber: Subscriber<T>): Subscription
}

export class BackpressurePublisher<T> implements Publisher<T> {
    private backpressure: Array<EmitAction<T>> = []
    private subscriber?: Subscriber<T>
    private requested: number = 0;
    private subscription: Subscription

    public constructor(sink: Publisher<T>) {
        this.subscription = sink.subscribe({
            onNext: (value: T) => {
                this.backpressure.push({emit: 'next', data: value})
                this.flush()
            },
            onError: (error: Error) => {
                this.backpressure.push({emit: 'error', data: error})
                this.flush()
            },
            onComplete: () => {
                this.subscription.unsubscribe()
                this.backpressure.push({emit: 'complete'})
                this.flush()
            }
        })
        this.subscription.request(Number.MAX_SAFE_INTEGER)
    }

    public subscribe(subscriber: Subscriber<T>): Subscription {
        if (this.subscriber != null) throw new Error("Backpressure unicast publisher is not accepting new subscribers")
        this.subscriber = subscriber;
        return {
            request: (count: number) => {
                this.requested += count
                this.flush()
            },
            unsubscribe: () => {
                this.backpressure = []
                this.subscription.unsubscribe()
            }
        };
    }

    private emit(action: EmitAction<T>) {
        switch (action.emit) {
            case "next": {
                try {
                    this.subscriber?.onNext(action.data as T)
                } catch (error) {
                    this.subscriber?.onError(error as Error)
                }
                break;
            }
            case "error": {
                this.subscriber?.onError(action.data as Error)
                break;
            }
            case "complete": {
                this.subscriber?.onComplete()
            }
        }
    }

    private flush() {
        while (this.requested > 0 && this.backpressure.length > 0) {
            this.requested--
            this.emit(this.backpressure.shift() as EmitAction<T>)
        }
        if (this.subscriber != null && this.backpressure[0]?.emit == 'complete') {
            this.emit(this.backpressure.shift() as EmitAction<T>)
        }
    }

}