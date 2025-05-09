import {BackpressurePublisher, Publisher} from "@/publishers/Publisher";
import {Sink} from "@/sinks/Sink";
import {Subscriber, Subscription} from "@/subscriptions";

export function combine<T>(sink: Sink<T> & Publisher<T>, generator: ((sink: Sink<T>) => void)): Publisher<T> {
    return new class CombinedPublisher extends BackpressurePublisher<T> {
        public override subscribe(subscriber: Subscriber<T>): Subscription {
            const gen = generator(sink) as unknown as Subscription
            const sub = super.subscribe(subscriber)
            return {
                request(count: number) {
                    sub.request(count)
                    gen?.request(count)
                },
                unsubscribe() {
                    sub.unsubscribe()
                    gen?.unsubscribe()
                }
            };
        }
    }(sink)
}