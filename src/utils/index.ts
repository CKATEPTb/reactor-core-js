import {BackpressurePublisher, Publisher} from "@/publishers/Publisher";
import {Sink} from "@/sinks/Sink";
import {Subscriber, Subscription} from "@/subscriptions";

/**
 * Combines a Sink and a generator function into a single Publisher.
 * Uses the BackpressurePublisher as a base to manage data flow and backpressure.
 *
 * @template T - The type of data being published.
 * @param {Sink<T> & Publisher<T>} sink - The sink that acts as both a data consumer and publisher.
 * @param {Function} generator - A function that generates data and pushes it to the sink.
 * @returns {Publisher<T>} A combined Publisher that handles data emission and backpressure.
 */
export function combine<T>(sink: Sink<T> & Publisher<T>, generator: ((sink: Sink<T>) => void)): Publisher<T> {
    return new class CombinedPublisher extends BackpressurePublisher<T> {
        public override subscribe(subscriber: Subscriber<T>): Subscription {
            const gen = generator(sink) as unknown as Subscription
            const sub = super.subscribe(subscriber)
            const req = typeof gen?.request == 'function'
            return {
                request(count: number) {
                    sub.request(count)
                    !req || gen?.request(count)
                },
                unsubscribe() {
                    sub.unsubscribe()
                    !req || gen?.unsubscribe()
                }
            };
        }
    }(sink)
}