import {BackpressurePublisher, Publisher} from "@/publishers/Publisher";
import {Sink} from "@/sinks/Sink";
import {Subscriber, Subscription} from "@/subscriptions";
import {ReplayLatestSink} from "@/sinks";
import {Flux} from "@/publishers";

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

/**
 * Creates a reactive subject that holds a single mutable value and supports subscriptions.
 * The subject allows updating the value and notifying subscribers of changes.
 *
 * @template T - The type of the value held by the subject.
 * @param {T} value - The initial value of the subject.
 * @returns {Object} An object with methods to interact with the subject.
 * @property {function(T): void} next - Updates the current value and notifies subscribers.
 * @property {function((current: T) => T): void} update - Updates the current value using a function and notifies subscribers.
 * @property {function(): T} get - Returns the current value of the subject.
 * @property {function(Subscriber<T>): Subscription} subscribe - Subscribes to changes and returns a subscription.
 *
 * @example
 * const count = subject(0);
 * count.next(1);  // Update the value to 1
 * count.update(prev => prev + 1);  // Increment the value
 * console.log(count.get());  // Output: 2
 * const subscription = count.subscribe({
 *   onNext: value => console.log('New value:', value),
 *   onComplete: () => console.log('Completed')
 * });
 * subscription.request(1);  // Request the next value
 * subscription.unsubscribe();  // Stop receiving updates
 */
export function subject<T>(value: T) {
    let subject = value;
    const sink = new ReplayLatestSink<T>(1)
    sink.next(value)
    return {
        next(value: T) {
            sink.next(subject = value)
        },
        update(fn: (current: T) => T) {
            this.next(fn(subject))
        },
        get() {
            return subject
        },
        subscribe({
                      onNext = (value: T) => {
                      },
                      onError = (error: Error) => {
                      },
                      onComplete = () => {
                      }
                  } = {}): Subscription {
            return Flux.from(sink).distinctUntilChanged().subscribe({onNext, onError, onComplete})
        }
    }
}