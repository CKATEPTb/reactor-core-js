import {Sink} from "@/sinks/Sink";
import {Publisher} from "@/publishers";
import {Subscriber, Subscription} from "@/subscriptions";

/**
 * Base class for unicast sinks (one producer → at most one subscriber).
 *
 * Handles the common subscribe() boilerplate:
 *  - Reject a second subscriber with a descriptive error
 *  - Replay terminal signal immediately if already terminated
 *  - Build the Subscription with Rule 3.9 enforcement and demand accumulation
 *
 * Hooks (all no-ops by default, override as needed):
 *  - `shouldReplayTerminalOnSubscribe()` — override to false for buffer-based sinks
 *    that can accept a subscriber after termination and deliver buffered items first
 *  - `onDemandGranted()`  — called after demand is accumulated (e.g. drain)
 *  - `onUnsubscribed()`   — extra cleanup on cancel (e.g. clear buffer)
 *  - `afterSubscribed()`  — called after subscriber.onSubscribe() (e.g. initial drain)
 *  - `onTerminal()`       — how to deliver terminal signal (default: direct; override for drain)
 */
export abstract class AbstractUnicastSink<T> implements Sink<T>, Publisher<T> {

    protected subscriber: Subscriber<T> | null = null;
    protected demand = 0;
    protected cancelled = false;
    protected terminated = false;
    protected terminalError: Error | null = null;

    abstract next(value: T): void;

    // ──── Hooks ──────────────────────────────────────────────────────────────

    /** True → replay terminal immediately for late subscribers. */
    protected shouldReplayTerminalOnSubscribe(): boolean {
        return this.terminated;
    }

    /** Called after demand is accumulated in this.demand. Default: noop. */
    protected onDemandGranted(): void {}

    /** Called after subscriber is cleared on unsubscribe. Default: noop. */
    protected onUnsubscribed(): void {}

    /** Called after subscriber.onSubscribe(sub). Default: noop. */
    protected afterSubscribed(): void {}

    /**
     * How to deliver the terminal signal. Called by both error() and complete().
     * Default: direct onError / onComplete. Override to use drain() instead.
     */
    protected onTerminal(): void {
        if (this.subscriber && !this.cancelled) {
            this.terminalError
                ? this.subscriber.onError(this.terminalError)
                : this.subscriber.onComplete();
        }
    }

    // ──── Terminal signal ─────────────────────────────────────────────────────

    error(error: Error): void {
        if (this.terminated) return;
        this.terminated = true;
        this.terminalError = error;
        this.onTerminal();
    }

    complete(): void {
        if (this.terminated) return;
        this.terminated = true;
        this.onTerminal();
    }

    // ──── Subscribe ───────────────────────────────────────────────────────────

    subscribe(subscriber: Subscriber<T>): Subscription {
        if (this.subscriber !== null) {
            const noop = { request() {}, unsubscribe() {} };
            subscriber.onSubscribe(noop);
            subscriber.onError(new Error(`${this.constructor.name} allows only one subscriber`));
            return noop;
        }
        if (this.shouldReplayTerminalOnSubscribe()) {
            const sub = { request() {}, unsubscribe() {} };
            subscriber.onSubscribe(sub);
            this.terminalError
                ? subscriber.onError(this.terminalError)
                : subscriber.onComplete();
            return sub;
        }

        this.subscriber = subscriber;
        const sub = {
            request: (n: number) => {
                if (this.cancelled) return;
                // Rule 3.9: request(n ≤ 0) MUST signal onError
                if (n <= 0) {
                    this.cancelled = true;
                    const s = this.subscriber;
                    this.subscriber = null;
                    s?.onError(new Error(`request must be > 0, but was ${n}`));
                    return;
                }
                this.demand = Math.min(this.demand + n, Number.MAX_SAFE_INTEGER);
                this.onDemandGranted();
            },
            unsubscribe: () => {
                this.cancelled = true;
                this.subscriber = null;
                this.onUnsubscribed();
            }
        };

        subscriber.onSubscribe(sub);
        this.afterSubscribed();
        return sub;
    }
}
