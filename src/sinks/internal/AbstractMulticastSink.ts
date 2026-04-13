import {Sink} from "@/sinks/Sink";
import {Publisher} from "@/publishers";
import {Subscriber, Subscription} from "@/subscriptions";

/**
 * Base class for all multicast sinks (one-to-many, Map-based subscriber tracking).
 *
 * Subclasses provide:
 *  - `createEntry()` — constructs a fresh per-subscriber state object
 *  - `next(value)`   — emission logic (differs per delivery strategy)
 *
 * Hooks (all no-ops by default, override as needed):
 *  - `shouldReplayTerminalOnSubscribe()` — when to replay terminal immediately on subscribe
 *  - `onDemandGranted(sub, entry)`       — called after demand is accumulated (e.g. drain)
 *  - `onUnsubscribed(sub, entry)`        — extra cleanup on cancel (e.g. autoCancel logic)
 *  - `onSubscribed(sub, entry)`          — called after subscriber.onSubscribe() (e.g. initial drain)
 *  - `deliverError(sub, entry)`          — how to signal error to one entry (default: direct)
 *  - `deliverComplete(sub, entry)`       — how to signal complete to one entry (default: direct)
 *  - `clearEntriesAfterComplete()`       — false for drain-based sinks (drainEntry cleans up)
 *
 * E must have at least `{ cancelled: boolean; demand: number }`.
 */
export abstract class AbstractMulticastSink<T, E extends { cancelled: boolean; demand: number }>
    implements Sink<T>, Publisher<T> {

    protected readonly entries = new Map<Subscriber<T>, E>();
    protected terminated = false;
    protected terminalError: Error | null = null;

    abstract next(value: T): void;
    protected abstract createEntry(): E;

    // ──── Hooks ──────────────────────────────────────────────────────────────

    /** True → replay terminal signal immediately when subscriber joins. */
    protected shouldReplayTerminalOnSubscribe(): boolean {
        return this.terminated;
    }

    /** Called after demand is accumulated in entry.demand. Default: noop. */
    protected onDemandGranted(_sub: Subscriber<T>, _entry: E): void {}

    /** Called after entry removal on unsubscribe. Default: noop. */
    protected onUnsubscribed(_sub: Subscriber<T>, _entry: E): void {}

    /** Called after subscriber.onSubscribe(sub). Default: noop. */
    protected onSubscribed(_sub: Subscriber<T>, _entry: E): void {}

    /** How to deliver error to one entry. Default: direct onError. */
    protected deliverError(sub: Subscriber<T>, entry: E): void {
        if (!entry.cancelled) sub.onError(this.terminalError!);
    }

    /** How to deliver complete to one entry. Default: direct onComplete. */
    protected deliverComplete(sub: Subscriber<T>, entry: E): void {
        if (!entry.cancelled) sub.onComplete();
    }

    /**
     * Whether to call entries.clear() after iterating in complete().
     * Override to false for drain-based sinks — drainEntry deletes entries individually.
     */
    protected clearEntriesAfterComplete(): boolean {
        return true;
    }

    // ──── Terminal signal ─────────────────────────────────────────────────────

    error(error: Error): void {
        if (this.terminated) return;
        this.terminated = true;
        this.terminalError = error;
        for (const [sub, entry] of this.entries) {
            this.deliverError(sub, entry);
        }
        this.entries.clear();
    }

    complete(): void {
        if (this.terminated) return;
        this.terminated = true;
        for (const [sub, entry] of this.entries) {
            this.deliverComplete(sub, entry);
        }
        if (this.clearEntriesAfterComplete()) this.entries.clear();
    }

    // ──── Subscribe ───────────────────────────────────────────────────────────

    subscribe(subscriber: Subscriber<T>): Subscription {
        if (this.shouldReplayTerminalOnSubscribe()) {
            const sub = { request() {}, unsubscribe() {} };
            subscriber.onSubscribe(sub);
            this.terminalError
                ? subscriber.onError(this.terminalError)
                : subscriber.onComplete();
            return sub;
        }

        const entry = this.createEntry();
        this.entries.set(subscriber, entry);

        const sub = {
            request: (n: number) => {
                if (entry.cancelled) return;
                // Rule 3.9: request(n ≤ 0) MUST signal onError
                if (n <= 0) {
                    entry.cancelled = true;
                    this.entries.delete(subscriber);
                    subscriber.onError(new Error(`request must be > 0, but was ${n}`));
                    return;
                }
                entry.demand = Math.min(entry.demand + n, Number.MAX_SAFE_INTEGER);
                this.onDemandGranted(subscriber, entry);
            },
            unsubscribe: () => {
                entry.cancelled = true;
                this.entries.delete(subscriber);
                this.onUnsubscribed(subscriber, entry);
            }
        };

        subscriber.onSubscribe(sub);
        this.onSubscribed(subscriber, entry);
        return sub;
    }
}
