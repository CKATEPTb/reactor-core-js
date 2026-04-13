import {AbstractUnicastSink} from "@/sinks/internal/AbstractUnicastSink";

export default class UnicastOnBackpressureBufferSink<T> extends AbstractUnicastSink<T> {

    private readonly buffer: T[] = [];
    // Rule 3.16: guard against re-entrant drain (onNext → request → drain)
    private draining = false;

    // ──── AbstractUnicastSink hooks ───────────────────────────────────────────

    /** Allow late subscribers — they receive buffered items then the terminal signal. */
    protected shouldReplayTerminalOnSubscribe(): boolean {
        return false;
    }

    /** Use drain() for both error and complete so buffered items are delivered first. */
    protected onTerminal(): void {
        this.drain();
    }

    protected onDemandGranted(): void {
        this.drain();
    }

    protected onUnsubscribed(): void {
        this.buffer.length = 0;
    }

    protected afterSubscribed(): void {
        this.drain();
    }

    // ──── Sink.next ───────────────────────────────────────────────────────────

    next(value: T): void {
        if (this.terminated || this.cancelled) return;
        this.buffer.push(value);
        this.drain();
    }

    // ──── Internal drain ──────────────────────────────────────────────────────

    private drain(): void {
        if (!this.subscriber || this.cancelled || this.draining) return;
        this.draining = true;
        try {
            while (this.demand > 0 && this.buffer.length > 0) {
                this.demand--;
                this.subscriber.onNext(this.buffer.shift()!);
                if (this.cancelled) return;
            }
            if (this.buffer.length === 0 && this.terminated) {
                const s = this.subscriber;
                this.subscriber = null;
                this.terminalError ? s.onError(this.terminalError) : s.onComplete();
            }
        } finally {
            this.draining = false;
        }
    }
}
