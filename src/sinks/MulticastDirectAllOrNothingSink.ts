import {AbstractMulticastSink} from "@/sinks/internal/AbstractMulticastSink";

type Entry = { demand: number; cancelled: boolean };

export default class MulticastDirectAllOrNothingSink<T> extends AbstractMulticastSink<T, Entry> {

    protected createEntry(): Entry {
        return { demand: 0, cancelled: false };
    }

    next(value: T): void {
        if (this.terminated) return;
        const active = [...this.entries.values()].filter(e => !e.cancelled);
        if (active.length === 0 || !active.every(e => e.demand > 0)) return;
        for (const [sub, entry] of this.entries) {
            if (!entry.cancelled) {
                entry.demand--;
                sub.onNext(value);
            }
        }
    }
}
