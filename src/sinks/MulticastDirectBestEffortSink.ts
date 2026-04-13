import {AbstractMulticastSink} from "@/sinks/internal/AbstractMulticastSink";
import {Subscriber} from "@/subscriptions";

type Entry = { demand: number; cancelled: boolean };

export default class MulticastDirectBestEffortSink<T> extends AbstractMulticastSink<T, Entry> {

    protected createEntry(): Entry {
        return { demand: 0, cancelled: false };
    }

    next(value: T): void {
        if (this.terminated) return;
        for (const [sub, entry] of this.entries) {
            if (!entry.cancelled && entry.demand > 0) {
                entry.demand--;
                sub.onNext(value);
            }
        }
    }
}
