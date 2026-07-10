import { describe, expect, it, vi } from "vitest";
import { Flux, type Subscriber, type Subscription } from "@/index.js";

describe("Reactive Streams signal sequence", () => {
  it("logs and enforces subscribe, request and onNext order", async () => {
    const events: string[] = [];
    let subscription: Subscription | undefined;

    Flux.range(1, 3).subscribe(sequenceSubscriber(events, {
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
        log(events, "request(1)");
        nextSubscription.request(1);
      },
      onNext(value) {
        if (value < 3) {
          log(events, "request(1)");
          subscription?.request(1);
        }
      }
    }));

    await vi.waitFor(() => expect(events[events.length - 1]).toBe("onComplete"));
    expect(events).toEqual([
      "onSubscribe",
      "request(1)",
      "onNext(1)",
      "request(1)",
      "onNext(2)",
      "request(1)",
      "onNext(3)",
      "onComplete"
    ]);
    expect(events.findIndex(event => event.startsWith("request"))).toBeLessThan(
      events.findIndex(event => event.startsWith("onNext"))
    );
  });

  it("logs that no value is emitted before explicit request", async () => {
    const events: string[] = [];
    let subscription: Subscription | undefined;

    Flux.range(1, 2).subscribe(sequenceSubscriber(events, {
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      }
    }));

    await new Promise(resolve => setTimeout(resolve, 10));
    expect(events).toEqual(["onSubscribe"]);

    log(events, "request(2)");
    subscription?.request(2);

    await vi.waitFor(() => expect(events[events.length - 1]).toBe("onComplete"));
    expect(events).toEqual(["onSubscribe", "request(2)", "onNext(1)", "onNext(2)", "onComplete"]);
  });
});

function sequenceSubscriber<T>(
  events: string[],
  hooks: {
    onSubscribe?(subscription: Subscription): void;
    onNext?(value: T): void;
  } = {}
): Subscriber<T> {
  return {
    onSubscribe(subscription) {
      log(events, "onSubscribe");
      hooks.onSubscribe?.(subscription);
    },
    onNext(value) {
      log(events, `onNext(${String(value)})`);
      hooks.onNext?.(value);
    },
    onError(error) {
      log(events, `onError(${String(error)})`);
      throw error;
    },
    onComplete() {
      log(events, "onComplete");
    }
  };
}

function log(events: string[], event: string): void {
  events.push(event);
  console.log(`[reactive-sequence] ${event}`);
}
