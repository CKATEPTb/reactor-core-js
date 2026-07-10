import { describe, expect, it } from "vitest";
import { AnimationFrameScheduler } from "@/schedulers/animation-frame-scheduler.js";
import { ImmediateScheduler } from "@/schedulers/immediate-scheduler.js";
import { MicrotaskScheduler } from "@/schedulers/microtask-scheduler.js";
import { Schedulers } from "@/schedulers/index.js";
import { TimeoutScheduler } from "@/schedulers/timeout-scheduler.js";

describe("Schedulers", () => {
  it("uses dedicated scheduler implementations", () => {
    expect(Object.isFrozen(Schedulers)).toBe(true);
    expect(Schedulers.immediate()).toBeInstanceOf(ImmediateScheduler);
    expect(Schedulers.microtask()).toBeInstanceOf(MicrotaskScheduler);
    expect(Schedulers.timeout()).toBeInstanceOf(TimeoutScheduler);
    expect(Schedulers.animationFrame()).toBeInstanceOf(AnimationFrameScheduler);
    expect(Schedulers.newSingle()).toBeInstanceOf(MicrotaskScheduler);
    expect(Schedulers.newBoundedElastic()).toBeInstanceOf(TimeoutScheduler);
  });

  it("accepts structured duration inputs on delayed scheduler paths", async () => {
    const events: string[] = [];

    await new Promise<void>(resolve => {
      Schedulers.immediate().schedule(() => {
        events.push("scheduled");
        resolve();
      }, { milliseconds: 1 });
      events.push("after");
    });

    expect(events).toEqual(["after", "scheduled"]);
  });
});
