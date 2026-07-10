import { afterEach, describe, expect, it, vi } from "vitest";
import { AnimationFrameScheduler } from "@/schedulers/animation-frame-scheduler.js";
import { DelayScheduler } from "@/schedulers/delay-scheduler.js";
import { ImmediateScheduler } from "@/schedulers/immediate-scheduler.js";
import { IntervalScheduler } from "@/schedulers/interval-scheduler.js";
import { MacroScheduler } from "@/schedulers/macro-scheduler.js";
import { MicrotaskScheduler } from "@/schedulers/microtask-scheduler.js";
import { Schedulers } from "@/schedulers/index.js";

describe("Schedulers", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("uses dedicated scheduler implementations", () => {
    expect(Object.isFrozen(Schedulers)).toBe(true);
    expect(Schedulers.immediate()).toBeInstanceOf(ImmediateScheduler);
    expect(Schedulers.micro()).toBeInstanceOf(MicrotaskScheduler);
    expect(Schedulers.macro()).toBeInstanceOf(MacroScheduler);
    expect(Schedulers.delay(1)).toBeInstanceOf(DelayScheduler);
    expect(Schedulers.interval(1)).toBeInstanceOf(IntervalScheduler);
    expect(Schedulers.microtask()).toBeInstanceOf(MicrotaskScheduler);
    expect(Schedulers.timeout()).toBeInstanceOf(MacroScheduler);
    expect(Schedulers.animationFrame()).toBeInstanceOf(AnimationFrameScheduler);
    expect(Schedulers.newSingle()).toBeInstanceOf(MicrotaskScheduler);
    expect(Schedulers.newBoundedElastic()).toBeInstanceOf(MacroScheduler);
  });

  it("keeps Reactor-style scheduler names as aliases for browser primitives", () => {
    expect(Schedulers.microtask()).toBe(Schedulers.micro());
    expect(Schedulers.single()).toBe(Schedulers.micro());
    expect(Schedulers.parallel()).toBe(Schedulers.micro());
    expect(Schedulers.timeout()).toBe(Schedulers.macro());
    expect(Schedulers.boundedElastic()).toBe(Schedulers.macro());
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

  it("uses configured timing defaults for delay and interval schedulers", () => {
    vi.useFakeTimers();
    const delayed = vi.fn();
    const periodic = vi.fn();

    Schedulers.delay(25).schedule(delayed);
    vi.advanceTimersByTime(24);
    expect(delayed).not.toHaveBeenCalled();

    vi.advanceTimersByTime(1);
    expect(delayed).toHaveBeenCalledTimes(1);

    const disposable = Schedulers.interval(10).schedulePeriodically(periodic);
    vi.advanceTimersByTime(9);
    expect(periodic).not.toHaveBeenCalled();

    vi.advanceTimersByTime(1);
    expect(periodic).toHaveBeenCalledTimes(1);

    vi.advanceTimersByTime(10);
    expect(periodic).toHaveBeenCalledTimes(2);

    disposable.dispose();
    vi.advanceTimersByTime(50);
    expect(periodic).toHaveBeenCalledTimes(2);
  });
});
