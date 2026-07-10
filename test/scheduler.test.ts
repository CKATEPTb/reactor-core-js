import { describe, expect, it } from "vitest";
import {
  AnimationFrameScheduler,
  ImmediateScheduler,
  MicrotaskScheduler,
  Schedulers,
  TimeoutScheduler
} from "@/scheduler/index.js";

describe("Schedulers", () => {
  it("uses dedicated scheduler implementations", () => {
    expect(Schedulers.immediate()).toBeInstanceOf(ImmediateScheduler);
    expect(Schedulers.microtask()).toBeInstanceOf(MicrotaskScheduler);
    expect(Schedulers.timeout()).toBeInstanceOf(TimeoutScheduler);
    expect(Schedulers.animationFrame()).toBeInstanceOf(AnimationFrameScheduler);
    expect(Schedulers.newSingle()).toBeInstanceOf(MicrotaskScheduler);
    expect(Schedulers.newBoundedElastic()).toBeInstanceOf(TimeoutScheduler);
  });
});
