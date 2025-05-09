import {ImmediateScheduler} from "@/schedulers/ImmediateScheduler";
import {MicroScheduler} from "@/schedulers/MicroScheduler";
import {MacroScheduler} from "@/schedulers/MacroScheduler";
import {DelayScheduler} from "@/schedulers/DelayScheduler";

export * from '@/schedulers/Scheduler'

export const Schedulers = {
    immediate: () => new ImmediateScheduler(),
    micro: () => new MicroScheduler(),
    macro: () => new MacroScheduler(),
    delay: (ms: number): DelayScheduler => new DelayScheduler(ms)
}