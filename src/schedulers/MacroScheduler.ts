import {Scheduler} from "@/schedulers/Scheduler";

export class MacroScheduler implements Scheduler {
    public schedule(task: () => void): void {
        setTimeout(task, 0)
    }
}
