import {Scheduler} from "@/schedulers/Scheduler";

export class MicroScheduler implements Scheduler {
    public schedule(task: () => void): void {
        Promise.resolve().then(task)
    }
}
