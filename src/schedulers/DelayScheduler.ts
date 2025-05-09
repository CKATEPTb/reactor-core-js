import {CancellableScheduler} from "@/schedulers/Scheduler";

export class DelayScheduler implements CancellableScheduler {
    private readonly delay: number

    constructor(delay: number) {
        this.delay = delay
    }

    public schedule(task: () => void): { cancel: () => void } {
        const id = setTimeout(task, this.delay)
        return {
            cancel: () => clearTimeout(id)
        }
    }
}
