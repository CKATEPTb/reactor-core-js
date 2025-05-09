import {Scheduler} from '@/schedulers/Scheduler'

export class ImmediateScheduler implements Scheduler {
    public schedule(task: () => void): void {
        task()
    }
}
