export interface Scheduler {
    schedule(task: () => void): void
}

export interface CancellableScheduler extends Scheduler {
    schedule(task: () => void): { cancel: () => void }
}
