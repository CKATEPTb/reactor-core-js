export interface Subscriber<T> {
    onNext(value: T): void

    onError(error: Error): void

    onComplete(): void
}