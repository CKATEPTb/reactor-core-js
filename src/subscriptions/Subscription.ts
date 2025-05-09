export interface Subscription {
    request(count: number): void

    unsubscribe(): void
}