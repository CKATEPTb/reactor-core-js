/**
 * @packageDocumentation
 * Reactive Streams core contracts and disposable helpers.
 */
/** Disposable resource with idempotent cleanup semantics. */
export interface Disposable {
    /** Releases the resource. Calling this more than once is allowed. */
    dispose(): void;

    /** Returns true after this disposable has been released. */
    isDisposed(): boolean;
}

/** Supported teardown representation for internal disposable adapters. */
export type Teardown = Disposable | (() => void) | void;
