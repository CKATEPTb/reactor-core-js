/**
 * @packageDocumentation
 * Reactive Streams core contracts and disposable helpers.
 */
import type {Disposable} from "@/core/types.js";

/** Simple disposable backed by an optional cleanup callback. */
export class BooleanDisposable implements Disposable {
    /** Tracks whether disposal already happened. */
    private disposed = false;
    /** Optional cleanup callback invoked on first disposal. */
    private readonly teardown: (() => void) | undefined;

    /** Creates a disposable that runs `teardown` once when disposed. */
    public constructor(teardown?: () => void) {
        this.teardown = teardown;
    }

    /** Releases this disposable and runs its teardown callback once. */
    public dispose(): void {
        if (this.disposed) {
            return;
        }
        this.disposed = true;
        this.teardown?.();
    }

    /** Returns true once this disposable has been released. */
    public isDisposed(): boolean {
        return this.disposed;
    }
}
