/**
 * @packageDocumentation
 * Reactive Streams core contracts and disposable helpers.
 */
import type {Disposable} from "@/core/types.js";

/** Disposable container that releases all registered children together. */
export class CompositeDisposable implements Disposable {
    /** Tracks whether this composite has already been disposed. */
    private disposed = false;
    /** Child disposables still owned by this composite. */
    private readonly disposables = new Set<Disposable>();

    /** Adds a child disposable unless this composite has already been disposed. */
    public add(disposable: Disposable): boolean {
        if (this.disposed) {
            disposable.dispose();
            return false;
        }
        this.disposables.add(disposable);
        return true;
    }

    /** Removes a child disposable without disposing it. */
    public remove(disposable: Disposable): boolean {
        return this.disposables.delete(disposable);
    }

    /** Disposes all children and prevents future additions. */
    public dispose(): void {
        if (this.disposed) {
            return;
        }
        this.disposed = true;
        for (const disposable of this.disposables) {
            disposable.dispose();
        }
        this.disposables.clear();
    }

    /** Returns true after this composite has been disposed. */
    public isDisposed(): boolean {
        return this.disposed;
    }
}
