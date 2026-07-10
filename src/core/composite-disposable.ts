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
    private disposables: Disposable | Set<Disposable> | undefined;

    /** Adds a child disposable unless this composite has already been disposed. */
    public add(disposable: Disposable): boolean {
        if (this.disposed) {
            disposable.dispose();
            return false;
        }
        if (this.disposables === undefined) {
            this.disposables = disposable;
        } else if (this.disposables instanceof Set) {
            this.disposables.add(disposable);
        } else if (this.disposables !== disposable) {
            this.disposables = new Set([this.disposables, disposable]);
        }
        return true;
    }

    /** Removes a child disposable without disposing it. */
    public remove(disposable: Disposable): boolean {
        if (this.disposables === undefined) {
            return false;
        }
        if (this.disposables instanceof Set) {
            const removed = this.disposables.delete(disposable);
            if (this.disposables.size === 0) {
                this.disposables = undefined;
            }
            return removed;
        }
        if (this.disposables !== disposable) {
            return false;
        }
        this.disposables = undefined;
        return true;
    }

    /** Disposes all children and prevents future additions. */
    public dispose(): void {
        if (this.disposed) {
            return;
        }
        this.disposed = true;
        const disposables = this.disposables;
        this.disposables = undefined;
        if (disposables === undefined) {
            return;
        }
        if (disposables instanceof Set) {
            for (const disposable of disposables) {
                disposable.dispose();
            }
            disposables.clear();
            return;
        }
        disposables.dispose();
    }

    /** Returns true after this composite has been disposed. */
    public isDisposed(): boolean {
        return this.disposed;
    }
}
