/**
 * @packageDocumentation
 * Reactive Streams core contracts and disposable helpers.
 */
import {BooleanDisposable} from "@/core/boolean-disposable.js";
import {CompositeDisposable} from "@/core/composite-disposable.js";
import type {Disposable} from "@/core/types.js";

/** Shared no-op disposable reused for already disposed results. */
const DISPOSED: Disposable = Object.freeze({
    /** Ignores disposal because the disposable is already disposed. */
    dispose() {
        // already disposed
    },
    /** Always reports that this disposable has been disposed. */
    isDisposed() {
        return true;
    }
});

/** Factory helpers for disposable values used by the runtime. */
export const Disposables = {
    /** Returns an already disposed disposable. */
    disposed(): Disposable {
        return DISPOSED;
    },

    /** Creates a disposable backed by an optional teardown callback. */
    single(teardown?: () => void): Disposable {
        return new BooleanDisposable(teardown);
    },

    /** Creates a composite disposable and adds the provided disposables. */
    composite(...disposables: Disposable[]): CompositeDisposable {
        const composite = new CompositeDisposable();
        for (const disposable of disposables) {
            composite.add(disposable);
        }
        return composite;
    }
};
