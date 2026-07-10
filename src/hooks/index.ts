/**
 * @packageDocumentation
 * Global hook facade used for dropped signal and operator error callbacks.
 */
/** Hook invoked when an error is dropped after the downstream can no longer observe it. */
export type ErrorHook = (error: unknown) => void;
/** Hook invoked when an operator error can be replaced or decorated. */
export type OperatorErrorHook = (error: unknown, data?: unknown) => unknown;

/** Currently registered dropped-error callback. */
let onErrorDroppedHook: ErrorHook | undefined;
/** Currently registered dropped-next callback. */
let onNextDroppedHook: ((value: unknown) => void) | undefined;
/** Currently registered operator-error callback. */
let onOperatorErrorHook: OperatorErrorHook | undefined;

/** Global hook registry for dropped signals and operator errors. */
export const Hooks = {
    /** Registers a callback for errors that are dropped after termination. */
    onErrorDropped(hook: ErrorHook): void {
        onErrorDroppedHook = hook;
    },

    /** Clears the dropped-error callback. */
    resetOnErrorDropped(): void {
        onErrorDroppedHook = undefined;
    },

    /** Registers a callback for values dropped after termination. */
    onNextDropped(hook: (value: unknown) => void): void {
        onNextDroppedHook = hook;
    },

    /** Clears the dropped-next callback. */
    resetOnNextDropped(): void {
        onNextDroppedHook = undefined;
    },

    /** Registers a callback used to transform operator errors. */
    onOperatorError(hook: OperatorErrorHook): void {
        onOperatorErrorHook = hook;
    },

    /** Clears the operator-error callback. */
    resetOnOperatorError(): void {
        onOperatorErrorHook = undefined;
    },

    /** Notifies the currently registered dropped-error callback. */
    dropError(error: unknown): void {
        onErrorDroppedHook?.(error);
    },

    /** Notifies the currently registered dropped-next callback. */
    dropNext(value: unknown): void {
        onNextDroppedHook?.(value);
    },

    /** Applies the operator-error callback when one is registered. */
    operatorError(error: unknown, data?: unknown): unknown {
        return onOperatorErrorHook ? onOperatorErrorHook(error, data) : error;
    }
};
