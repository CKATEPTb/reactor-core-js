/**
 * @packageDocumentation
 * Materialized signal representation and signal kinds.
 */
import {SignalType} from "@/signal/signal-type.js";

/** Immutable representation of a Reactor signal. */
export class Signal<T> {
    /** Signal kind represented by this instance. */
    public readonly type: SignalType;
    /** Value carried by onNext signals. */
    public readonly value: T | undefined;
    /** Error carried by onError signals. */
    public readonly error: unknown;

    /** Creates an immutable signal value. */
    private constructor(type: SignalType, value?: T, error?: unknown) {
        this.type = type;
        this.value = value;
        this.error = error;
    }

    /** Creates an onNext signal. */
    public static next<T>(value: T): Signal<T> {
        return new Signal(SignalType.NEXT, value);
    }

    /** Creates an onError signal. */
    public static error<T = never>(error: unknown): Signal<T> {
        return new Signal<T>(SignalType.ERROR, undefined, error);
    }

    /** Creates an onComplete signal. */
    public static complete<T = never>(): Signal<T> {
        return new Signal<T>(SignalType.COMPLETE);
    }

    /** Returns true for onNext signals. */
    public isOnNext(): boolean {
        return this.type === SignalType.NEXT;
    }

    /** Returns true for onError signals. */
    public isOnError(): boolean {
        return this.type === SignalType.ERROR;
    }

    /** Returns true for onComplete signals. */
    public isOnComplete(): boolean {
        return this.type === SignalType.COMPLETE;
    }

    /** Returns the signal value when present. */
    public get(): T | undefined {
        return this.value;
    }

    /** Returns the signal error when present. */
    public getThrowable(): unknown {
        return this.error;
    }
}
