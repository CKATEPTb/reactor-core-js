/**
 * @packageDocumentation
 * Materialized signal representation and signal kinds.
 */
/** Reactive signal kinds used by materialize/dematerialize and sink handlers. */
export enum SignalType {
    /** Subscription signal. */
    SUBSCRIBE = "subscribe",
    /** Request signal. */
    REQUEST = "request",
    /** Cancellation signal. */
    CANCEL = "cancel",
    /** Next value signal. */
    NEXT = "next",
    /** Error terminal signal. */
    ERROR = "error",
    /** Complete terminal signal. */
    COMPLETE = "complete",
    /** Post-termination callback signal. */
    AFTER_TERMINATE = "afterTerminate",
    /** Current context lookup signal. */
    CURRENT_CONTEXT = "currentContext",
    /** Context write signal. */
    ON_CONTEXT = "onContext"
}
