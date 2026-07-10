/**
 * @packageDocumentation
 * Context storage and lookup primitives for Reactor-style contextual operators.
 */
import type {ContextKey} from "@/context/types.js";

/** Shared immutable empty context entries map. */
const EMPTY_CONTEXT_ENTRIES: ReadonlyMap<ContextKey, unknown> = new Map();

/** Read-only immutable context view. */
export class ContextView {
    /** Stores immutable context entries by arbitrary user-provided keys. */
    protected entries: ReadonlyMap<ContextKey, unknown>;

    /** Creates a read-only context view from entries, copying unless entries are trusted immutable data. */
    public constructor(entries?: Iterable<readonly [ContextKey, unknown]>, trusted = false) {
        this.entries =
            entries === undefined
                ? EMPTY_CONTEXT_ENTRIES
                : trusted
                    ? (entries as ReadonlyMap<ContextKey, unknown>)
                    : new Map(entries);
    }

    /** Returns the value for `key` or throws when absent. */
    public get<T = unknown>(key: ContextKey): T {
        if (!this.entries.has(key)) {
            throw new Error(`Context does not contain key: ${String(key)}`);
        }
        return this.entries.get(key) as T;
    }

    /** Returns the value for `key` or `defaultValue` when absent. */
    public getOrDefault<T>(key: ContextKey, defaultValue: T): T {
        return this.entries.has(key) ? (this.entries.get(key) as T) : defaultValue;
    }

    /** Returns the value for `key` or undefined when absent. */
    public getOrEmpty<T = unknown>(key: ContextKey): T | undefined {
        return this.entries.get(key) as T | undefined;
    }

    /** Returns true when the context contains `key`. */
    public hasKey(key: ContextKey): boolean {
        return this.entries.has(key);
    }

    /** Returns true when this context has no entries. */
    public isEmpty(): boolean {
        return this.entries.size === 0;
    }

    /** Returns the number of entries. */
    public size(): number {
        return this.entries.size;
    }

    /** Invokes `consumer` for each context entry. */
    public forEach(consumer: (key: ContextKey, value: unknown) => void): void {
        for (const [key, value] of this.entries) {
            consumer(key, value);
        }
    }

    /** Returns an iterator over context entries. */
    public stream(): IterableIterator<readonly [ContextKey, unknown]> {
        return this.entries.entries();
    }

    /** Copies this context view to a readonly map. */
    public toMap(): ReadonlyMap<ContextKey, unknown> {
        return new Map(this.entries);
    }
}
