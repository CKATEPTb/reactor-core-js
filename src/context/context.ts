/**
 * @packageDocumentation
 * Context storage and lookup primitives for Reactor-style contextual operators.
 */
import {ContextView} from "@/context/context-view.js";
import type {ContextKey} from "@/context/types.js";

/** Immutable writable context that returns new instances on mutation. */
export class Context extends ContextView {
    /** Shared singleton used for all empty context instances. */
    private static readonly EMPTY = new Context();
    /** Cached read-only facade for this immutable context. */
    private readOnlyView: ContextView | undefined;

    /** Creates a context and validates every provided entry. */
    private constructor(entries?: Iterable<readonly [ContextKey, unknown]>, trusted = false, validate = true) {
        super();
        const nextEntries =
            entries === undefined
                ? this.entries
                : trusted
                    ? (entries as ReadonlyMap<ContextKey, unknown>)
                    : new Map(entries);
        if (validate) {
            for (const [key, value] of nextEntries) {
                assertContextEntry(key, value);
            }
        }
        this.entries = nextEntries;
    }

    /** Returns the shared empty context. */
    public static empty(): Context {
        return Context.EMPTY;
    }

    /** Creates a context from key/value pairs. */
    public static of(): Context;
    /** Creates a context from one key/value pair. */
    public static of(key: ContextKey, value: unknown): Context;
    /** Creates a context from two key/value pairs. */
    public static of(key1: ContextKey, value1: unknown, key2: ContextKey, value2: unknown): Context;
    /** Creates a context from three key/value pairs. */
    public static of(
        key1: ContextKey,
        value1: unknown,
        key2: ContextKey,
        value2: unknown,
        key3: ContextKey,
        value3: unknown
    ): Context;
    /** Creates a context from four key/value pairs. */
    public static of(
        key1: ContextKey,
        value1: unknown,
        key2: ContextKey,
        value2: unknown,
        key3: ContextKey,
        value3: unknown,
        key4: ContextKey,
        value4: unknown
    ): Context;
    /** Creates a context from five key/value pairs. */
    public static of(
        key1: ContextKey,
        value1: unknown,
        key2: ContextKey,
        value2: unknown,
        key3: ContextKey,
        value3: unknown,
        key4: ContextKey,
        value4: unknown,
        key5: ContextKey,
        value5: unknown
    ): Context;
    /** Creates a context from an arbitrary even-length key/value pair list. */
    public static of(...pairs: unknown[]): Context {
        if (pairs.length === 0) {
            return Context.empty();
        }
        if (pairs.length === 1 && pairs[0] instanceof Map) {
            return Context.from(pairs[0]);
        }
        if (pairs.length % 2 !== 0) {
            throw new RangeError("Context.of expects key/value pairs");
        }
        const entries = new Map<ContextKey, unknown>();
        for (let i = 0; i < pairs.length; i += 2) {
            const key = pairs[i];
            const value = pairs[i + 1];
            assertContextEntry(key, value);
            entries.set(key, value);
        }
        return entries.size === 0 ? Context.empty() : new Context(entries, true, false);
    }

    /** Creates a context from iterable entries or an object record. */
    public static from(entries: Iterable<readonly [ContextKey, unknown]> | Record<PropertyKey, unknown>): Context {
        if (typeof (entries as { [Symbol.iterator]?: unknown })[Symbol.iterator] === "function") {
            const sized = entries as { readonly length?: number; readonly size?: number };
            if (sized.size === 0 || sized.length === 0) {
                return Context.empty();
            }
            return Context.fromEntries(entries as Iterable<readonly [ContextKey, unknown]>);
        }
        return Context.fromRecord(entries as Record<PropertyKey, unknown>);
    }

    /** Returns a new context with `key` associated to `value`. */
    public put(key: ContextKey, value: unknown): Context {
        assertContextEntry(key, value);
        if (Object.is(this.entries.get(key), value)) {
            return this;
        }
        const next = new Map(this.entries);
        next.set(key, value);
        return new Context(next, true, false);
    }

    /** Returns this context unchanged when `value` is nullish, otherwise puts it. */
    public putNonNull(key: ContextKey, value: unknown | null | undefined): Context {
        return value === null || value === undefined ? this : this.put(key, value);
    }

    /** Returns a new context containing this context and all entries from `other`. */
    public putAll(other: ContextView | Iterable<readonly [ContextKey, unknown]>): Context {
        let next: Map<ContextKey, unknown> | undefined;
        const entries = other instanceof ContextView ? other.stream() : other;
        for (const [key, value] of entries) {
            assertContextEntry(key, value);
            if (next === undefined) {
                if (Object.is(this.entries.get(key), value)) {
                    continue;
                }
                next = new Map(this.entries);
            }
            next.set(key, value);
        }
        return next === undefined ? this : new Context(next, true, false);
    }

    /** Returns a new context without `key`. */
    public delete(key: ContextKey): Context {
        if (!this.entries.has(key)) {
            return this;
        }
        const next = new Map(this.entries);
        next.delete(key);
        return next.size === 0 ? Context.empty() : new Context(next, true, false);
    }

    /** Returns a read-only view of this context. */
    public readOnly(): ContextView {
        this.readOnlyView ??= new ContextView(this.entries, true);
        return this.readOnlyView;
    }

    /** Creates a context from enumerable string and symbol record keys. */
    private static fromRecord(record: Record<PropertyKey, unknown>): Context {
        let entries: Map<ContextKey, unknown> | undefined;
        for (const key of Object.keys(record)) {
            const value = record[key];
            assertContextEntry(key, value);
            (entries ??= new Map()).set(key, value);
        }
        for (const key of Object.getOwnPropertySymbols(record)) {
            if (!Object.prototype.propertyIsEnumerable.call(record, key)) {
                continue;
            }
            const value = record[key];
            assertContextEntry(key, value);
            (entries ??= new Map()).set(key, value);
        }
        return entries === undefined ? Context.empty() : new Context(entries, true, false);
    }

    /** Creates a context from iterable entries while validating in the same pass. */
    private static fromEntries(entries: Iterable<readonly [ContextKey, unknown]>): Context {
        let next: Map<ContextKey, unknown> | undefined;
        for (const [key, value] of entries) {
            assertContextEntry(key, value);
            (next ??= new Map()).set(key, value);
        }
        return next === undefined ? Context.empty() : new Context(next, true, false);
    }
}

/** Validates that a context key and value can be stored. */
function assertContextEntry(key: ContextKey, value: unknown): void {
    if (key === null || key === undefined) {
        throw new TypeError("Context key must not be null or undefined");
    }
    if (value === null || value === undefined) {
        throw new TypeError("Context value must not be null or undefined");
    }
}
