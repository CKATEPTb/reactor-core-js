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

    /** Creates a context and validates every provided entry. */
    private constructor(entries?: Iterable<readonly [ContextKey, unknown]>) {
        super(entries);
        for (const [key, value] of this.entries) {
            assertContextEntry(key, value);
        }
    }

    /** Returns the shared empty context. */
    public static empty(): Context {
        return Context.EMPTY;
    }

    /** Creates a context from key/value pairs. */
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
        if (pairs.length === 1 && pairs[0] instanceof Map) {
            return Context.from(pairs[0]);
        }
        if (pairs.length % 2 !== 0) {
            throw new RangeError("Context.of expects key/value pairs");
        }
        const entries: Array<[ContextKey, unknown]> = [];
        for (let i = 0; i < pairs.length; i += 2) {
            entries.push([pairs[i], pairs[i + 1]]);
        }
        return entries.length === 0 ? Context.empty() : new Context(entries);
    }

    /** Creates a context from iterable entries or an object record. */
    public static from(entries: Iterable<readonly [ContextKey, unknown]> | Record<PropertyKey, unknown>): Context {
        if (typeof (entries as { [Symbol.iterator]?: unknown })[Symbol.iterator] === "function") {
            return new Context(entries as Iterable<readonly [ContextKey, unknown]>);
        }
        return new Context(Object.entries(entries as Record<string, unknown>));
    }

    /** Returns a new context with `key` associated to `value`. */
    public put(key: ContextKey, value: unknown): Context {
        assertContextEntry(key, value);
        const next = new Map(this.entries);
        next.set(key, value);
        return new Context(next);
    }

    /** Returns this context unchanged when `value` is nullish, otherwise puts it. */
    public putNonNull(key: ContextKey, value: unknown | null | undefined): Context {
        return value === null || value === undefined ? this : this.put(key, value);
    }

    /** Returns a new context containing this context and all entries from `other`. */
    public putAll(other: ContextView | Iterable<readonly [ContextKey, unknown]>): Context {
        const next = new Map(this.entries);
        const entries = other instanceof ContextView ? other.stream() : other;
        for (const [key, value] of entries) {
            assertContextEntry(key, value);
            next.set(key, value);
        }
        return new Context(next);
    }

    /** Returns a new context without `key`. */
    public delete(key: ContextKey): Context {
        if (!this.entries.has(key)) {
            return this;
        }
        const next = new Map(this.entries);
        next.delete(key);
        return next.size === 0 ? Context.empty() : new Context(next);
    }

    /** Returns a read-only view of this context. */
    public readOnly(): ContextView {
        return new ContextView(this.entries);
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
