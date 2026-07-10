/**
 * @packageDocumentation
 * Internal iterable terminal helpers.
 */
import {type AnyIterable, isAsyncIterable} from "@/internal/iterable.js";

/** Collects an iterable into an array, using a synchronous loop when possible. */
export function collectIterable<T>(source: AnyIterable<T>): T[] | Promise<T[]> {
    if (isAsyncIterable<T>(source)) {
        return collectAsync(source);
    }
    const values: T[] = [];
    for (const value of source) {
        values.push(value);
    }
    return values;
}

/** Returns the last value from an iterable, or undefined when it is empty. */
export function lastIterableValue<T>(source: AnyIterable<T>): T | undefined | Promise<T | undefined> {
    if (isAsyncIterable<T>(source)) {
        return lastAsync(source);
    }
    let last: T | undefined;
    for (const value of source) {
        last = value;
    }
    return last;
}

/** Counts iterable values, using a synchronous loop when possible. */
export function countIterable<T>(source: AnyIterable<T>): number | Promise<number> {
    if (isAsyncIterable<T>(source)) {
        return countAsync(source);
    }
    let count = 0;
    for (const _ of source) {
        count += 1;
    }
    return count;
}

/** Collects an async iterable into an array. */
async function collectAsync<T>(source: AsyncIterable<T>): Promise<T[]> {
    const values: T[] = [];
    for await (const value of source) {
        values.push(value);
    }
    return values;
}

/** Returns the last async iterable value, or undefined when empty. */
async function lastAsync<T>(source: AsyncIterable<T>): Promise<T | undefined> {
    let last: T | undefined;
    for await (const value of source) {
        last = value;
    }
    return last;
}

/** Counts async iterable values. */
async function countAsync<T>(source: AsyncIterable<T>): Promise<number> {
    let count = 0;
    for await (const _ of source) {
        count += 1;
    }
    return count;
}
