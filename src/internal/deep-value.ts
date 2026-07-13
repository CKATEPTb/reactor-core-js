/**
 * @packageDocumentation
 * Internal helpers for snapshotting and comparing nested JavaScript values.
 */

/** Value categories whose mutable state can be captured without retaining the source object. */
type DeepValueKind = "array" | "date" | "regexp" | "map" | "set" | "record" | "opaque";

/** Retains reliable kind metadata for snapshots whose prototypes may contain stateful getters. */
const SNAPSHOT_KINDS = new WeakMap<object, DeepValueKind>();

/** Creates a detached snapshot of nested enumerable state and supported mutable value types. */
export function snapshotDeep(value: unknown): unknown {
    const snapshots = new Map<object, unknown>();

    function snapshotValue(current: unknown): unknown {
        if (!isObject(current) || typeof current === "function") {
            return current;
        }

        if (snapshots.has(current)) {
            return snapshots.get(current);
        }

        const kind = valueKind(current);
        let snapshot: object;
        switch (kind) {
            case "array":
                snapshot = new Array((current as unknown[]).length);
                break;
            case "date":
                snapshot = new Date((current as unknown as Date).getTime());
                break;
            case "regexp": {
                const source = current as unknown as RegExp;
                const regexp = new RegExp(source.source, source.flags);
                regexp.lastIndex = source.lastIndex;
                snapshot = regexp;
                break;
            }
            case "map":
                snapshot = new Map();
                break;
            case "set":
                snapshot = new Set();
                break;
            case "record":
                snapshot = Object.create(Object.getPrototypeOf(current)) as object;
                break;
            case "opaque":
                return current;
        }

        if (kind === "array") {
            Object.setPrototypeOf(snapshot, Object.getPrototypeOf(current));
        }
        SNAPSHOT_KINDS.set(snapshot, kind);
        snapshots.set(current, snapshot);

        if (kind === "map") {
            const source = current as unknown as Map<unknown, unknown>;
            const target = snapshot as Map<unknown, unknown>;
            for (const [key, entryValue] of source) {
                target.set(snapshotValue(key), snapshotValue(entryValue));
            }
        } else if (kind === "set") {
            const source = current as unknown as Set<unknown>;
            const target = snapshot as Set<unknown>;
            for (const entryValue of source) {
                target.add(snapshotValue(entryValue));
            }
        }

        copyEnumerableState(current, snapshot, snapshotValue);
        return snapshot;
    }

    return snapshotValue(value);
}

/** Compares nested enumerable state, supported mutable values, cycles and shared references. */
export function deepEqual(left: unknown, right: unknown): boolean {
    const leftToRight = new Map<object, object>();
    const rightToLeft = new Map<object, object>();

    function equal(leftValue: unknown, rightValue: unknown): boolean {
        if (Object.is(leftValue, rightValue)) {
            if (isObject(leftValue)) {
                if (leftToRight.has(leftValue) || rightToLeft.has(leftValue)) {
                    return leftToRight.get(leftValue) === leftValue && rightToLeft.get(leftValue) === leftValue;
                }
                leftToRight.set(leftValue, leftValue);
                rightToLeft.set(leftValue, leftValue);
            }
            return true;
        }
        if (!isObject(leftValue) || !isObject(rightValue)) {
            return false;
        }
        if (typeof leftValue === "function" || typeof rightValue === "function") {
            return false;
        }
        if (Object.getPrototypeOf(leftValue) !== Object.getPrototypeOf(rightValue)) {
            return false;
        }

        const kind = valueKind(leftValue);
        if (kind === "opaque" || kind !== valueKind(rightValue)) {
            return false;
        }
        if (leftToRight.has(leftValue) || rightToLeft.has(rightValue)) {
            return leftToRight.get(leftValue) === rightValue && rightToLeft.get(rightValue) === leftValue;
        }
        leftToRight.set(leftValue, rightValue);
        rightToLeft.set(rightValue, leftValue);

        switch (kind) {
            case "array":
                if ((leftValue as unknown[]).length !== (rightValue as unknown[]).length) {
                    return false;
                }
                break;
            case "date":
                if (!Object.is((leftValue as Date).getTime(), (rightValue as Date).getTime())) {
                    return false;
                }
                break;
            case "regexp": {
                const leftRegExp = leftValue as RegExp;
                const rightRegExp = rightValue as RegExp;
                if (leftRegExp.source !== rightRegExp.source
                    || leftRegExp.flags !== rightRegExp.flags
                    || leftRegExp.lastIndex !== rightRegExp.lastIndex) {
                    return false;
                }
                break;
            }
            case "map":
                if (!equalMap(leftValue as Map<unknown, unknown>, rightValue as Map<unknown, unknown>, equal)) {
                    return false;
                }
                break;
            case "set":
                if (!equalSet(leftValue as Set<unknown>, rightValue as Set<unknown>, equal)) {
                    return false;
                }
                break;
            case "record":
                break;
        }
        return equalEnumerableState(leftValue, rightValue, equal);
    }

    return equal(left, right);
}

/** Returns true for non-null object and function values. */
function isObject(value: unknown): value is object {
    return (typeof value === "object" && value !== null) || typeof value === "function";
}

/** Classifies values with observable state supported by the deep snapshot. */
function valueKind(value: object): DeepValueKind {
    const snapshotKind = SNAPSHOT_KINDS.get(value);
    if (snapshotKind) {
        return snapshotKind;
    }
    if (Array.isArray(value)) {
        return "array";
    }
    const prototype = Object.getPrototypeOf(value);
    if (value instanceof Date && prototype === Date.prototype) {
        return "date";
    }
    if (value instanceof RegExp && prototype === RegExp.prototype) {
        return "regexp";
    }
    if (value instanceof Map && prototype === Map.prototype) {
        return "map";
    }
    if (value instanceof Set && prototype === Set.prototype) {
        return "set";
    }
    return Object.prototype.toString.call(value) === "[object Object]" ? "record" : "opaque";
}

/** Copies enumerable own properties without retaining nested object references. */
function copyEnumerableState(source: object, target: object, snapshot: (value: unknown) => unknown): void {
    for (const key of enumerableKeys(source)) {
        if (!Object.prototype.hasOwnProperty.call(target, key)) {
            Object.defineProperty(target, key, {
                configurable: true,
                enumerable: true,
                value: snapshot(Reflect.get(source, key)),
                writable: true
            });
        }
    }
}

/** Compares enumerable own properties in linear time. */
function equalEnumerableState(
    left: object,
    right: object,
    equal: (left: unknown, right: unknown) => boolean
): boolean {
    const leftKeys = enumerableKeys(left);
    if (leftKeys.length !== enumerableKeys(right).length) {
        return false;
    }
    for (const key of leftKeys) {
        if (!Object.prototype.propertyIsEnumerable.call(right, key)
            || !equal(Reflect.get(left, key), Reflect.get(right, key))) {
            return false;
        }
    }
    return true;
}

/** Lists enumerable string and symbol properties owned by a value. */
function enumerableKeys(value: object): PropertyKey[] {
    return Reflect.ownKeys(value).filter(key => Object.prototype.propertyIsEnumerable.call(value, key));
}

/** Compares Maps in their observable iteration order. */
function equalMap(
    left: Map<unknown, unknown>,
    right: Map<unknown, unknown>,
    equal: (left: unknown, right: unknown) => boolean
): boolean {
    if (left.size !== right.size) {
        return false;
    }
    const rightEntries = right.entries();
    for (const [leftKey, leftValue] of left) {
        const rightEntry = rightEntries.next();
        if (rightEntry.done || !equal(leftKey, rightEntry.value[0]) || !equal(leftValue, rightEntry.value[1])) {
            return false;
        }
    }
    return true;
}

/** Compares Sets in their observable iteration order. */
function equalSet(
    left: Set<unknown>,
    right: Set<unknown>,
    equal: (left: unknown, right: unknown) => boolean
): boolean {
    if (left.size !== right.size) {
        return false;
    }
    const rightValues = right.values();
    for (const leftValue of left) {
        const rightValue = rightValues.next();
        if (rightValue.done || !equal(leftValue, rightValue.value)) {
            return false;
        }
    }
    return true;
}
