/**
 * @packageDocumentation
 * Browser scheduler implementations and contracts.
 */
import type {DurationInput} from "@/scheduler/types.js";

/** Converts a duration input into milliseconds. */
export function toMillis(duration: DurationInput | undefined): number {
    if (duration === undefined) {
        return 0;
    }
    if (typeof duration === "number") {
        return Math.max(0, duration);
    }
    return Math.max(
        0,
        (duration.milliseconds ?? 0) +
        (duration.seconds ?? 0) * 1_000 +
        (duration.minutes ?? 0) * 60_000 +
        (duration.hours ?? 0) * 3_600_000
    );
}
