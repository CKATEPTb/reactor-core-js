/**
 * @packageDocumentation
 * Focused Mono public entrypoint.
 */
/**
 * Mono-only entrypoint.
 *
 * This entrypoint registers Mono and Flux operators required by Mono while
 * avoiding unrelated public facades such as sinks.
 */
export {Mono} from "@/publishers/index.js";
