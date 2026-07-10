/**
 * @packageDocumentation
 * Focused Flux public entrypoint.
 */
/**
 * Flux-only entrypoint.
 *
 * Importing from this module avoids pulling sink and scheduler facades through
 * the package root. Operator modules are registered by the publisher package.
 */
export {Flux} from "@/publishers/index.js";
