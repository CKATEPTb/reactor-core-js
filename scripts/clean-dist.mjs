/**
 * Removes the generated distribution folder before compiling.
 *
 * TypeScript does not delete files whose source paths were removed or renamed,
 * so a clean build keeps package entrypoints free of stale artifacts.
 */
import { rmSync } from "node:fs";
import { resolve } from "node:path";

/** Absolute path to the generated distribution directory. */
const dist = resolve("dist");

rmSync(dist, { force: true, recursive: true });
