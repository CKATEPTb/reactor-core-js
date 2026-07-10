import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const distRoot = path.join(root, "dist");
const packageJsonPath = path.join(root, "package.json");
const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, "utf8"));
const failures = [];
const forbiddenRuntimePatterns = [
  [/\bfrom\s+["']node:/, "a node: import"],
  [/\bimport\s*\(\s*["']node:/, "a dynamic node: import"],
  [/\brequire\s*\(/, "CommonJS require"],
  [/\bprocess\./, "the process global"],
  [/\bBuffer\b/, "the Buffer global"],
  [/\b__dirname\b/, "__dirname"],
  [/\b__filename\b/, "__filename"],
  [/(?:\bfrom\s+|\bimport\s*\(\s*)["']@\//, "an unreplaced internal alias import"]
];

if (!fs.existsSync(distRoot)) {
  failures.push("dist directory does not exist; run npm run build first");
} else {
  validateBrowserRuntime();
}

validatePackageTargets();

if (failures.length > 0) {
  console.error(`[browser-package] ${failures.length} issue(s) found:`);
  for (const failure of failures) {
    console.error(`- ${failure}`);
  }
  process.exit(1);
}

console.log("[browser-package] package metadata and dist runtime look browser-safe");

function validateBrowserRuntime() {
  for (const file of walk(distRoot)) {
    if (!file.endsWith(".js")) {
      continue;
    }

    const source = fs.readFileSync(file, "utf8");
    const relative = toPackagePath(file);

    for (const [pattern, label] of forbiddenRuntimePatterns) {
      if (pattern.test(source)) {
        failures.push(`${relative} contains ${label}`);
      }
    }
  }
}

function validatePackageTargets() {
  const packageTargets = [
    packageJson.main,
    packageJson.module,
    packageJson.browser,
    packageJson.types,
    ...collectExportTargets(packageJson.exports)
  ].filter(Boolean);

  for (const target of packageTargets) {
    if (typeof target !== "string" || !target.startsWith("./")) {
      continue;
    }

    const absolute = path.join(root, target);
    if (!fs.existsSync(absolute)) {
      failures.push(`package target ${target} does not exist`);
    }
  }
}

function collectExportTargets(value) {
  if (typeof value === "string") {
    return [value];
  }
  if (!value || typeof value !== "object") {
    return [];
  }
  return Object.values(value).flatMap(collectExportTargets);
}

function walk(directory) {
  return fs.readdirSync(directory, {withFileTypes: true}).flatMap(entry => {
    const absolute = path.join(directory, entry.name);
    return entry.isDirectory() ? walk(absolute) : [absolute];
  });
}

function toPackagePath(file) {
  return path.relative(root, file).replace(/\\/g, "/");
}
