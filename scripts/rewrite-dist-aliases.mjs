import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const distRoot = path.join(root, "dist");

for (const file of walk(distRoot)) {
  if (!file.endsWith(".js") && !file.endsWith(".d.ts")) {
    continue;
  }

  const source = fs.readFileSync(file, "utf8");
  const rewritten = source.replace(/(["'])@\/([^"']+)\1/g, (_match, quote, target) => {
    const absoluteTarget = path.join(distRoot, target);
    let relative = path.relative(path.dirname(file), absoluteTarget).replace(/\\/g, "/");
    if (!relative.startsWith(".")) {
      relative = `./${relative}`;
    }
    return `${quote}${relative}${quote}`;
  });

  if (rewritten !== source) {
    fs.writeFileSync(file, rewritten);
  }
}

function walk(directory) {
  if (!fs.existsSync(directory)) {
    return [];
  }

  return fs.readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    const absolute = path.join(directory, entry.name);
    return entry.isDirectory() ? walk(absolute) : [absolute];
  });
}
