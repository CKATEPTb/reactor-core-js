import fs from "node:fs";
import path from "node:path";
import ts from "typescript";
import { describe, expect, it } from "vitest";

const declarationKinds = new Set<ts.SyntaxKind>([
  ts.SyntaxKind.ClassDeclaration,
  ts.SyntaxKind.Constructor,
  ts.SyntaxKind.EnumDeclaration,
  ts.SyntaxKind.EnumMember,
  ts.SyntaxKind.FunctionDeclaration,
  ts.SyntaxKind.GetAccessor,
  ts.SyntaxKind.InterfaceDeclaration,
  ts.SyntaxKind.MethodDeclaration,
  ts.SyntaxKind.MethodSignature,
  ts.SyntaxKind.PropertyDeclaration,
  ts.SyntaxKind.PropertySignature,
  ts.SyntaxKind.SetAccessor,
  ts.SyntaxKind.TypeAliasDeclaration,
  ts.SyntaxKind.VariableStatement
]);

const typoPatterns: Array<readonly [RegExp, string]> = [
  [/\brecieve(?:s|d)?\b/i, "receive"],
  [/\bseperate(?:s|d)?\b/i, "separate"],
  [/\boccurence(?:s)?\b/i, "occurrence"],
  [/\boccured\b/i, "occurred"],
  [/\bsucces\b/i, "success"],
  [/\bsucess\b/i, "success"],
  [/\bemiting\b/i, "emitting"],
  [/\bemmits\b/i, "emits"],
  [/\bteh\b/i, "the"],
  [/\badress(?:es)?\b/i, "address"]
];

describe("TSDoc coverage", () => {
  it("documents source files and structural declarations", () => {
    const missing: string[] = [];
    const invalidText: string[] = [];

    for (const file of sourceFiles(path.join(process.cwd(), "src"))) {
      const sourceText = fs.readFileSync(file, "utf8");
      const sourceFile = ts.createSourceFile(file, sourceText, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
      const relativeFile = path.relative(process.cwd(), file).replace(/\\/g, "/");

      if (!sourceText.trimStart().startsWith("/**") || !sourceText.includes("@packageDocumentation")) {
        missing.push(`${relativeFile}:1 missing @packageDocumentation`);
      }

      visitDocumentedDeclarations(sourceFile, sourceFile, missing, relativeFile);
      validateTSDocText(sourceText, invalidText, relativeFile);
    }

    expect(missing).toEqual([]);
    expect(invalidText).toEqual([]);
  });
});

function sourceFiles(root: string): string[] {
  return fs
    .readdirSync(root, { withFileTypes: true })
    .flatMap(entry => {
      const absolute = path.join(root, entry.name);
      if (entry.isDirectory()) {
        return sourceFiles(absolute);
      }
      return entry.isFile() && entry.name.endsWith(".ts") ? [absolute] : [];
    })
    .sort();
}

function visitDocumentedDeclarations(
  node: ts.Node,
  sourceFile: ts.SourceFile,
  missing: string[],
  relativeFile: string
): void {
  if (requiresTSDoc(node, sourceFile) && !hasTSDoc(node, sourceFile)) {
    const position = sourceFile.getLineAndCharacterOfPosition(node.getStart(sourceFile));
    missing.push(`${relativeFile}:${position.line + 1}:${position.character + 1} missing TSDoc`);
  }

  ts.forEachChild(node, child => visitDocumentedDeclarations(child, sourceFile, missing, relativeFile));
}

function requiresTSDoc(node: ts.Node, sourceFile: ts.SourceFile): boolean {
  if (!declarationKinds.has(node.kind)) {
    return false;
  }

  const parent = node.parent;
  return (
    parent === sourceFile ||
    ts.isClassElement(node) ||
    (ts.isTypeElement(node) && ts.isInterfaceDeclaration(parent)) ||
    ts.isEnumMember(node) ||
    (ts.isInterfaceDeclaration(node) && parent.kind === ts.SyntaxKind.ModuleBlock) ||
    isDocumentedObjectMethod(node)
  );
}

function isDocumentedObjectMethod(node: ts.Node): boolean {
  if (!ts.isMethodDeclaration(node) || !ts.isObjectLiteralExpression(node.parent)) {
    return false;
  }

  const container = node.parent.parent;
  return container?.parent?.kind === ts.SyntaxKind.SourceFile || ts.isReturnStatement(container);
}

function hasTSDoc(node: ts.Node, sourceFile: ts.SourceFile): boolean {
  const ranges =
    ts.getLeadingCommentRanges(sourceFile.text, node.pos) ??
    ts.getLeadingCommentRanges(sourceFile.text, node.getFullStart()) ??
    [];
  return ranges.some(range => sourceFile.text.startsWith("/**", range.pos));
}

function validateTSDocText(sourceText: string, invalidText: string[], relativeFile: string): void {
  for (const match of sourceText.matchAll(/\/\*\*([\s\S]*?)\*\//g)) {
    const line = sourceText.slice(0, match.index).split(/\r?\n/).length;
    const text = normalizeTSDoc(match[1]!);

    if (/\p{Script=Cyrillic}/u.test(text)) {
      invalidText.push(`${relativeFile}:${line} TSDoc must be written in English`);
    }
    if (/\bReactor\s+\w+\s+operator\./.test(text)) {
      invalidText.push(`${relativeFile}:${line} replace placeholder operator TSDoc with behavior`);
    }
    for (const [pattern, suggestion] of typoPatterns) {
      const typo = text.match(pattern)?.[0];
      if (typo) {
        invalidText.push(`${relativeFile}:${line} possible TSDoc typo "${typo}", expected "${suggestion}"`);
      }
    }
  }
}

function normalizeTSDoc(raw: string): string {
  return raw
    .split(/\r?\n/)
    .map(line => line.replace(/^\s*\* ?/, "").trim())
    .filter(Boolean)
    .join(" ");
}
