/**
 * Semantic analyzer for detecting application entrypoint patterns in Python
 * source code.
 */

import { importWasmModule } from '../wasm/load';

/**
 * Check if Python source code contains or exports:
 * - A top-level 'app' callable (e.g., Flask, FastAPI, Sanic apps)
 * - A top-level 'application' callable (e.g., Django )
 * - A top-level 'handler' class (e.g., BaseHTTPRequestHandler subclass)
 *
 * This function uses a WASM-based Python parser (ruff_python_ast) for
 * accurate AST analysis without requiring a Python runtime.
 *
 * @param source - The Python source code to analyze
 * @returns Promise that resolves to true if an app or handler is found, false otherwise.
 *          Returns false for invalid Python syntax.
 *
 * @example
 * ```typescript
 * import { containsAppOrHandler } from '@vercel/python-analysis';
 *
 * const hasApp = await containsAppOrHandler(`
 * from flask import Flask
 * app = Flask(__name__)
 * `);
 * console.log(hasApp); // true
 * ```
 */
export async function containsAppOrHandler(source: string): Promise<boolean> {
  // Skip parsing if file doesn't contain {app|application|[Hh]andler}
  if (
    !source.includes('app') &&
    !source.includes('application') &&
    !source.includes('handler') &&
    !source.includes('Handler')
  ) {
    return false;
  }
  const mod = await importWasmModule();
  return mod.containsAppOrHandler(source);
}

/**
 * Either a directly-found string value, or sibling module names that may define
 * the constant via import.
 */
export interface StringConstantResult {
  /** The string value if defined directly in this file, or null. */
  value: string | null;
  /**
   * Sibling module names to check when the constant is not defined directly
   * but may come from an import. Empty when the constant is defined directly,
   * when a non-sibling explicit import was found (can't resolve), or when
   * there are no relevant imports at all.
   */
  relativeImports: string[];
}

/**
 * Extract the string value of a top-level constant with the given name.
 * A constant is a simple (`NAME = "value"`) or annotated (`NAME: str = "value"`)
 * assignment at module level. Returns null if not found or the value is not a
 * string literal.
 */
export async function getStringConstant(
  source: string,
  name: string
): Promise<string | null> {
  const mod = await importWasmModule();
  return mod.getStringConstant(source, name).value ?? null;
}

/**
 * Extract the string value of a top-level constant with the given name, or
 * return sibling module names to check when it comes from an import. A constant
 * is a simple (`NAME = "value"`) or annotated (`NAME: str = "value"`) assignment
 * at module level.
 */
export async function getStringConstantOrImports(
  source: string,
  name: string
): Promise<StringConstantResult> {
  const mod = await importWasmModule();
  const result = mod.getStringConstant(source, name);
  return {
    value: result.value ?? null,
    relativeImports: result.relativeImports,
  };
}

/** Simple check for DJANGO_SETTINGS_MODULE presence so we can skip WASM when absent */
const DJANGO_SETTINGS_MODULE_PATTERN_RE = /DJANGO_SETTINGS_MODULE/;

/**
 * Parse manage.py content for DJANGO_SETTINGS_MODULE (e.g. from
 * os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'app.settings')).
 * Uses the WASM Python parser to extract the value from the AST.
 *
 * @param content - Raw content of manage.py
 * @returns The settings module string (e.g. 'app.settings') or null if not found
 */
export async function parseDjangoSettingsModule(
  content: string
): Promise<string | null> {
  if (!DJANGO_SETTINGS_MODULE_PATTERN_RE.test(content)) {
    return null;
  }
  const mod = await importWasmModule();
  return mod.parseDjangoSettingsModule(content) ?? null;
}
