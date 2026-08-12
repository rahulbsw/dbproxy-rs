import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const websiteDir = path.dirname(fileURLToPath(import.meta.url));
const outputDir = path.join(websiteDir, "dist");

if (!fs.existsSync(outputDir)) {
  throw new Error("website/dist does not exist; run npm run build first");
}

const htmlFiles = [];
function collect(directory) {
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const absolute = path.join(directory, entry.name);
    if (entry.isDirectory()) collect(absolute);
    if (entry.isFile() && entry.name.endsWith(".html")) htmlFiles.push(absolute);
  }
}
collect(outputDir);

const failures = [];
for (const file of htmlFiles) {
  const html = fs.readFileSync(file, "utf8");
  const relativeFile = path.relative(outputDir, file);
  for (const required of ["<title>", "data-search-open", 'id="main-content"']) {
    if (!html.includes(required)) failures.push(`${relativeFile}: missing ${required}`);
  }

  for (const match of html.matchAll(/(?:href|src)="([^"]+)"/g)) {
    const target = match[1];
    if (/^(?:https?:|mailto:)/.test(target)) continue;
    const [withoutFragment, fragment] = target.split("#", 2);
    const resolved = withoutFragment && withoutFragment !== "./"
      ? path.resolve(path.dirname(file), withoutFragment)
      : file;
    if (!fs.existsSync(resolved)) {
      failures.push(`${relativeFile}: missing ${target}`);
      continue;
    }
    if (fragment && resolved.endsWith(".html")) {
      const targetHtml = fs.readFileSync(resolved, "utf8");
      if (!targetHtml.includes(`id="${decodeURIComponent(fragment)}"`)) {
        failures.push(`${relativeFile}: missing anchor ${target}`);
      }
    }
  }
}

const searchIndexPath = path.join(outputDir, "search-index.json");
const searchIndex = JSON.parse(fs.readFileSync(searchIndexPath, "utf8"));
if (searchIndex.length < htmlFiles.length) {
  failures.push("search index does not cover every generated page");
}
for (const entry of searchIndex) {
  const [fileTarget, fragment] = entry.url.split("#", 2);
  const resolved = path.join(outputDir, fileTarget);
  if (!fs.existsSync(resolved)) {
    failures.push(`search index: missing ${entry.url}`);
    continue;
  }
  if (fragment) {
    const html = fs.readFileSync(resolved, "utf8");
    if (!html.includes(`id="${decodeURIComponent(fragment)}"`)) {
      failures.push(`search index: missing anchor ${entry.url}`);
    }
  }
}

const mermaidBundle = path.join(outputDir, "assets", "vendor", "mermaid.min.js");
if (!fs.existsSync(mermaidBundle)) {
  failures.push("Mermaid browser bundle is missing");
}
const architectureHtml = fs.readFileSync(path.join(outputDir, "docs", "architecture.html"), "utf8");
if (!architectureHtml.includes('src="../assets/vendor/mermaid.min.js"')) {
  failures.push("architecture page does not load the local Mermaid bundle");
}
const mermaidSources = architectureHtml.match(/class="language-mermaid"/g) ?? [];
if (mermaidSources.length !== 5) {
  failures.push(`architecture page contains ${mermaidSources.length} Mermaid sources; expected 5`);
}
const homeHtml = fs.readFileSync(path.join(outputDir, "index.html"), "utf8");
if (homeHtml.includes("assets/vendor/mermaid.min.js")) {
  failures.push("homepage loads Mermaid without any diagrams");
}

if (failures.length) {
  console.error(failures.join("\n"));
  process.exit(1);
}

console.log(`Validated ${htmlFiles.length} HTML pages, local assets, and ${searchIndex.length} search sections.`);
