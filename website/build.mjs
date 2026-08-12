import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { marked } from "marked";
import { getHeadingList, gfmHeadingId } from "marked-gfm-heading-id";

const websiteDir = path.dirname(fileURLToPath(import.meta.url));
const repositoryDir = path.resolve(websiteDir, "..");
const outputDir = path.join(websiteDir, "dist");
const staticDir = path.join(websiteDir, "static");
const githubRepository = "https://github.com/rahulbsw/dbproxy-rs";

const categories = [
  {
    name: "Start here",
    description: "Evaluate DBProxy and run a safe first deployment.",
  },
  {
    name: "Route traffic",
    description: "Choose protocol modes, pools, shards, and topology behavior.",
  },
  {
    name: "Protect data",
    description: "Define cache, transaction, fanout, and runtime-policy contracts.",
  },
  {
    name: "Operate",
    description: "Monitor limits, readiness, failure modes, and open boundaries.",
  },
  {
    name: "Project",
    description: "Deploy the chart, contribute changes, and report security issues.",
  },
];

const pages = [
  {
    source: "docs/quick-start.md",
    output: "docs/quick-start.html",
    category: "Start here",
    title: "Quick start",
    description: "Start, verify, inspect, and stop the multi-protocol demo.",
  },
  {
    source: "docs/deployment.md",
    output: "docs/deployment.html",
    category: "Start here",
    title: "Deployment",
    description: "Run DBProxy locally or deploy it to Kubernetes.",
  },
  {
    source: "docs/product-positioning.md",
    output: "docs/product-positioning.html",
    category: "Start here",
    title: "Choose DBProxy",
    description: "Know when DBProxy fits and when a distributed platform is the better choice.",
  },
  {
    source: "docs/architecture.md",
    output: "docs/architecture.html",
    category: "Route traffic",
    title: "Architecture",
    description: "Understand listeners, routing, affinity, failure, and security boundaries.",
  },
  {
    source: "docs/sharding-examples.md",
    output: "docs/sharding-examples.html",
    category: "Route traffic",
    title: "Sharding examples",
    description: "Copy working hash, range, and value configurations for every protocol.",
  },
  {
    source: "docs/protocol-sharding.md",
    output: "docs/protocol-sharding.html",
    category: "Route traffic",
    title: "Protocol sharding",
    description: "Apply PostgreSQL, TimescaleDB, Redis, and Valkey routing contracts.",
  },
  {
    source: "docs/postgres-pooling.md",
    output: "docs/postgres-pooling.html",
    category: "Route traffic",
    title: "PostgreSQL pooling",
    description: "Configure transaction pooling, reset behavior, TLS, and pinning.",
  },
  {
    source: "docs/redis-topology.md",
    output: "docs/redis-topology.html",
    category: "Route traffic",
    title: "Redis topology",
    description: "Operate Sentinel discovery and native Redis Cluster routing.",
  },
  {
    source: "docs/cache.md",
    output: "docs/cache.html",
    category: "Protect data",
    title: "MySQL read-through cache",
    description: "Set cache eligibility, bounds, consistency, and invalidation behavior.",
  },
  {
    source: "docs/cdc-cache-invalidation.md",
    output: "docs/cdc-cache-invalidation.html",
    category: "Protect data",
    title: "CDC invalidation",
    description: "Define durable offsets and fail-closed handling for direct writes.",
  },
  {
    source: "docs/mysql-xa.md",
    output: "docs/mysql-xa.html",
    category: "Protect data",
    title: "MySQL XA fanout",
    description: "Operate atomic fanout, recovery, storage, and application retries.",
  },
  {
    source: "docs/control-plane.md",
    output: "docs/control-plane.html",
    category: "Protect data",
    title: "Shared configuration",
    description: "Publish immutable MySQL policy generations through etcd.",
  },
  {
    source: "docs/operations.md",
    output: "docs/operations.html",
    category: "Operate",
    title: "Operations and metrics",
    description: "Use readiness, Prometheus metrics, alerts, and capacity controls.",
  },
  {
    source: "docs/query-observatory.md",
    output: "docs/query-observatory.html",
    category: "Operate",
    title: "Query observatory",
    description: "Inspect privacy-safe query shapes and explain cache decisions.",
  },
  {
    source: "docs/boundary-closure.md",
    output: "docs/boundary-closure.html",
    category: "Operate",
    title: "Boundary status",
    description: "See the disposition, invariants, and remaining work for each constraint.",
  },
  {
    source: "docs/review.md",
    output: "docs/review.html",
    category: "Operate",
    title: "Assurance review",
    description: "Review enforced database, networking, and sensitive-data guarantees.",
  },
  {
    source: "docs/advanced-features.md",
    output: "docs/advanced-features.html",
    category: "Operate",
    title: "Advanced roadmap",
    description: "Read the acceptance gates for distributed execution and resharding.",
  },
  {
    source: "charts/dbproxy/README.md",
    output: "docs/helm-chart.html",
    category: "Project",
    title: "Helm chart",
    description: "Install independent DBProxy replicas with production controls.",
  },
  {
    source: "CONTRIBUTING.md",
    output: "contributing.html",
    category: "Project",
    title: "Contributing",
    description: "Build, validate, document, and review a DBProxy change.",
  },
  {
    source: "SECURITY.md",
    output: "security.html",
    category: "Project",
    title: "Security",
    description: "Report vulnerabilities and apply the deployment security contract.",
  },
];

const sourceToOutput = new Map(pages.map((page) => [page.source, page.output]));

marked.use(gfmHeadingId());
marked.setOptions({
  gfm: true,
  breaks: false,
});

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function relativeHref(fromOutput, toOutput) {
  return path.posix.relative(path.posix.dirname(fromOutput), toOutput) || "./";
}

function rootPrefix(output) {
  const relative = path.posix.relative(path.posix.dirname(output), ".");
  return relative ? `${relative}/` : "./";
}

function rewriteMarkdownLinks(markdown, page) {
  return markdown.replace(/(!?\[[^\]]*\]\()([^\s)]+)(\))/g, (match, open, rawTarget, close) => {
    if (/^(?:https?:|mailto:|#)/i.test(rawTarget)) {
      return match;
    }

    const [targetPath, fragment] = rawTarget.split("#", 2);
    const resolvedSource = path.posix.normalize(
      path.posix.join(path.posix.dirname(page.source), targetPath),
    );
    const mappedOutput = sourceToOutput.get(resolvedSource);
    const destination = mappedOutput ?? resolvedSource;
    const relative = relativeHref(page.output, destination);
    const anchor = fragment ? `#${fragment}` : "";
    return `${open}${relative}${anchor}${close}`;
  });
}

function stripMarkdown(value) {
  return value
    .replace(/```[\s\S]*?```/g, " ")
    .replace(/`([^`]+)`/g, "$1")
    .replace(/!\[([^\]]*)\]\([^)]*\)/g, "$1")
    .replace(/\[([^\]]+)\]\([^)]*\)/g, "$1")
    .replace(/<[^>]+>/g, " ")
    .replace(/^[#>*+-]+\s*/gm, "")
    .replace(/[|*_~]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function githubSlug(value) {
  return stripMarkdown(value)
    .toLowerCase()
    .trim()
    .replace(/[^\p{Letter}\p{Number}\s-]/gu, "")
    .replace(/\s+/g, "-");
}

function searchSections(page, markdown) {
  const sections = [];
  let section = "Overview";
  let anchor = "";
  let lines = [];

  function commit() {
    const text = stripMarkdown(lines.join("\n"));
    if (!text) return;
    sections.push({
      title: page.title,
      section,
      category: page.category,
      url: `${page.output}${anchor}`,
      text,
    });
  }

  for (const line of markdown.split("\n")) {
    const match = /^(#{2,3})\s+(.+)$/.exec(line);
    if (match) {
      commit();
      section = stripMarkdown(match[2]);
      anchor = `#${githubSlug(match[2])}`;
      lines = [];
      continue;
    }
    if (!line.startsWith("# ")) lines.push(line);
  }
  commit();
  return sections;
}

function navigation(currentOutput) {
  const groups = categories
    .map((category) => {
      const links = pages
        .filter((page) => page.category === category.name)
        .map((page) => {
          const active = page.output === currentOutput;
          return `<li><a href="${escapeHtml(relativeHref(currentOutput, page.output))}"${active ? ' aria-current="page"' : ""}>${escapeHtml(page.title)}</a></li>`;
        })
        .join("");
      return `<section class="nav-group"><h2>${escapeHtml(category.name)}</h2><ul>${links}</ul></section>`;
    })
    .join("");

  return `<nav class="doc-nav" id="doc-navigation" aria-label="Documentation">
    <a class="nav-overview" href="${escapeHtml(relativeHref(currentOutput, "index.html"))}"${currentOutput === "index.html" ? ' aria-current="page"' : ""}>Overview</a>
    ${groups}
  </nav>`;
}

function searchDialog() {
  return `<dialog class="search-dialog" data-search-dialog aria-labelledby="search-title">
    <div class="search-frame">
      <header class="search-header">
        <svg aria-hidden="true" viewBox="0 0 24 24"><path d="m21 21-4.4-4.4m2.4-5.1a7.5 7.5 0 1 1-15 0 7.5 7.5 0 0 1 15 0Z"/></svg>
        <label class="sr-only" for="site-search" id="search-title">Search documentation</label>
        <input id="site-search" data-search-input type="search" placeholder="Search commands, protocols, or failure modes" autocomplete="off">
        <button type="button" class="search-close" data-search-close aria-label="Close search">Esc</button>
      </header>
      <div class="search-help" data-search-help>Try “transaction pooling”, “MOVED”, “XA recovery”, or “shard key”.</div>
      <div class="search-results" data-search-results aria-live="polite"></div>
    </div>
  </dialog>`;
}

function siteHeader(currentOutput) {
  const homeHref = relativeHref(currentOutput, "index.html");
  return `<header class="site-header">
    <a class="brand" href="${escapeHtml(homeHref)}" aria-label="DBProxy documentation home">
      <svg class="brand-mark" viewBox="0 0 42 32" aria-hidden="true">
        <path d="M3 7h12l6 9h18M3 25h12l6-9"/>
        <circle cx="3" cy="7" r="2"/><circle cx="3" cy="25" r="2"/><circle cx="39" cy="16" r="2"/>
      </svg>
      <span><strong>dbproxy-rs</strong><small>operator documentation</small></span>
    </a>
    <div class="header-actions">
      <button type="button" class="search-trigger" data-search-open aria-label="Search documentation">
        <svg aria-hidden="true" viewBox="0 0 24 24"><path d="m21 21-4.4-4.4m2.4-5.1a7.5 7.5 0 1 1-15 0 7.5 7.5 0 0 1 15 0Z"/></svg>
        <span>Search docs</span><kbd>/</kbd>
      </button>
      <a class="github-link" href="${githubRepository}" rel="noreferrer">GitHub <span aria-hidden="true">↗</span></a>
      <button type="button" class="menu-trigger" data-menu-toggle aria-controls="doc-navigation" aria-expanded="false">
        <span class="sr-only">Open documentation menu</span><i></i><i></i>
      </button>
    </div>
  </header>`;
}

function pageHead({ title, description, output }) {
  const assetPrefix = rootPrefix(output);
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="color-scheme" content="light">
  <meta name="theme-color" content="#102832">
  <meta name="description" content="${escapeHtml(description)}">
  <title>${escapeHtml(title)} · dbproxy-rs</title>
  <link rel="icon" href="${assetPrefix}assets/favicon.svg" type="image/svg+xml">
  <link rel="stylesheet" href="${assetPrefix}assets/styles.css">
  <script type="module" src="${assetPrefix}assets/app.js"></script>
</head>
<body data-root="${assetPrefix}">`;
}

function tableOfContents(headings) {
  const links = headings
    .filter((heading) => heading.level === 2 || heading.level === 3)
    .map((heading) => `<li class="toc-level-${heading.level}"><a href="#${escapeHtml(heading.id)}">${escapeHtml(stripMarkdown(heading.raw))}</a></li>`)
    .join("");

  if (!links) return "";
  return `<aside class="page-toc" aria-label="On this page"><h2>On this page</h2><ol>${links}</ol></aside>`;
}

function footer(currentOutput) {
  return `<footer class="site-footer">
    <span>Apache-2.0 · Rust 1.89+</span>
    <a href="${escapeHtml(relativeHref(currentOutput, "security.html"))}">Security</a>
    <a href="${escapeHtml(relativeHref(currentOutput, "contributing.html"))}">Contributing</a>
  </footer>`;
}

function renderDocument(page, markdown) {
  const rewritten = rewriteMarkdownLinks(markdown, page);
  const article = marked.parse(rewritten);
  const headings = getHeadingList();
  const currentIndex = pages.findIndex((candidate) => candidate.output === page.output);
  const previous = pages[currentIndex - 1];
  const next = pages[currentIndex + 1];
  const pager = `<nav class="page-pager" aria-label="Adjacent documentation">
    ${previous ? `<a class="pager-previous" href="${escapeHtml(relativeHref(page.output, previous.output))}"><span>Previous</span>${escapeHtml(previous.title)}</a>` : "<span></span>"}
    ${next ? `<a class="pager-next" href="${escapeHtml(relativeHref(page.output, next.output))}"><span>Next</span>${escapeHtml(next.title)}</a>` : "<span></span>"}
  </nav>`;

  return `${pageHead({ ...page, title: page.title })}
    ${siteHeader(page.output)}
    <div class="mobile-nav-backdrop" data-menu-backdrop></div>
    <div class="docs-layout">
      ${navigation(page.output)}
      <main class="doc-main" id="main-content">
        <div class="doc-kicker"><span>${escapeHtml(page.category)}</span><a href="${githubRepository}/blob/main/${page.source}">Edit this page ↗</a></div>
        <article class="prose">${article}</article>
        ${pager}
        ${footer(page.output)}
      </main>
      ${tableOfContents(headings)}
    </div>
    ${searchDialog()}
  </body>
</html>`;
}

function routeTrace() {
  return `<section class="route-console" aria-labelledby="route-console-title">
    <header>
      <div><span class="live-dot"></span><span>routing decision</span></div>
      <code>op_7J2k…mQ</code>
    </header>
    <h2 class="sr-only" id="route-console-title">Example routing decisions</h2>
    <div class="route-tabs" role="tablist" aria-label="Protocol example">
      <button type="button" role="tab" aria-selected="true" data-route-tab="mysql">MySQL</button>
      <button type="button" role="tab" aria-selected="false" data-route-tab="postgres">Postgres</button>
      <button type="button" role="tab" aria-selected="false" data-route-tab="redis">Redis</button>
    </div>
    <div class="trace-path" aria-live="polite">
      <div class="trace-node"><span>request</span><strong data-trace-request>SELECT … tenant_id = 11</strong></div>
      <div class="trace-link"><i></i><span data-trace-rule>AST key</span></div>
      <div class="trace-node trace-decision"><span>dbproxy decision</span><strong data-trace-decision>read · shard 01</strong></div>
      <div class="trace-link"><i></i><span>healthy</span></div>
      <div class="trace-node"><span>destination</span><strong data-trace-target>orders-1 / replica-b</strong></div>
    </div>
    <footer><span data-trace-policy>fallback: primary</span><strong data-trace-status>ROUTE ACCEPTED</strong></footer>
  </section>`;
}

function homeCategoryCards() {
  return categories
    .filter((category) => category.name !== "Project")
    .map((category) => {
      const categoryPages = pages.filter((page) => page.category === category.name);
      const links = categoryPages
        .slice(0, 4)
        .map((page) => `<li><a href="${escapeHtml(page.output)}"><span>${escapeHtml(page.title)}</span><span aria-hidden="true">→</span></a></li>`)
        .join("");
      return `<article class="path-card"><header><h3>${escapeHtml(category.name)}</h3><p>${escapeHtml(category.description)}</p></header><ul>${links}</ul></article>`;
    })
    .join("");
}

function renderHome() {
  const output = "index.html";
  return `${pageHead({
    title: "Route databases without hiding the hard parts",
    description: "DBProxy operator documentation for MySQL, PostgreSQL, TimescaleDB, Redis, and Valkey routing.",
    output,
  })}
    ${siteHeader(output)}
    <div class="home-mobile-navigation">${navigation(output)}</div>
    <div class="mobile-nav-backdrop" data-menu-backdrop></div>
    <main id="main-content">
      <section class="hero">
        <div class="hero-copy">
          <p class="eyebrow">Rust data-plane proxy · pre-1.0</p>
          <h1>One hop.<br><em>Explicit decisions.</em></h1>
          <p class="hero-lede">Route MySQL, PostgreSQL/TimescaleDB, and Redis/Valkey traffic across pools and shards while keeping failure boundaries visible.</p>
          <div class="hero-actions">
            <a class="button-primary" href="docs/quick-start.html">Run the quick start <span aria-hidden="true">→</span></a>
            <a class="button-secondary" href="docs/sharding-examples.html">Configure sharding</a>
          </div>
          <ul class="hero-facts" aria-label="Project traits">
            <li><span>Protocols</span><strong>MySQL · PostgreSQL · RESP</strong></li>
            <li><span>Routing</span><strong>read/write · hash · range · value</strong></li>
            <li><span>Model</span><strong>independent data-plane replicas</strong></li>
          </ul>
        </div>
        ${routeTrace()}
      </section>

      <section class="home-section paths" aria-labelledby="paths-title">
        <header class="section-heading"><p>Documentation paths</p><h2 id="paths-title">Start with the decision you need to make.</h2></header>
        <div class="path-grid">${homeCategoryCards()}</div>
      </section>

      <section class="home-section protocol-section" aria-labelledby="protocol-title">
        <header class="section-heading"><p>Protocol modes</p><h2 id="protocol-title">Preserve the wire, or terminate it for control.</h2></header>
        <div class="protocol-grid">
          <article><span class="protocol-name">MySQL</span><h3>Statement-aware by default</h3><p>Terminate the protocol for SQL parsing, prepared routing, pooled backends, cache policy, and XA fanout.</p><a href="docs/architecture.html">Read the architecture →</a></article>
          <article><span class="protocol-name">PostgreSQL / TimescaleDB</span><h3>Choose fidelity or pooling</h3><p>Use transparent relay for full protocol fidelity, or transaction mode for bounded pools and statement routing.</p><a href="docs/postgres-pooling.html">Compare the modes →</a></article>
          <article><span class="protocol-name">Redis / Valkey</span><h3>Pin a connection or route keys</h3><p>Keep RESP transparent, or use command mode for CRC16 placement and guarded multi-key operations.</p><a href="docs/redis-topology.html">Choose a topology →</a></article>
        </div>
      </section>

      <section class="home-section boundary-callout" aria-labelledby="boundary-title">
        <div><p class="eyebrow">Production contract</p><h2 id="boundary-title">Uncertainty does not become a replica read or a cross-shard guess.</h2></div>
        <div><p>Unsupported SQL shapes, topology ambiguity, metadata mismatch, and unsafe distributed operations fail closed. The boundary pages state what DBProxy can prove today.</p><div class="callout-links"><a href="docs/boundary-closure.html">Inspect boundary status</a><a href="docs/review.html">Read the assurance review</a></div></div>
      </section>

      <section class="home-section quick-command" aria-labelledby="command-title">
        <div><p>Local demo</p><h2 id="command-title">Three commands to a routed stack.</h2></div>
        <pre><code>./deploy.sh
./scripts/smoke.sh
./deploy.sh local-down</code></pre>
        <a href="docs/quick-start.html">Endpoints and expected results →</a>
      </section>
    </main>
    ${footer(output)}
    ${searchDialog()}
  </body>
</html>`;
}

function copyTree(source, destination) {
  if (!fs.existsSync(source)) return;
  fs.cpSync(source, destination, { recursive: true });
}

fs.rmSync(outputDir, { recursive: true, force: true });
fs.mkdirSync(outputDir, { recursive: true });
copyTree(staticDir, outputDir);
copyTree(path.join(repositoryDir, "config"), path.join(outputDir, "config"));
copyTree(path.join(repositoryDir, "scripts"), path.join(outputDir, "scripts"));
copyTree(path.join(repositoryDir, "charts", "dbproxy"), path.join(outputDir, "charts", "dbproxy"));
fs.copyFileSync(path.join(repositoryDir, "docker-compose.yml"), path.join(outputDir, "docker-compose.yml"));
fs.copyFileSync(path.join(repositoryDir, "LICENSE"), path.join(outputDir, "LICENSE"));
fs.writeFileSync(path.join(outputDir, ".nojekyll"), "");
fs.writeFileSync(path.join(outputDir, "index.html"), renderHome());

const searchIndex = [
  {
    title: "DBProxy overview",
    section: "Route databases without hiding the hard parts",
    category: "Start here",
    url: "index.html",
    text: "MySQL PostgreSQL TimescaleDB Redis Valkey read write routing sharding connection pools caching XA fanout metrics operator documentation",
  },
];

for (const page of pages) {
  const markdownPath = path.join(repositoryDir, page.source);
  const markdown = fs.readFileSync(markdownPath, "utf8");
  const html = renderDocument(page, markdown);
  const destination = path.join(outputDir, page.output);
  fs.mkdirSync(path.dirname(destination), { recursive: true });
  fs.writeFileSync(destination, html);
  searchIndex.push(...searchSections(page, markdown));
}

fs.writeFileSync(
  path.join(outputDir, "search-index.json"),
  `${JSON.stringify(searchIndex)}\n`,
);

console.log(`Built ${pages.length + 1} pages and ${searchIndex.length} search sections in website/dist.`);
