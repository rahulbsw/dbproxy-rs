const root = document.body.dataset.root || "./";
const menuToggle = document.querySelector("[data-menu-toggle]");
const menuBackdrop = document.querySelector("[data-menu-backdrop]");
const documentationNav = document.querySelector(".doc-nav");
const mobileNavigation = window.matchMedia("(max-width: 60rem)");

function setMenu(open) {
  document.body.classList.toggle("menu-open", open);
  menuToggle?.setAttribute("aria-expanded", String(open));
  menuToggle?.setAttribute("aria-label", open ? "Close documentation menu" : "Open documentation menu");
  if (documentationNav && mobileNavigation.matches) {
    documentationNav.inert = !open;
    documentationNav.setAttribute("aria-hidden", String(!open));
  }
}

function syncNavigationMode() {
  if (!documentationNav) return;
  if (mobileNavigation.matches) {
    setMenu(false);
  } else {
    document.body.classList.remove("menu-open");
    documentationNav.inert = false;
    documentationNav.removeAttribute("aria-hidden");
    menuToggle?.setAttribute("aria-expanded", "false");
  }
}

menuToggle?.addEventListener("click", () => {
  setMenu(!document.body.classList.contains("menu-open"));
});
menuBackdrop?.addEventListener("click", () => setMenu(false));
document.querySelectorAll(".doc-nav a").forEach((link) => {
  link.addEventListener("click", () => setMenu(false));
});
mobileNavigation.addEventListener("change", syncNavigationMode);
syncNavigationMode();

const routeExamples = {
  mysql: {
    request: "SELECT … WHERE tenant_id = 11",
    rule: "AST key",
    decision: "read · shard 01",
    target: "orders-1 / replica-b",
    policy: "fallback: primary",
    status: "ROUTE ACCEPTED",
  },
  postgres: {
    request: "UPDATE users … WHERE userid = $1",
    rule: "typed bind",
    decision: "write · shard 08",
    target: "users-8 / primary",
    policy: "transaction: pinned",
    status: "ROUTE ACCEPTED",
  },
  redis: {
    request: "MGET user:12 user:98",
    rule: "CRC16 keys",
    decision: "bounded fanout · 2 slots",
    target: "slot 139 / slot 9214",
    policy: "merge: input order",
    status: "PLAN ACCEPTED",
  },
};

const routeConsole = document.querySelector(".route-console");
document.querySelectorAll("[data-route-tab]").forEach((tab) => {
  tab.addEventListener("click", () => {
    const example = routeExamples[tab.dataset.routeTab];
    if (!example || !routeConsole) return;
    document.querySelectorAll("[data-route-tab]").forEach((candidate) => {
      candidate.setAttribute("aria-selected", String(candidate === tab));
    });
    for (const [key, value] of Object.entries(example)) {
      const target = routeConsole.querySelector(`[data-trace-${key}]`);
      if (target) target.textContent = value;
    }
    routeConsole.classList.remove("is-changing");
    requestAnimationFrame(() => routeConsole.classList.add("is-changing"));
  });
});

document.querySelectorAll(".prose pre").forEach((block) => {
  const code = block.querySelector("code");
  if (!code) return;
  const button = document.createElement("button");
  button.type = "button";
  button.className = "copy-code";
  button.textContent = "Copy";
  button.setAttribute("aria-label", "Copy code block");
  button.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(code.textContent || "");
      button.textContent = "Copied";
    } catch {
      button.textContent = "Select text";
    }
    window.setTimeout(() => { button.textContent = "Copy"; }, 1600);
  });
  block.append(button);
});

const tocLinks = [...document.querySelectorAll(".page-toc a")];
if (tocLinks.length && "IntersectionObserver" in window) {
  const linksById = new Map(tocLinks.map((link) => [decodeURIComponent(link.hash.slice(1)), link]));
  const observer = new IntersectionObserver((entries) => {
    const visible = entries
      .filter((entry) => entry.isIntersecting)
      .sort((a, b) => a.boundingClientRect.top - b.boundingClientRect.top)[0];
    if (!visible) return;
    tocLinks.forEach((link) => link.classList.remove("is-active"));
    linksById.get(visible.target.id)?.classList.add("is-active");
  }, { rootMargin: "-15% 0px -72%", threshold: 0 });
  linksById.forEach((_, id) => {
    const heading = document.getElementById(id);
    if (heading) observer.observe(heading);
  });
}

const dialog = document.querySelector("[data-search-dialog]");
const searchInput = document.querySelector("[data-search-input]");
const resultsNode = document.querySelector("[data-search-results]");
const helpNode = document.querySelector("[data-search-help]");
let searchIndex;
let selectedResult = -1;

async function loadSearchIndex() {
  if (searchIndex) return searchIndex;
  const response = await fetch(`${root}search-index.json`);
  if (!response.ok) throw new Error(`search index returned ${response.status}`);
  searchIndex = await response.json();
  return searchIndex;
}

function tokenize(value) {
  return value.toLowerCase().trim().split(/[^a-z0-9_.$/-]+/).filter(Boolean);
}

function occurrenceCount(haystack, needle) {
  let count = 0;
  let position = 0;
  while ((position = haystack.indexOf(needle, position)) !== -1) {
    count += 1;
    position += needle.length;
  }
  return count;
}

function scoreEntry(entry, terms) {
  const title = entry.title.toLowerCase();
  const section = entry.section.toLowerCase();
  const text = entry.text.toLowerCase();
  let score = 0;
  for (const term of terms) {
    if (!title.includes(term) && !section.includes(term) && !text.includes(term)) return 0;
    if (title === term) score += 50;
    if (title.includes(term)) score += 24;
    if (section.includes(term)) score += 15;
    score += Math.min(occurrenceCount(text, term), 8);
  }
  return score;
}

function resultSnippet(text, terms) {
  const lower = text.toLowerCase();
  const positions = terms.map((term) => lower.indexOf(term)).filter((position) => position >= 0);
  const first = positions.length ? Math.min(...positions) : 0;
  const start = Math.max(0, first - 65);
  const end = Math.min(text.length, start + 185);
  return `${start ? "…" : ""}${text.slice(start, end).trim()}${end < text.length ? "…" : ""}`;
}

function setSelectedResult(index) {
  const links = [...resultsNode.querySelectorAll(".search-result")];
  if (!links.length) return;
  selectedResult = (index + links.length) % links.length;
  links.forEach((link, linkIndex) => link.classList.toggle("is-selected", linkIndex === selectedResult));
  links[selectedResult].scrollIntoView({ block: "nearest" });
}

function renderResults(entries, terms) {
  resultsNode.replaceChildren();
  selectedResult = -1;
  if (!entries.length) {
    const empty = document.createElement("p");
    empty.className = "search-empty";
    empty.textContent = "No documentation matched. Try a protocol, command, metric, or failure mode.";
    resultsNode.append(empty);
    return;
  }

  for (const entry of entries) {
    const link = document.createElement("a");
    link.className = "search-result";
    link.href = `${root}${entry.url}`;

    const title = document.createElement("strong");
    title.textContent = entry.section === "Overview" ? entry.title : `${entry.title} · ${entry.section}`;
    const category = document.createElement("span");
    category.className = "result-category";
    category.textContent = entry.category;
    const snippet = document.createElement("p");
    snippet.textContent = resultSnippet(entry.text, terms);
    link.append(title, category, snippet);
    resultsNode.append(link);
  }
}

async function runSearch(query) {
  const terms = tokenize(query);
  if (!terms.length) {
    resultsNode.replaceChildren();
    helpNode.hidden = false;
    return;
  }

  helpNode.hidden = true;
  try {
    const index = await loadSearchIndex();
    const matches = index
      .map((entry) => ({ entry, score: scoreEntry(entry, terms) }))
      .filter((candidate) => candidate.score > 0)
      .sort((a, b) => b.score - a.score || a.entry.title.localeCompare(b.entry.title))
      .slice(0, 12)
      .map((candidate) => candidate.entry);
    renderResults(matches, terms);
  } catch {
    resultsNode.replaceChildren();
    const error = document.createElement("p");
    error.className = "search-empty";
    error.textContent = "Search index could not be loaded. Refresh the page and try again.";
    resultsNode.append(error);
  }
}

function openSearch() {
  if (!dialog) return;
  if (typeof dialog.showModal === "function") dialog.showModal();
  else dialog.setAttribute("open", "");
  window.setTimeout(() => searchInput?.focus(), 0);
  loadSearchIndex().catch(() => {});
}

function closeSearch() {
  if (!dialog) return;
  if (typeof dialog.close === "function") dialog.close();
  else dialog.removeAttribute("open");
}

document.querySelectorAll("[data-search-open]").forEach((button) => button.addEventListener("click", openSearch));
document.querySelector("[data-search-close]")?.addEventListener("click", closeSearch);
dialog?.addEventListener("click", (event) => {
  if (event.target === dialog) closeSearch();
});
searchInput?.addEventListener("input", () => runSearch(searchInput.value));
searchInput?.addEventListener("keydown", (event) => {
  const links = [...resultsNode.querySelectorAll(".search-result")];
  if (event.key === "ArrowDown") {
    event.preventDefault();
    setSelectedResult(selectedResult + 1);
  } else if (event.key === "ArrowUp") {
    event.preventDefault();
    setSelectedResult(selectedResult - 1);
  } else if (event.key === "Enter" && selectedResult >= 0) {
    event.preventDefault();
    links[selectedResult]?.click();
  }
});

document.addEventListener("keydown", (event) => {
  const target = event.target;
  const isTyping = target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement || target?.isContentEditable;
  if ((event.key === "/" && !isTyping) || ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k")) {
    event.preventDefault();
    openSearch();
  }
  if (event.key === "Escape") {
    setMenu(false);
  }
});
