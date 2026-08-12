# Documentation website

The website build converts the repository's Markdown guides into a static
GitHub Pages site. Markdown files remain the content source; do not edit files
under `website/dist`.

## Local build

```bash
cd website
npm ci
npm run build
npm run check
```

Serve `website/dist` with any static file server. For example:

```bash
python3 -m http.server 4173 --directory website/dist
```

The build produces:

- a task-based landing page;
- categorized navigation and per-page tables of contents;
- a section-level client-side search index;
- responsive desktop and mobile layouts;
- local copies of referenced configuration, script, chart, and license files.

The `Documentation site` workflow deploys the generated artifact after relevant
files reach `main`. GitHub repository settings must use **GitHub Actions** as the
Pages source.
