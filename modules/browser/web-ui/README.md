# Web UI

React browser application for conversations, confirmations, execution graph, files,
media/site progress, account settings and the sandbox inspector.

Active transcript output keeps one Turn-scoped cumulative public-character
counter mounted across tool calls. Harness round details animate content-free
character totals received from the API's live model-progress projection.

- Build: `npm --prefix web run build`
- Artifact: `web/dist` with a configured distribution digest
- Verification: Vitest, production build and desktop/mobile Playwright checks
