# Runtime Skill Bundle

The immutable Runtime bundle contains 33 filesystem Skills. The exact source
commit, content digest, license, dependency profile and test case for each is
recorded in `runtime/skills/manifest.json`; `sources.json` is its editable
source.

| Skill/source | Bundled scope |
| --- | --- |
| `github/awesome-copilot` | `doc-and-modernize` |
| `Agents365-ai/drawio-skill` | `drawio-skill` |
| `wheresryan22/anatomy` | committed Skill, kit, examples, references and scripts |
| `cathrynlavery/diagram-design` | `diagram-design` |
| `ningzimu/image-to-editable-ppt-skill` | Skill plus `editppt` CLI source |
| `ningzimu/codex-ppt-skill` | `codex-ppt` |
| `obra/superpowers` | `brainstorming`, `systematic-debugging`, `verification-before-completion`, `writing-plans` |
| `nimrodfisher/data-analytics-skills` | `programmatic-eda`, `data-quality-audit`, `query-validation` |
| `anthropics/skills` | all 19 directories containing `SKILL.md` |
| `vj-distributed-media-edit-system` | `manim-video` with pinned Manim/LaTeX runtime dependencies |

Every source is pinned by commit and copied into the repository; builds do not
fetch Skill text. The upstream `doc-coauthoring` folder is the only directory
whose upstream tree does not declare a license; the manifest records that fact
rather than assigning one.

Shared dependencies include LibreOffice Writer/Calc/Impress, Poppler, Pandoc,
qpdf, Tesseract English/Simplified Chinese, Graphviz, draw.io Desktop,
Playwright/Chromium, document/PDF/spreadsheet Python libraries, `editppt`, and
the Node libraries named by the office and Anatomy Skills. The data workflows
use pinned NumPy, pandas and sqlglot packages. Debian, PyPI, npm
and Playwright dependencies use domestic mirrors. The large draw.io `.deb` is
an ignored build input verified by upstream SHA-256 before installation.

Skill instructions cannot widen a Turn's capability, network policy,
credentials, filesystem ownership or approval boundary. Provider tasks still
use scoped Gateway routes and budgets.
