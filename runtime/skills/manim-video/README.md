# Manim Video Skill

Production pipeline for mathematical and technical animations using [Manim Community Edition](https://www.manim.community/).

## What it does

Creates 3Blue1Brown-style animated videos from text prompts. The agent handles the full pipeline: creative planning, Python code generation, rendering, scene stitching, and iterative refinement.

## Use cases

- **Concept explainers** — "Explain how neural networks learn"
- **Equation derivations** — "Animate the proof of the Pythagorean theorem"
- **Algorithm visualizations** — "Show how quicksort works step by step"
- **Data stories** — "Animate our before/after performance metrics"
- **Architecture diagrams** — "Show our microservice architecture building up"

## Prerequisites

The selected Runtime image must provide pinned Manim CE 0.20.1, LaTeX,
dvisvgm and ffmpeg. Use the bundled check before rendering:

```bash
bash skills/manim-video/scripts/setup.sh
```

Do not install dependencies from a running sandbox. Image builds use the
configured domestic Debian and PyPI mirrors and verify the resulting toolchain
before publishing the image.
