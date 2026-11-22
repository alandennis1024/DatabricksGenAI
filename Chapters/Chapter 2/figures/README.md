# Chapter 2 figure sources

Mermaid sources for the diagrams in Chapter 2.

## Disclaimer

These are working sources, provided as-is for readers who want to reproduce
or adapt the book's diagrams. They are not final art: the publisher's
production team may redraw or restyle figures before publication, so the
printed figure may differ from what these sources render. If you reuse or
adapt a diagram, please credit the book.

## Rendering

Install the Mermaid CLI (the sources were rendered with `mmdc` 11.12.0):

```bash
npm install -g @mermaid-js/mermaid-cli
```

Render a source to PNG (white background, explicit width in pixels).
The `.mmd` extension is Mermaid too; the command is the same:

```bash
mmdc -i "anatomy_ai_solution.mmd" -o "anatomy_ai_solution.png" -b white -w 1600
```

Adjust `-w` to taste; omit it to render at the default size.

## Files

| Source | Book figure |
|---|---|
| `anatomy_ai_solution.mmd` | Anatomy of an AI solution (`anatomy_ai_solution.png`) |
| `agent-components.mmd` | Agent components |
| `single-model-application.mmd` | Single-model application |
