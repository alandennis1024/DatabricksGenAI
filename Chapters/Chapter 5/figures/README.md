# Chapter 5 figure sources

Mermaid source for the human-AI interaction diagram in Chapter 5.

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

Render the source to PNG (white background, explicit width in pixels):

```bash
mmdc -i "Human-AI Interaction.mermaid" -o "human_ai_interaction.png" -b white -w 1600
```

Adjust `-w` to taste; omit it to render at the default size.

## Files

| Source | Book figure |
|---|---|
| `Human-AI Interaction.mermaid` | The human-AI interaction continuum (<<humanaiinteractioncontinuum>>) |
