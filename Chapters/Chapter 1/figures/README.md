# Chapter 1 figure sources

Mermaid sources for the diagrams in Chapter 1.

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

Render a source to PNG (white background, explicit width in pixels):

```bash
mmdc -i "Rag.mermaid" -o "Rag.png" -b white -w 2400
```

Adjust `-w` to taste; omit it to render at the default size.

## Files

| Source | Book figure |
|---|---|
| `areas of CS.mermaid` | Areas of computer science (`areasofCS.png`) |
| `Types of Generative Models.mermaid` | Subfields of AI (`subfields_of_AI.png`) |
| `Relative Costs and Complexity.mermaid` | Relative costs and complexity (`RelativeCostsandComplexity.png`) |
| `Rag.mermaid` | RAG (`Rag.png`) |
| `Fine Tuning.mermaid` | Fine-tuning (`FineTuning.png`) |
| `Pretraining.mermaid` | Pretraining (`Pretraining.png`) |
| `Agent High Level.mermaid` | Agent, high level (`AgentHighLevel.png`) |
| `Agent Components.mermaid` | Agent components (`AgentComponents.png`) |
| `Guided Agent.mermaid` | Guided agent (`GuidedAgent.png`) |
| `Self-Directed Agent.mermaid` | Self-directed agent (`Self-DirectedAgent.png`) |
