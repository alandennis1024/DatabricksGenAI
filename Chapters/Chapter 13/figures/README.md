# Chapter 13 figure sources

Mermaid sources for the deployment diagrams in Chapter 13. The chapter's
PNGs are generated from these sources — edit the source, re-render, and
commit both together so they cannot drift.

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

Render each source to its PNG (white background; widths as used in the
book repo):

```bash
mmdc -i "CICD_Deployment_Architecture.mermaid" -o "cicd_deployment_architecture.png" -b white -w 1600
mmdc -i "CICD_Model_Deployment_Flow.mermaid"  -o "model_deployment_flow.png"       -b white -w 1600
mmdc -i "Databricks_Deployment_Stack.mermaid"  -o "DatabricksDeploymentStack-larger.png" -b white -w 2400
mmdc -i "DABS vs MLOps Stacks.mermaid"         -o "DABSvsMLOpsStacks.png"          -b white
```

Adjust `-w` to taste; omit it to render at the default size.

## Files

| Source | Book figure |
|---|---|
| `Databricks_Deployment_Stack.mermaid` | The Databricks deployment stack (`DatabricksDeploymentStack-larger.png`) |
| `CICD_Deployment_Architecture.mermaid` | End-to-end CI/CD deployment architecture (`cicd_deployment_architecture.png`) |
| `CICD_Model_Deployment_Flow.mermaid` | Model deployment flow (`model_deployment_flow.png`) |
| `DABS vs MLOps Stacks.mermaid` | MLOps Stacks and DABs decision tree (`DABSvsMLOpsStacks.png`) |
