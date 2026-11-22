# Decision record: <capability name>

Write one record for each new capability, before it reaches production.
Keep it in version control next to the code, and update it when the
capability changes. Replace every `<...>` placeholder.

| Field | Entry |
|---|---|
| Capability | <what the system will do, in one sentence> |
| Status | Proposed / Approved / Retired |
| Date and version | <YYYY-MM-DD, v1> |

## Intended use

<Who uses it, for which decision, and what the system may do.>

## Excluded use

<What the capability must never do, even if asked or technically able.>

## Owner

<Named person or team accountable for behavior, incidents, and retirement.>

## Data classification

<Data it reads and writes, with the classification of each source. Include
retention for traces and cached context.>

## Threat model

<Abuse paths: indirect prompt injection, cross-tenant retrieval, tool
parameter smuggling, excessive agency, denial of service. For each, name
the control that sits outside the model.>

## Evaluation evidence

<Evaluation sets and runs that justify release: smoke, regression,
adversarial, and quality results, with thresholds and run links.>

## Operational SLOs

<Targets for quality, reliability, latency, and cost per successful task.>

## Approval boundary

<Which actions run automatically, which need a person, who may approve,
and how long an approval lasts.>

## Rollback

<How to turn the capability off or back to the previous version, who may do
it, and how long it takes. Include compensation for actions already taken.>

## Review

<When the record is next reviewed, and what change forces an earlier
review, such as a new tool, data source, model, or user group.>
