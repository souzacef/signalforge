# SignalForge portfolio demo

This walkthrough is designed for a focused 5–8 minute technical demonstration. The goal is to show the implemented reliability and safety choices, not to tour every endpoint or configuration option.

## Before the demo

Prepare the packaged local demo using the repository's documented Compose workflow. Bootstrap at least two users so proposer/approver separation can be demonstrated cleanly, for example an `operator` and an `admin`.

If AI enrichment is part of the demo, enable the `ai` profile with a valid Gemini API key ahead of time and make sure at least one incident already has a persisted enrichment result. If remediation execution is demonstrated, use only an intentionally trusted local or test endpoint in the remediation allowlist. Do not point the worker at a real production control plane for a portfolio demo.

## 1. Start with Operations Overview

**Show:** the authenticated Overview page with counts for open, acknowledged, critical-open, and pending-approval records, plus the recent incident and remediation previews.

**Say:** the overview is deliberately assembled from current domain APIs rather than from a second analytics model. It gives the operator a quick operational entry point without inventing trends or historical metrics the backend does not track.

**Do not claim:** that the dashboard is a real-time analytics or incident-intelligence system.

## 2. Open the incident queue

**Show:** filters, server-side pagination, severity/status badges, and a few incidents with realistic titles and sources.

**Say:** filtering and pagination are server-backed, and filter/page state is kept in the URL so list-to-detail navigation preserves operator context.

**Engineering point:** the console is not a static mock. It is a typed Angular client over the authenticated FastAPI domain APIs.

## 3. Open an incident and explain deterministic triage

**Show:** an incident detail page with its lifecycle state and persisted triage result.

**Say:** triage is deterministic and authoritative. The incident consumer derives priority from the immutable incident event snapshot, persists the processing receipt and triage result transactionally, and treats duplicate delivery idempotently.

A compact explanation is enough:

> RabbitMQ delivery is at-least-once, so SignalForge assumes duplicates can happen. The durable consumer receipt makes redelivery safe instead of pretending delivery is exactly once.

**Do not claim:** exactly-once messaging.

## 4. Show AI enrichment as advisory context

**Show:** a persisted enrichment result on the same incident, if available.

**Say:** Gemini is intentionally downstream of deterministic triage. It can add advisory context and investigation guidance, but it cannot change priority, human-review requirements, incident state, or trigger remediation.

**Engineering point:** model availability and model quality are kept outside the correctness boundary. Incident intake and deterministic triage remain useful when AI is unavailable.

**Do not claim:** autonomous incident resolution, AI-driven prioritization, or RAG. RAG is not implemented in the current release.

## 5. Demonstrate the lifecycle

**Show:** acknowledge an open incident, then resolve an acknowledged incident when the current state permits it.

**Say:** lifecycle transitions are server-authoritative and intentionally narrow: `open -> acknowledged -> resolved`. The UI does not optimistically invent state, and arbitrary status editing or reopening is not exposed.

**Engineering point:** invalid transitions return conflict rather than being repaired client-side.

## 6. Open the remediation queue

**Show:** pending proposals and proposal detail.

**Say:** remediation is modeled as a separate human-reviewed workflow. Today the supported action is deliberately narrow, `restart_service`, against a logical target.

If useful, create a proposal as the operator account and show that the authenticated user is recorded as proposer rather than supplied by the client.

## 7. Show proposer/approver separation

**Show:** the proposal as the user who created it, then switch to the admin account for approval.

**Say:** the proposer cannot approve their own proposal. This keeps proposal creation separate from authorization of the action. Operators can withdraw their own pending proposal, while admins can reject pending proposals and approve another user's proposal.

**Engineering point:** this separation is enforced on the backend, not merely hidden in the frontend.

## 8. Emphasize that approval is not execution

**Show:** an approved proposal before execution is requested.

**Say:** approval records the human decision only. It does not publish an execution event and does not call an external actuator. Execution requires a second explicit admin action.

This is one of the most important boundaries to make visible in the demo.

## 9. Request controlled execution

Only perform this step against an intentionally trusted demo/test endpoint configured in the allowlist.

**Show:** the confirmation UI, request execution, and then the durable execution status.

**Say:** the execute endpoint commits one execution record and one `remediation.execution.requested` outbox event in the same PostgreSQL transaction. The outbox dispatcher publishes it asynchronously to RabbitMQ, and the remediation worker resolves the logical target through an operator-controlled allowlist.

**Engineering point:** proposal targets are not interpreted as arbitrary URLs or shell commands.

## 10. Explain `failed` vs `outcome_unknown`

You do not need to manufacture an ambiguous failure live. Show the status vocabulary in the UI or explain it from an existing record.

**Say:** SignalForge does not blindly retry a potentially completed external side effect. A timeout or interrupted request may leave the actual restart result unknowable, so the durable model distinguishes `outcome_unknown` from `failed` and `succeeded`.

A useful one-line explanation:

> If I cannot prove the restart did not happen, retrying automatically may be less safe than surfacing uncertainty to the operator.

Also mention that the frontend reconciles durable execution state after an ambiguous request response instead of immediately offering another POST.

## 11. Briefly show observability

If the observability profile is running, finish with the Grafana SignalForge Overview dashboard and one Tempo trace.

**Show:** API and pipeline metrics, optional worker panels, and a distributed trace spanning API, outbox publication, consumer processing, and either Gemini or remediation when available.

**Say:** metrics use bounded labels and avoid incident IDs, target names, endpoints, credentials, and error strings as labels. Tracing is optional and does not participate in application readiness.

Keep this portion short. The purpose is to show that the asynchronous architecture can be inspected, not to turn the demo into a Grafana tutorial.

## Suggested closing

A concise closing summary for a technical reviewer:

> SignalForge is intentionally small in product scope, but the workflow is complete: authenticated incident intake, transactional event publication, at-least-once-safe consumers, deterministic triage, advisory AI, human-reviewed remediation, explicit handling of ambiguous external effects, observability, and a production-packaged Angular console.

## What not to overclaim

- Do not call the system production-ready or battle-tested.
- Do not claim exactly-once message delivery or exactly-once actuator execution.
- Do not describe AI as authoritative or autonomous.
- Do not claim RAG, Kubernetes, Terraform, Helm, AWS deployment, centralized logging, or production alerting are implemented.
- Do not describe the remediation worker as safe for arbitrary endpoints. It relies on an explicit trusted allowlist and is opt-in.

## Suggested screenshot set

Capture these from the real running application in the next portfolio pass. Avoid mock data that looks obviously artificial, but do not expose real credentials, secrets, private URLs, or sensitive host information.

1. **Operations Overview**
   - State: populated metrics plus both recent-preview panels.
   - Why: gives a recruiter or engineer an immediate sense of a finished operator console rather than a collection of APIs.

2. **Incident detail with deterministic triage and AI enrichment**
   - State: an incident with meaningful title/severity, persisted priority, and a completed advisory enrichment.
   - Why: visually communicates the project's central deterministic-vs-AI boundary.

3. **Incident queue**
   - State: several incidents, one or two active filters, pagination visible.
   - Why: shows practical operational UX and the breadth of the incident model without requiring explanation.

4. **Pending remediation proposal detail**
   - State: proposer attribution and approval/rejection controls visible for an appropriate reviewer account.
   - Why: makes human-in-the-loop review and role separation tangible.

5. **Approved proposal with execution status**
   - State: separate execution request/status area, ideally a safe terminal demo result.
   - Why: demonstrates that approval and execution are intentionally separate phases.

6. **Grafana / Tempo view**
   - State: SignalForge Overview dashboard or a readable distributed trace spanning the asynchronous pipeline.
   - Why: backs up the observability story with a real operational view rather than another architecture diagram.

The README should use only the strongest 3–4 screenshots once captured. The remaining images can stay in the demo documentation if they add useful depth.
