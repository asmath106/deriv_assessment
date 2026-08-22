# Part 3 — TL Extension

> Not yet worked through — this section still needs your reasoning. Headers below follow the brief's required sub-questions as a scaffold only.

## 3a. Unified Real-Time and Batch Architecture

The business needs a real-time fraud-detection signal firing within seconds of each deposit event, while the existing batch analytics pipeline continues to serve the weekly C-suite report — and both internal systems and an external partner API need to consume the data.

- Tooling choice and reasoning: _TBD_
- How the real-time and batch paths coexist without one blocking the other: _TBD_
- Latency vs. consistency trade-off, and where eventual consistency is explicitly accepted: _TBD_
- How the external API consumer gets a consistent, secure view: _TBD_

## 3b. Build vs. Buy

A new third-party payment processor needs to be onboarded as a data source — build a custom connector, or use an integration platform (e.g. Fivetran, RudderStack).

- Decision criteria: _TBD_
- Concrete conditions for when each option wins: _TBD_
- Recommendation for this specific case, and what would change your mind: _TBD_
