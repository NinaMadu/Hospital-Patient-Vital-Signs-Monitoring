# Architecture decision records

One short file per decision, numbered in order: `0001-lambda-architecture.md`, `0002-simulated-clock.md`, …
Copy [0000-template.md](0000-template.md). These notes become the source for the report's architecture and tech-stack sections.

## Decisions to record on Day 1 (from the project plan)

| # | Decision | Owner of the write-up |
|---|---|---|
| 0001 | Lambda architecture (Kappa rejected) | A (argued by all) |
| 0002 | Simulated clock: 1 sim day = 300 s | A |
| 0003 | Vitals event schema | A |
| 0004 | Lab file format and atomic delivery | B |
| 0005 | Kafka topics, keys and partition counts | A |
| 0006 | Window, slide and watermark sizes | A |
| 0007 | Storage: Parquet lake + PostgreSQL | B |
| 0008 | Shared risk rules for speed and batch layers | B |
| 0009 | Pinned versions and Docker Compose layout | C |
| 0010 | Observability approach | C |
