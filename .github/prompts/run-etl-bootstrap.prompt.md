---
mode: agent
tools: ["run_in_terminal"]
description: "Use when: you want to restart the ETL project stack and trigger the DAG in one command."
---

Run this from the repository root to relaunch the full project stack and trigger the DAG:

sudo bash scripts/bootstrap_and_run.sh

Then summarize:

1. docker compose service status,
2. latest DAG run state,
3. any failing task with next fix command.
