# NFL Lakehouse

Study project for the Databricks Certified Data Engineer Professional exam.

## Architecture
ESPN public API → ingest job → UC Volume (landing) → Lakeflow pipeline
(bronze → silver → gold) → deployed with Databricks Asset Bundles + GitHub Actions.

## Stack
Azure Databricks (serverless, Unity Catalog), ADLS Gen2, Lakeflow Declarative Pipelines,
Asset Bundles, GitHub Actions.

## Repo layout
(short version of the tree from section 2)

## Setup
1. One-time Unity Catalog setup: `setup/00_unity_catalog.sql`
2. Deploy: (Phase 1)

## Progress
- [x] Phase 0: Azure, storage, Unity Catalog, CLI, repo
- [ ] Phase 1: first bundle
- [ ] ...