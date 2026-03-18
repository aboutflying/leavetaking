# Political Purchaser

## Project Overview

A browser extension + backend system that shows users the political spending trail behind
products they browse on Amazon. Unlike existing tools (Goods Unite Us, Progressive Shopper),
this system is **configurable by policy issue** rather than simple party alignment, and it
**exposes the full money trail as a visual graph** so users can see exactly why a product
gets a particular score.

## Architecture

### Data Pipeline (batch, runs weekly)

A Python pipeline that:

1. **Brand Resolution**: Maps Amazon brand names to corporate legal entities using
   Wikidata SPARQL queries and OpenCorporates API lookups. Outputs Brand -> Corporation
   OWNED_BY edges and Corporation -> Corporation SUBSIDIARY_OF edges.

2. **FEC Contribution Ingestion** (Tier 1 — implemented): Streams FEC bulk data files
   using Python generators. Loads: Committee Master (cm), Candidate Master (cn),
   Committee-to-Candidate contributions (pas2, filtered to support-only 24K/24Z
   transaction types), and Candidate-Committee Linkage (ccl). Corporate PACs are
   identified by `connected_org_name` (non-empty) or `interest_group_category == 'C'`.
   Amendment deduplication via Neo4j MERGE on `tran_id` (last-write-wins). Individual
   contributions (indiv) and executive donation tracking are Tier 2 (deferred).

3. **Scorecard Ingestion**: Pulls legislative scorecards from organizations (ACLU, EFF,
   League of Conservation Voters, etc.) and loads as SCORED/RATES edges linking candidates
   to issue dimensions.

4. **Score Pre-computation**: Traverses the graph to compute per-brand, per-issue scores.
   Flattens results into a lookup table (JSON/SQLite) that the browser extension caches locally
   for fast badge rendering on Amazon pages.

### Graph Database (Neo4j)

Neo4j Community Edition is the source of truth. The graph model enables:
- Variable-length ownership traversal (brand -> ultimate parent)
- Multi-hop money trail queries (brand -> corp -> PAC -> candidate -> issue)
- Visual exploration during development via Neo4j Browser
- Live "show me the trail" queries from the extension detail view

### Browser Extension (Chrome, Manifest V3)

- Content scripts inject score badges on Amazon search results and product pages
- Badges are rendered from locally cached pre-computed scores (fast, offline-capable)
- Clicking a badge opens a detail view showing the full graph trail (live query to backend API)
- Settings page for configuring issue preferences and scorecard trust

### API Layer

Lightweight API (FastAPI) that:
- Serves pre-computed score lookups for the extension
- Runs live Cypher queries for the graph visualization detail view
- Handles extension config/preferences

## Graph Data Model

### Node Types

| Node       | Key Properties                                              | Source              |
|------------|-------------------------------------------------------------|---------------------|
| Brand      | name, amazon_slug, aliases[]                                | Amazon scrape       |
| Corporation| name, ticker, cik, fec_committee_id, jurisdiction, oc_id    | OpenCorporates, SEC |
| Person     | name, title, fec_contributor_id                             | FEC bulk data       |
| Candidate  | name, fec_candidate_id, office, state, party                | FEC bulk data       |
| Committee  | name, fec_committee_id, type                                | FEC bulk data       |
| Issue      | name, description                                           | Manual/config       |
| Scorecard  | org_name, year, methodology_url                             | Scorecard sources   |

### Edge Types

```
(:Brand)-[:OWNED_BY]->(:Corporation)
(:Corporation)-[:SUBSIDIARY_OF]->(:Corporation)
(:Corporation)-[:OPERATES_PAC]->(:Committee)
(:Person)-[:EXECUTIVE_OF]->(:Corporation)
(:Person)-[:DONATED_TO {amount, date, cycle}]->(:Committee | :Candidate)
(:Committee)-[:CONTRIBUTED_TO {tran_id, amount, date, cycle}]->(:Candidate)
(:Candidate)-[:AUTHORIZED_COMMITTEE {linkage_id, designation, type}]->(:Committee)
(:Candidate)-[:SCORED {score, year}]->(:Issue)
(:Scorecard)-[:RATES {score, year}]->(:Candidate)
(:Scorecard)-[:COVERS]->(:Issue)
```

### Key Query Pattern

The core traversal from brand to weighted issue scores:

```cypher
MATCH (b:Brand {name: $brandName})-[:OWNED_BY]->(:Corporation)-[:SUBSIDIARY_OF*0..10]->(corp:Corporation)
MATCH (corp)-[:OPERATES_PAC]->(:Committee)-[:CONTRIBUTED_TO]->(cand:Candidate)
MATCH (sc:Scorecard)-[:RATES {score: score}]->(cand)
MATCH (sc)-[:COVERS]->(issue:Issue)
WHERE issue.name IN $userIssues
  AND sc.org_name IN $trustedScorecards
RETURN issue.name,
       AVG(score) AS avg_candidate_score,
       SUM(cand.total_received) AS total_money_flow
```

A second path handles executive individual donations:

```cypher
MATCH (b:Brand {name: $brandName})-[:OWNED_BY]->(:Corporation)-[:SUBSIDIARY_OF*0..10]->(corp:Corporation)
MATCH (p:Person)-[:EXECUTIVE_OF]->(corp)
MATCH (p)-[:DONATED_TO]->(cand:Candidate)
MATCH (sc:Scorecard)-[:RATES {score: score}]->(cand)
MATCH (sc)-[:COVERS]->(issue:Issue)
WHERE issue.name IN $userIssues
  AND sc.org_name IN $trustedScorecards
RETURN issue.name,
       AVG(score) AS avg_candidate_score,
       SUM(p.donation_amount) AS total_money_flow
```

## Data Sources

### Free / Open

- **OpenFEC API** (api.open.fec.gov): FEC campaign finance data. Free API key, 1000 req/hr
  with demo key. Also available as bulk CSV downloads from fec.gov/data/browse-data.
- **FEC Bulk Data**: https://www.fec.gov/data/browse-data/?tab=bulk-data
  Tier 1 files: committee master (cm), candidate master (cn), committee-to-candidate
  contributions (pas2), candidate-committee linkage (ccl). Tier 2 (deferred): individual
  contributions (indiv) for executive donation tracking.
- **Wikidata SPARQL** (query.wikidata.org): Corporate ownership/subsidiary relationships.
  Properties: P749 (parent organization), P355 (subsidiary), P1128 (employees),
  P414 (stock exchange), P946 (ISIN). Free, no auth needed.
- **OpenCorporates API** (api.opencorporates.com): Company registry data across 145
  jurisdictions. Free tier available, includes parent company and subsidiary statements.
- **ProPublica Congress API**: Vote records, bill data, member info. Free API key.
- **VoteSmart API**: Voting records, interest group ratings, candidate bios.

### Scorecard Sources (for issue scoring)

- ACLU (civil liberties) - annual congressional scorecard
- EFF (digital rights/privacy) - not a formal scorecard, but endorsements and bill positions
- League of Conservation Voters (environment) - annual National Environmental Scorecard
- Human Rights Campaign (LGBTQ+ rights) - congressional scorecard
- AFL-CIO (labor) - legislative scorecard
- NumbersUSA (immigration - restrictionist perspective)
- Heritage Action (conservative policy baseline)
- NRA (gun rights) / Giffords (gun safety)

These provide opposing viewpoints on the same issues. Users choose which scorecards
they trust, which is how configurability works without the system taking a political stance.

## Design Decisions

- **Graph DB from day 1** (Neo4j Community Edition) because the "show me the money trail"
  visualization is a core differentiating feature, not an afterthought.
- **Pre-computed weekly scores** for extension badges. The FEC data doesn't change in
  real-time, and weekly refresh matches filing cadence.
- **Amazon-primary** for MVP. Single DOM to parse reduces extension complexity.
- **Scorecard-based issue scoring** rather than party labels. This is what makes the tool
  configurable and politically neutral in framing.
- **Separate Brand and Corporation nodes** because brands are consumer-facing labels and
  corporations are legal entities. One corp can own many brands.
- **Temporal properties on edges** (date, cycle) to support filtering by time window
  and showing trend data.
- **Dark money is a known blind spot.** 501(c)(4) donations are not disclosed to the FEC
  and cannot be tracked. The system should be transparent about this limitation in the UI.

## Tech Stack

- **Graph DB**: Neo4j Community Edition (Docker)
- **Pipeline**: Python 3.12+, neo4j driver, requests, pandas (for FEC bulk CSV processing)
- **API**: FastAPI
- **Extension**: Chrome Manifest V3, vanilla JS or lightweight framework
- **Graph Visualization**: D3.js force-directed graph or vis.js Network (evaluate both)
- **Pre-computed cache**: JSON file or SQLite, bundled with extension updates

## Project Structure

```
political-purchaser/
  CLAUDE.md              # This file
  README.md              # User-facing documentation
  docker-compose.yml     # Neo4j + API services
  pyproject.toml         # Python project config
  schema/
    constraints.cypher   # Neo4j schema constraints and indexes
    seed_issues.cypher   # Seed data for Issue nodes and initial Scorecard nodes
  pipeline/
    __init__.py
    config.py            # API keys, Neo4j connection, file paths
    fetchers/            # Data source fetchers (FEC, OpenCorporates, Wikidata, scorecards)
    processors/          # Data cleaning, entity resolution, score computation
    loaders/             # Neo4j graph loaders
    run_pipeline.py      # Orchestrator: fetch -> process -> load -> pre-compute
  api/
    main.py              # FastAPI app
    routes/              # Score lookup, graph trail query, config endpoints
  extension/
    manifest.json
    content.js           # Amazon page injection
    popup.html/js        # Settings and detail view
    scores.json          # Pre-computed score cache
  scripts/
    bootstrap_brands.py  # One-time: build initial brand -> corp mapping
    export_scores.py     # Export pre-computed scores for extension
  tests/
    test_pipeline.py
    test_queries.py
```

## MVP Milestones

1. ✅ Neo4j running in Docker, schema constraints applied, seed Issue/Scorecard nodes loaded
2. ⬜ Brand resolution pipeline: top 500 Amazon brands mapped to corporate parents via Wikidata
3. ✅ FEC Tier 1 pipeline: PAC contributions (cm, cn, pas2, ccl) loaded with streaming + filters
4. ⬜ FEC Tier 2 pipeline: executive individual donations (indiv) linked via Person nodes
5. ⬜ Scorecard pipeline: at least 3 scorecards loaded (ACLU, LCV, one conservative baseline)
6. ⬜ Pre-computed score export working
7. ⬜ Extension shell: badges rendering on Amazon search results from cached scores
8. ⬜ Graph trail API endpoint + visualization in extension detail view
9. ⬜ User preference configuration (issues, scorecards, weighting)
