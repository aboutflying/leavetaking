// ============================================================
// Neo4j Schema Constraints and Indexes for Political Purchaser
// ============================================================

// --- Uniqueness Constraints ---

CREATE CONSTRAINT brand_name IF NOT EXISTS
FOR (b:Brand) REQUIRE b.name IS UNIQUE;

CREATE INDEX corporation_name IF NOT EXISTS
FOR (c:Corporation) ON (c.name);

CREATE CONSTRAINT person_fec_id IF NOT EXISTS
FOR (p:Person) REQUIRE p.fec_contributor_id IS UNIQUE;

CREATE CONSTRAINT candidate_fec_id IF NOT EXISTS
FOR (c:Candidate) REQUIRE c.fec_candidate_id IS UNIQUE;

CREATE CONSTRAINT committee_fec_id IF NOT EXISTS
FOR (c:Committee) REQUIRE c.fec_committee_id IS UNIQUE;

CREATE CONSTRAINT issue_name IF NOT EXISTS
FOR (i:Issue) REQUIRE i.name IS UNIQUE;

CREATE CONSTRAINT scorecard_org_name IF NOT EXISTS
FOR (s:Scorecard) REQUIRE s.org_name IS UNIQUE;

// --- Indexes for common lookups ---

CREATE INDEX brand_amazon_slug IF NOT EXISTS
FOR (b:Brand) ON (b.amazon_slug);

CREATE INDEX corporation_ticker IF NOT EXISTS
FOR (c:Corporation) ON (c.ticker);

CREATE INDEX corporation_oc_id IF NOT EXISTS
FOR (c:Corporation) ON (c.oc_id);

CREATE INDEX corporation_cik IF NOT EXISTS
FOR (c:Corporation) ON (c.cik);

CREATE INDEX corporation_fortune100_rank IF NOT EXISTS
FOR (c:Corporation) ON (c.fortune100_rank);

CREATE INDEX candidate_name IF NOT EXISTS
FOR (c:Candidate) ON (c.name);

CREATE INDEX committee_name IF NOT EXISTS
FOR (c:Committee) ON (c.name);

CREATE INDEX committee_type IF NOT EXISTS
FOR (c:Committee) ON (c.type);

CREATE INDEX committee_org_type IF NOT EXISTS
FOR (c:Committee) ON (c.org_type);

CREATE INDEX person_name IF NOT EXISTS
FOR (p:Person) ON (p.name);
