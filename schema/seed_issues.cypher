// ============================================================
// Seed Data: Issue nodes and initial Scorecard nodes
// ============================================================

// --- Issue Nodes ---

MERGE (i:Issue {name: "civil_liberties"})
SET i.description = "Civil liberties including free speech, due process, and privacy rights";

MERGE (i:Issue {name: "environment"})
SET i.description = "Environmental protection, climate change policy, and conservation";

MERGE (i:Issue {name: "digital_rights"})
SET i.description = "Digital privacy, surveillance, net neutrality, and online freedoms";

MERGE (i:Issue {name: "labor"})
SET i.description = "Workers rights, union protections, wages, and workplace safety";

MERGE (i:Issue {name: "lgbtq_rights"})
SET i.description = "LGBTQ+ equality, anti-discrimination protections, and marriage rights";

MERGE (i:Issue {name: "immigration"})
SET i.description = "Immigration policy, border security, and refugee/asylum policy";

MERGE (i:Issue {name: "gun_policy"})
SET i.description = "Firearm regulation, gun rights, and gun violence prevention";

MERGE (i:Issue {name: "healthcare"})
SET i.description = "Healthcare access, insurance policy, and public health";

MERGE (i:Issue {name: "education"})
SET i.description = "Education funding, school choice, and higher education policy";

MERGE (i:Issue {name: "fiscal_policy"})
SET i.description = "Taxation, government spending, and budget policy";

// --- Scorecard Nodes ---
// year lives on RATES edges, not on the Scorecard node itself.

MERGE (s:Scorecard {org_name: "ACLU"})
SET s.methodology_url = "https://www.aclu.org/legislative-scorecard";

MERGE (s:Scorecard {org_name: "League of Conservation Voters"})
SET s.methodology_url = "https://scorecard.lcv.org/";

MERGE (s:Scorecard {org_name: "Human Rights Campaign"})
SET s.methodology_url = "https://www.hrc.org/resources/congressional-scorecard";

MERGE (s:Scorecard {org_name: "AFL-CIO"})
SET s.methodology_url = "https://aflcio.org/scorecard";

MERGE (s:Scorecard {org_name: "EFF"})
SET s.methodology_url = "https://www.eff.org/issues";

// --- Link Scorecards to Issues ---

MATCH (s:Scorecard {org_name: "ACLU"})
MATCH (i:Issue {name: "civil_liberties"})
MERGE (s)-[:COVERS]->(i);

MATCH (s:Scorecard {org_name: "ACLU"})
MATCH (i:Issue {name: "digital_rights"})
MERGE (s)-[:COVERS]->(i);

MATCH (s:Scorecard {org_name: "League of Conservation Voters"})
MATCH (i:Issue {name: "environment"})
MERGE (s)-[:COVERS]->(i);

MATCH (s:Scorecard {org_name: "Human Rights Campaign"})
MATCH (i:Issue {name: "lgbtq_rights"})
MERGE (s)-[:COVERS]->(i);

MATCH (s:Scorecard {org_name: "AFL-CIO"})
MATCH (i:Issue {name: "labor"})
MERGE (s)-[:COVERS]->(i);

MATCH (s:Scorecard {org_name: "EFF"})
MATCH (i:Issue {name: "digital_rights"})
MERGE (s)-[:COVERS]->(i);
