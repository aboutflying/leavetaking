"""One-time script: build initial brand -> corporation mapping for top Amazon brands."""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

from pipeline.config import ensure_data_dirs, get_neo4j_driver, settings
from pipeline.fetchers.opencorporates import search_companies
from pipeline.fetchers.wikidata import batch_resolve_brands, get_ownership_chain
from pipeline.loaders.graph_loader import (
    load_brands,
    load_corporations,
    load_ownership_edges,
    load_subsidiary_edges,
)
from pipeline.processors.entity_resolution import match_brand_to_corporation

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Top Amazon brands for MVP. In production, these would be scraped/crawled.
TOP_BRANDS = [
    "Amazon Basics", "Apple", "Samsung", "Sony", "LG", "Bose",
    "Nike", "Adidas", "Under Armour", "Levi's", "Hasbro", "Mattel",
    "Procter & Gamble", "Unilever", "Johnson & Johnson", "Colgate-Palmolive",
    "Nestle", "PepsiCo", "Coca-Cola", "General Mills", "Kellogg's",
    "Kraft Heinz", "Mars", "Mondelez", "Hershey", "Tyson Foods",
    "3M", "Honeywell", "General Electric", "Whirlpool", "Black & Decker",
    "Duracell", "Energizer", "Clorox", "SC Johnson", "Church & Dwight",
    "Estee Lauder", "L'Oreal", "Revlon", "Maybelline",
    "Microsoft", "Google", "Intel", "AMD", "NVIDIA",
    "HP", "Dell", "Lenovo", "ASUS", "Acer",
]


def main():
    ensure_data_dirs()
    cache_path = settings.data_dir / "brand_resolutions.json"

    # Step 1: Resolve brands via Wikidata
    logger.info("Resolving %d brands via Wikidata...", len(TOP_BRANDS))
    wd_results = batch_resolve_brands(TOP_BRANDS, delay=1.5)

    # Step 2: Cross-reference with OpenCorporates
    logger.info("Cross-referencing with OpenCorporates...")
    resolutions = {}
    for brand_name in TOP_BRANDS:
        wd_matches = wd_results.get(brand_name, [])
        try:
            oc_matches = search_companies(brand_name)
        except Exception:
            logger.exception("OpenCorporates lookup failed for %s", brand_name)
            oc_matches = []

        match = match_brand_to_corporation(brand_name, wd_matches, oc_matches)
        if match:
            resolutions[brand_name] = match

    # Cache results
    cache_path.write_text(json.dumps(resolutions, indent=2))
    logger.info("Cached %d resolutions to %s", len(resolutions), cache_path)

    # Step 3: Load into Neo4j
    driver = get_neo4j_driver()
    try:
        with driver.session() as session:
            # Load brand nodes
            brands = [
                {"name": name, "amazon_slug": name.lower().replace(" ", "-"), "aliases": []}
                for name in TOP_BRANDS
            ]
            load_brands(session, brands)

            # Load corporation nodes and ownership edges
            corporations = []
            ownership_edges = []
            subsidiary_edges = []

            for brand_name, match in resolutions.items():
                corp_name = match["name"]
                corporations.append({
                    "name": corp_name,
                    "ticker": match.get("ticker"),
                    "cik": None,
                    "jurisdiction": match.get("jurisdiction"),
                    "oc_id": str(match.get("oc_id")) if match.get("oc_id") else None,
                })
                ownership_edges.append({
                    "brand_name": brand_name,
                    "corporation_name": corp_name,
                })

                # Get ownership chain for parent companies
                if match.get("qid"):
                    try:
                        chain = get_ownership_chain(match["qid"])
                        for link in chain:
                            corporations.append({
                                "name": link["parent_name"],
                                "ticker": None,
                                "cik": None,
                                "jurisdiction": None,
                                "oc_id": None,
                            })
                            subsidiary_edges.append({
                                "child_name": link["child_name"],
                                "parent_name": link["parent_name"],
                            })
                    except Exception:
                        logger.exception("Ownership chain failed for %s", corp_name)

            # Deduplicate corporations by name
            seen = set()
            unique_corps = []
            for c in corporations:
                if c["name"] not in seen:
                    unique_corps.append(c)
                    seen.add(c["name"])

            load_corporations(session, unique_corps)
            load_ownership_edges(session, ownership_edges)
            load_subsidiary_edges(session, subsidiary_edges)

            logger.info(
                "Loaded %d brands, %d corporations, %d ownership edges, %d subsidiary edges",
                len(brands), len(unique_corps), len(ownership_edges), len(subsidiary_edges),
            )
    finally:
        driver.close()

    logger.info("Brand bootstrap complete.")


if __name__ == "__main__":
    main()
