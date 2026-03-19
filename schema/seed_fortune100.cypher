// ============================================================
// Seed Data: Fortune 100 corporations (2024 list, FY2023 revenues)
// ============================================================
// Idempotent: MERGE on name + SET properties (last-write-wins).
// Private companies have ticker = null.
// These nodes are created before brand resolution runs so PAC
// linkage immediately benefits from a richer Corporation base.
// ============================================================

MERGE (c:Corporation {name: "Walmart"})
SET c.ticker = "WMT", c.fortune100_rank = 1, c.sector = "Retail";

MERGE (c:Corporation {name: "Amazon"})
SET c.ticker = "AMZN", c.fortune100_rank = 2, c.sector = "Retail";

MERGE (c:Corporation {name: "Apple"})
SET c.ticker = "AAPL", c.fortune100_rank = 3, c.sector = "Technology";

MERGE (c:Corporation {name: "UnitedHealth Group"})
SET c.ticker = "UNH", c.fortune100_rank = 4, c.sector = "Healthcare";

MERGE (c:Corporation {name: "Berkshire Hathaway"})
SET c.ticker = "BRK.A", c.fortune100_rank = 5, c.sector = "Financial Services";

MERGE (c:Corporation {name: "CVS Health"})
SET c.ticker = "CVS", c.fortune100_rank = 6, c.sector = "Healthcare";

MERGE (c:Corporation {name: "Exxon Mobil"})
SET c.ticker = "XOM", c.fortune100_rank = 7, c.sector = "Energy";

MERGE (c:Corporation {name: "Alphabet"})
SET c.ticker = "GOOGL", c.fortune100_rank = 8, c.sector = "Technology";

MERGE (c:Corporation {name: "McKesson"})
SET c.ticker = "MCK", c.fortune100_rank = 9, c.sector = "Healthcare";

MERGE (c:Corporation {name: "Cencora"})
SET c.ticker = "COR", c.fortune100_rank = 10, c.sector = "Healthcare";

MERGE (c:Corporation {name: "Costco Wholesale"})
SET c.ticker = "COST", c.fortune100_rank = 11, c.sector = "Retail";

MERGE (c:Corporation {name: "JPMorgan Chase"})
SET c.ticker = "JPM", c.fortune100_rank = 12, c.sector = "Financial Services";

MERGE (c:Corporation {name: "Microsoft"})
SET c.ticker = "MSFT", c.fortune100_rank = 13, c.sector = "Technology";

MERGE (c:Corporation {name: "Cardinal Health"})
SET c.ticker = "CAH", c.fortune100_rank = 14, c.sector = "Healthcare";

MERGE (c:Corporation {name: "Chevron"})
SET c.ticker = "CVX", c.fortune100_rank = 15, c.sector = "Energy";

MERGE (c:Corporation {name: "Cigna Group"})
SET c.ticker = "CI", c.fortune100_rank = 16, c.sector = "Insurance";

MERGE (c:Corporation {name: "Ford Motor"})
SET c.ticker = "F", c.fortune100_rank = 17, c.sector = "Automotive";

MERGE (c:Corporation {name: "Bank of America"})
SET c.ticker = "BAC", c.fortune100_rank = 18, c.sector = "Financial Services";

MERGE (c:Corporation {name: "General Motors"})
SET c.ticker = "GM", c.fortune100_rank = 19, c.sector = "Automotive";

MERGE (c:Corporation {name: "Elevance Health"})
SET c.ticker = "ELV", c.fortune100_rank = 20, c.sector = "Healthcare";

MERGE (c:Corporation {name: "Citigroup"})
SET c.ticker = "C", c.fortune100_rank = 21, c.sector = "Financial Services";

MERGE (c:Corporation {name: "Centene"})
SET c.ticker = "CNC", c.fortune100_rank = 22, c.sector = "Healthcare";

MERGE (c:Corporation {name: "Home Depot"})
SET c.ticker = "HD", c.fortune100_rank = 23, c.sector = "Retail";

MERGE (c:Corporation {name: "Marathon Petroleum"})
SET c.ticker = "MPC", c.fortune100_rank = 24, c.sector = "Energy";

MERGE (c:Corporation {name: "Kroger"})
SET c.ticker = "KR", c.fortune100_rank = 25, c.sector = "Retail";

MERGE (c:Corporation {name: "Phillips 66"})
SET c.ticker = "PSX", c.fortune100_rank = 26, c.sector = "Energy";

MERGE (c:Corporation {name: "Fannie Mae"})
SET c.ticker = "FNMA", c.fortune100_rank = 27, c.sector = "Financial Services";

MERGE (c:Corporation {name: "Walgreens Boots Alliance"})
SET c.ticker = "WBA", c.fortune100_rank = 28, c.sector = "Healthcare";

MERGE (c:Corporation {name: "Valero Energy"})
SET c.ticker = "VLO", c.fortune100_rank = 29, c.sector = "Energy";

MERGE (c:Corporation {name: "Meta Platforms"})
SET c.ticker = "META", c.fortune100_rank = 30, c.sector = "Technology";

MERGE (c:Corporation {name: "Verizon Communications"})
SET c.ticker = "VZ", c.fortune100_rank = 31, c.sector = "Telecommunications";

MERGE (c:Corporation {name: "AT&T"})
SET c.ticker = "T", c.fortune100_rank = 32, c.sector = "Telecommunications";

MERGE (c:Corporation {name: "Comcast"})
SET c.ticker = "CMCSA", c.fortune100_rank = 33, c.sector = "Telecommunications";

MERGE (c:Corporation {name: "Wells Fargo"})
SET c.ticker = "WFC", c.fortune100_rank = 34, c.sector = "Financial Services";

MERGE (c:Corporation {name: "Goldman Sachs Group"})
SET c.ticker = "GS", c.fortune100_rank = 35, c.sector = "Financial Services";

MERGE (c:Corporation {name: "Freddie Mac"})
SET c.ticker = "FMCC", c.fortune100_rank = 36, c.sector = "Financial Services";

MERGE (c:Corporation {name: "Target"})
SET c.ticker = "TGT", c.fortune100_rank = 37, c.sector = "Retail";

MERGE (c:Corporation {name: "Humana"})
SET c.ticker = "HUM", c.fortune100_rank = 38, c.sector = "Healthcare";

MERGE (c:Corporation {name: "State Farm Insurance"})
SET c.ticker = null, c.fortune100_rank = 39, c.sector = "Insurance";

MERGE (c:Corporation {name: "Tesla"})
SET c.ticker = "TSLA", c.fortune100_rank = 40, c.sector = "Automotive";

MERGE (c:Corporation {name: "Morgan Stanley"})
SET c.ticker = "MS", c.fortune100_rank = 41, c.sector = "Financial Services";

MERGE (c:Corporation {name: "Johnson & Johnson"})
SET c.ticker = "JNJ", c.fortune100_rank = 42, c.sector = "Pharmaceuticals";

MERGE (c:Corporation {name: "Archer Daniels Midland"})
SET c.ticker = "ADM", c.fortune100_rank = 43, c.sector = "Food & Agriculture";

MERGE (c:Corporation {name: "PepsiCo"})
SET c.ticker = "PEP", c.fortune100_rank = 44, c.sector = "Consumer Goods";

MERGE (c:Corporation {name: "UPS"})
SET c.ticker = "UPS", c.fortune100_rank = 45, c.sector = "Transportation";

MERGE (c:Corporation {name: "FedEx"})
SET c.ticker = "FDX", c.fortune100_rank = 46, c.sector = "Transportation";

MERGE (c:Corporation {name: "Walt Disney"})
SET c.ticker = "DIS", c.fortune100_rank = 47, c.sector = "Media & Entertainment";

MERGE (c:Corporation {name: "Dell Technologies"})
SET c.ticker = "DELL", c.fortune100_rank = 48, c.sector = "Technology";

MERGE (c:Corporation {name: "Lowe's"})
SET c.ticker = "LOW", c.fortune100_rank = 49, c.sector = "Retail";

MERGE (c:Corporation {name: "Procter & Gamble"})
SET c.ticker = "PG", c.fortune100_rank = 50, c.sector = "Consumer Goods";

MERGE (c:Corporation {name: "Energy Transfer"})
SET c.ticker = "ET", c.fortune100_rank = 51, c.sector = "Energy";

MERGE (c:Corporation {name: "Boeing"})
SET c.ticker = "BA", c.fortune100_rank = 52, c.sector = "Aerospace & Defense";

MERGE (c:Corporation {name: "Albertsons"})
SET c.ticker = "ACI", c.fortune100_rank = 53, c.sector = "Retail";

MERGE (c:Corporation {name: "Sysco"})
SET c.ticker = "SYY", c.fortune100_rank = 54, c.sector = "Food & Agriculture";

MERGE (c:Corporation {name: "RTX"})
SET c.ticker = "RTX", c.fortune100_rank = 55, c.sector = "Aerospace & Defense";

MERGE (c:Corporation {name: "General Electric"})
SET c.ticker = "GE", c.fortune100_rank = 56, c.sector = "Industrial";

MERGE (c:Corporation {name: "Lockheed Martin"})
SET c.ticker = "LMT", c.fortune100_rank = 57, c.sector = "Aerospace & Defense";

MERGE (c:Corporation {name: "American Express"})
SET c.ticker = "AXP", c.fortune100_rank = 58, c.sector = "Financial Services";

MERGE (c:Corporation {name: "Caterpillar"})
SET c.ticker = "CAT", c.fortune100_rank = 59, c.sector = "Industrial";

MERGE (c:Corporation {name: "MetLife"})
SET c.ticker = "MET", c.fortune100_rank = 60, c.sector = "Insurance";

MERGE (c:Corporation {name: "HCA Healthcare"})
SET c.ticker = "HCA", c.fortune100_rank = 61, c.sector = "Healthcare";

MERGE (c:Corporation {name: "Progressive"})
SET c.ticker = "PGR", c.fortune100_rank = 62, c.sector = "Insurance";

MERGE (c:Corporation {name: "IBM"})
SET c.ticker = "IBM", c.fortune100_rank = 63, c.sector = "Technology";

MERGE (c:Corporation {name: "Deere"})
SET c.ticker = "DE", c.fortune100_rank = 64, c.sector = "Industrial";

MERGE (c:Corporation {name: "Nvidia"})
SET c.ticker = "NVDA", c.fortune100_rank = 65, c.sector = "Technology";

MERGE (c:Corporation {name: "StoneX Group"})
SET c.ticker = "SNEX", c.fortune100_rank = 66, c.sector = "Financial Services";

MERGE (c:Corporation {name: "Merck"})
SET c.ticker = "MRK", c.fortune100_rank = 67, c.sector = "Pharmaceuticals";

MERGE (c:Corporation {name: "ConocoPhillips"})
SET c.ticker = "COP", c.fortune100_rank = 68, c.sector = "Energy";

MERGE (c:Corporation {name: "Pfizer"})
SET c.ticker = "PFE", c.fortune100_rank = 69, c.sector = "Pharmaceuticals";

MERGE (c:Corporation {name: "Delta Air Lines"})
SET c.ticker = "DAL", c.fortune100_rank = 70, c.sector = "Transportation";

MERGE (c:Corporation {name: "TD Synnex"})
SET c.ticker = "SNX", c.fortune100_rank = 71, c.sector = "Technology";

MERGE (c:Corporation {name: "Publix Super Markets"})
SET c.ticker = null, c.fortune100_rank = 72, c.sector = "Retail";

MERGE (c:Corporation {name: "Allstate"})
SET c.ticker = "ALL", c.fortune100_rank = 73, c.sector = "Insurance";

MERGE (c:Corporation {name: "Cisco Systems"})
SET c.ticker = "CSCO", c.fortune100_rank = 74, c.sector = "Technology";

MERGE (c:Corporation {name: "Nationwide"})
SET c.ticker = null, c.fortune100_rank = 75, c.sector = "Insurance";

MERGE (c:Corporation {name: "Charter Communications"})
SET c.ticker = "CHTR", c.fortune100_rank = 76, c.sector = "Telecommunications";

MERGE (c:Corporation {name: "AbbVie"})
SET c.ticker = "ABBV", c.fortune100_rank = 77, c.sector = "Pharmaceuticals";

MERGE (c:Corporation {name: "New York Life Insurance"})
SET c.ticker = null, c.fortune100_rank = 78, c.sector = "Insurance";

MERGE (c:Corporation {name: "Intel"})
SET c.ticker = "INTC", c.fortune100_rank = 79, c.sector = "Technology";

MERGE (c:Corporation {name: "TJX"})
SET c.ticker = "TJX", c.fortune100_rank = 80, c.sector = "Retail";

MERGE (c:Corporation {name: "Prudential Financial"})
SET c.ticker = "PRU", c.fortune100_rank = 81, c.sector = "Financial Services";

MERGE (c:Corporation {name: "HP"})
SET c.ticker = "HPQ", c.fortune100_rank = 82, c.sector = "Technology";

MERGE (c:Corporation {name: "United Airlines Holdings"})
SET c.ticker = "UAL", c.fortune100_rank = 83, c.sector = "Transportation";

MERGE (c:Corporation {name: "Performance Food Group"})
SET c.ticker = "PFGC", c.fortune100_rank = 84, c.sector = "Food & Agriculture";

MERGE (c:Corporation {name: "Tyson Foods"})
SET c.ticker = "TSN", c.fortune100_rank = 85, c.sector = "Food & Agriculture";

MERGE (c:Corporation {name: "American Airlines Group"})
SET c.ticker = "AAL", c.fortune100_rank = 86, c.sector = "Transportation";

MERGE (c:Corporation {name: "Liberty Mutual Insurance Group"})
SET c.ticker = null, c.fortune100_rank = 87, c.sector = "Insurance";

MERGE (c:Corporation {name: "Nike"})
SET c.ticker = "NKE", c.fortune100_rank = 88, c.sector = "Consumer Goods";

MERGE (c:Corporation {name: "Oracle"})
SET c.ticker = "ORCL", c.fortune100_rank = 89, c.sector = "Technology";

MERGE (c:Corporation {name: "Enterprise Products Partners"})
SET c.ticker = "EPD", c.fortune100_rank = 90, c.sector = "Energy";

MERGE (c:Corporation {name: "Capital One Financial"})
SET c.ticker = "COF", c.fortune100_rank = 91, c.sector = "Financial Services";

MERGE (c:Corporation {name: "Plains GP Holdings"})
SET c.ticker = "PAGP", c.fortune100_rank = 92, c.sector = "Energy";

MERGE (c:Corporation {name: "World Kinect"})
SET c.ticker = "WKC", c.fortune100_rank = 93, c.sector = "Energy";

MERGE (c:Corporation {name: "AIG"})
SET c.ticker = "AIG", c.fortune100_rank = 94, c.sector = "Insurance";

MERGE (c:Corporation {name: "Coca-Cola"})
SET c.ticker = "KO", c.fortune100_rank = 95, c.sector = "Consumer Goods";

MERGE (c:Corporation {name: "TIAA"})
SET c.ticker = null, c.fortune100_rank = 96, c.sector = "Financial Services";

MERGE (c:Corporation {name: "CHS"})
SET c.ticker = null, c.fortune100_rank = 97, c.sector = "Food & Agriculture";

MERGE (c:Corporation {name: "Bristol-Myers Squibb"})
SET c.ticker = "BMY", c.fortune100_rank = 98, c.sector = "Pharmaceuticals";

MERGE (c:Corporation {name: "Dow"})
SET c.ticker = "DOW", c.fortune100_rank = 99, c.sector = "Chemicals";

MERGE (c:Corporation {name: "Best Buy"})
SET c.ticker = "BBY", c.fortune100_rank = 100, c.sector = "Retail";
