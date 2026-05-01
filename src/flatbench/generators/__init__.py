"""Data generators for benchmark testing."""

import csv
import json
import random
import string
import hashlib
import uuid
import argparse
from datetime import datetime, timedelta
from typing import Any


# ─── Controlled anchor data for result verification ────────────────────────────
# These are deterministic "spike" rows planted at known intervals so that range queries
# and aggregations produce known, verifiable hit counts.

CONTROL_ANCHORS = {
    "article": {
        # tags spikes: (tag, interval, count) → every `interval` rows, insert that tag
        # e.g. "machine-learning" every 1000 rows → exactly rows/1000 rows have this tag alone
        "tags_spikes": [
            ("machine-learning", 1000),  # 100 rows in 100k
            ("api", 500),                # 200 rows
            ("devops", 2000),            # 50 rows
            ("security", 2500),          # 40 rows
        ],
        # views spikes: (exact_views_value, interval) → rows with views==value at those intervals
        "views_spikes": [
            (0, 500),      # 200 rows with views=0
            (42, 1000),   # 100 rows with views=42
            (100, 2500),  # 40 rows with views=100
        ],
        # author spikes: (author_prefix, interval) → rows with author=prefix at those intervals
        "author_spikes": [
            ("author_alpha", 1000),   # 100 rows with author starting with "author_alpha"
            ("author_beta", 2000),    # 50 rows with author starting with "author_beta"
        ],
        # content spikes: (keyword, interval) → rows containing keyword in content
        "content_spikes": [
            ("BLOCKCHAIN_UNIQUE_KEYWORD_XYZ", 1000),  # 100 rows
            ("PERFMONITORING123_SECRET", 500),          # 200 rows
        ],
        # published_at spikes: (year, interval) → rows with that exact year
        "year_spikes": [
            (2025, 500),   # 200 rows published in 2025
            (2024, 1000),  # 100 rows published in 2024
        ],
    },
    "sosmed": {
        "platform_spikes": [
            ("instagram", 1000),   # 100 rows
            ("facebook", 2000),     # 50 rows
        ],
        "likes_spikes": [
            (0, 500),    # 200 rows with likes=0
            (1, 1000),   # 100 rows with likes=1
        ],
        "content_spikes": [
            ("SOSMED_UNIQUE_CONTENT_MARKER_ABC", 1000),  # 100 rows
        ],
    },
    "devops": {
        "level_spikes": [
            ("ERROR", 1000),   # 100 rows
            ("WARN", 500),     # 200 rows
        ],
        "service_spikes": [
            ("api-gateway", 1000),   # 100 rows
            ("auth-service", 2000),   # 50 rows
        ],
        "duration_spikes": [
            (0, 500),       # 200 rows with duration_ms=0
            (9999, 1000),  # 100 rows with duration_ms=9999
        ],
    },
}


def _is_anchor_row(row_idx: int, interval: int) -> bool:
    """Returns True if this row index should be an anchor (spike) row."""
    return row_idx > 0 and row_idx % interval == 0


def _generate_article_anchor(row_idx: int, total_rows: int):
    """Plant a controlled anchor row for article schema. Returns row or None."""
    anchors = CONTROL_ANCHORS.get("article", {})
    tags_spikes = anchors.get("tags_spikes", [])
    views_spikes = anchors.get("views_spikes", [])
    author_spikes = anchors.get("author_spikes", [])
    content_spikes = anchors.get("content_spikes", [])
    year_spikes = anchors.get("year_spikes", [])

    # Priority: only one anchor per row — check each in order
    for tag, interval in tags_spikes:
        if _is_anchor_row(row_idx, interval):
            return {
                "id": row_idx,
                "title": f"Anchor Article for tag:{tag}",
                "content": "This is an anchor row for verification purposes.",
                "tags": json.dumps([tag]),
                "views": row_idx % 100,  # arbitrary but consistent
                "published_at": f"{random.choice([2020,2021,2022,2023,2024,2025])}-01-01",
                "author": f"author_regular_{row_idx}",
            }

    for views_val, interval in views_spikes:
        if _is_anchor_row(row_idx, interval):
            return {
                "id": row_idx,
                "title": f"Anchor Article for views:{views_val}",
                "content": "This is an anchor row for verification purposes.",
                "tags": json.dumps(["general", "anchor"]),
                "views": views_val,
                "published_at": "2024-01-01",
                "author": f"author_regular_{row_idx}",
            }

    for author_prefix, interval in author_spikes:
        if _is_anchor_row(row_idx, interval):
            return {
                "id": row_idx,
                "title": "Anchor Article for author spike",
                "content": "This is an anchor row for verification purposes.",
                "tags": json.dumps(["general"]),
                "views": row_idx % 1000,
                "published_at": "2024-01-01",
                "author": f"{author_prefix}_{row_idx}",
            }

    for keyword, interval in content_spikes:
        if _is_anchor_row(row_idx, interval):
            return {
                "id": row_idx,
                "title": "Anchor Article for content spike",
                "content": f"This article contains the unique marker {keyword} for verification.",
                "tags": json.dumps(["general"]),
                "views": row_idx % 1000,
                "published_at": "2024-01-01",
                "author": f"author_regular_{row_idx}",
            }

    for year, interval in year_spikes:
        if _is_anchor_row(row_idx, interval):
            return {
                "id": row_idx,
                "title": f"Anchor Article for year:{year}",
                "content": "This is an anchor row for verification purposes.",
                "tags": json.dumps(["general"]),
                "views": row_idx % 10000,
                "published_at": f"{year}-06-15",
                "author": f"author_regular_{row_idx}",
            }

    return None


# ─── Field generators ────────────────────────────────────────────────────────

def weighted_choice(options: list, weights: list) -> str:
    return random.choices(options, weights=weights, k=1)[0]

def rand_str(length: int = 10) -> str:
    return ''.join(random.choices(string.ascii_letters, k=length))

def rand_int(min_val: int = 0, max_val: int = 1000000) -> int:
    return random.randint(min_val, max_val)

def rand_float(min_val: float = 0.0, max_val: float = 1000000.0, decimals: int = 2) -> float:
    val = random.uniform(min_val, max_val)
    return round(val, decimals)

def rand_bool() -> bool:
    return random.random() < 0.5

def rand_date(start_year: int = 2020, end_year: int = 2026) -> str:
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).strftime("%Y-%m-%d")

def rand_datetime(start_year: int = 2020, end_year: int = 2026) -> str:
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_seconds = random.randint(0, delta.days * 86400)
    return (start + timedelta(seconds=random_seconds)).strftime("%Y-%m-%dT%H:%M:%S")

def rand_email() -> str:
    domains = ["gmail.com", "yahoo.com", "flatseek.io", "hotmail.com", "outlook.com", "dev.io", "test.co"]
    return f"{rand_str(8).lower()}@{random.choice(domains)}"

def rand_phone() -> str:
    return f"+62{rand_int(800000000, 899999999)}"

def rand_country() -> str:
    countries = [
        "Indonesia", "Malaysia", "Singapore", "Thailand", "Vietnam",
        "Philippines", "Myanmar", "Cambodia", "Laos", "Brunei",
        "USA", "UK", "Germany", "France", "Japan", "Korea", "China", "India"
    ]
    return random.choice(countries)

def rand_city(country: str = None) -> str:
    cities = {
        "Indonesia": ["Jakarta", "Surabaya", "Bandung", "Bali", "Medan", "Makassar", "Palembang"],
        "Malaysia": ["Kuala Lumpur", "Penang", "Johor", "Malacca"],
        "Singapore": ["Singapore"],
        "Thailand": ["Bangkok", "Phuket", "Chiang Mai"],
        "USA": ["New York", "Los Angeles", "San Francisco", "Seattle", "Austin"],
        "Japan": ["Tokyo", "Osaka", "Kyoto"],
    }
    if country and country in cities:
        return random.choice(cities[country])
    all_cities = [c for cities in cities.values() for c in cities]
    return random.choice(all_cities)

def rand_tags(min_tags: int = 1, max_tags: int = 5) -> list:
    all_tags = [
        "python", "golang", "javascript", "typescript", "java", "rust",
        "kubernetes", "docker", "judotens", "aws", "gcp", "azure", "terraform",
        "graphql", "rest", "grpc", "redis", "postgres", "mysql", "mongodb",
        "linux", "windows", "macos", "android", "ios", "react", "vue", "angular",
        "api", "backend", "frontend", "devops", "security", "database",
        "machine-learning", "ai", "data-engineering", "analytics"
    ]
    n = random.randint(min_tags, max_tags)
    return random.sample(all_tags, n)

def rand_array_of_strings(min_items: int = 1, max_items: int = 5) -> list:
    words = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
             "iota", "kappa", "lambda", "mu", "nu", "xi", "omicron", "pi"]
    n = random.randint(min_items, max_items)
    return random.sample(words, n)

def rand_nested_object() -> dict:
    return {
        "id": rand_int(1, 1000),
        "name": rand_str(20),
        "value": rand_float(0, 1000),
        "active": rand_bool(),
        "metadata": {
            "created": rand_date(),
            "version": f"{rand_int(1,10)}.{rand_int(0,20)}.{rand_int(0,100)}",
            "tags": rand_tags(1, 3)
        }
    }

def rand_ip() -> str:
    return f"{rand_int(1,255)}.{rand_int(0,255)}.{rand_int(0,255)}.{rand_int(0,255)}"

def rand_article_title() -> str:
    templates = [
        "The Complete Guide to {}",
        "Understanding {}: A Practical Approach",
        "Modern {} for Real-World Applications",
        "How to Master {} in 2026",
        "{}: Best Practices and Common Pitfalls",
        "Advanced {}: Tips and Techniques",
        "A Deep Dive into {}",
        "{}: From Basics to Advanced",
        "The Ultimate {} Handbook",
        "Breaking Down {}: What You Need to Know",
        "Implementing {} Effectively",
        "{} vs Traditional Approaches",
    ]
    topics = [
        "microservices", "kubernetes", "docker", "react", "typescript",
        "machine learning", "data pipelines", "api design", "devops",
        "cloud architecture", "security", "performance optimization",
        "distributed systems", "ci-cd", "terraform", "graphql", "rest",
        "websocket", "redis", "postgresql", "mongodb", "aws", "gcp",
        "serverless", "lambda", "containerization", "observability",
    ]
    return random.choice(templates).format(random.choice(topics))


ARTICLE_CONTENT_POOL = [
    "Performance optimization is a critical consideration in any production environment. Studies have shown that even small improvements in response times can lead to significant increases in user satisfaction and conversion rates.",
    "Testing is an often overlooked but essential part of the development cycle. Unit tests verify individual components in isolation. Integration tests ensure components work together. End-to-end tests validate the entire system.",
    "In practice, there are several approaches one can take when implementing this solution. The first approach involves a direct method that is straightforward but may not scale well under heavy load. The second uses a more sophisticated technique that requires additional infrastructure but provides better performance guarantees.",
    "This article provides a comprehensive overview of the topic at hand. We will explore the key concepts, practical applications, and common pitfalls that developers and practitioners encounter when working with this technology.",
    "Scalability is not just about handling more traffic — it's about maintaining consistent performance as complexity grows. Horizontal scaling introduces challenges around data consistency and coordination between nodes.",
    "Security should be considered from the beginning, not added as an afterthought. Authentication, authorization, input validation, and encryption are foundational elements that every system needs.",
    "Monitoring and observability go hand in hand. You cannot improve what you cannot measure. Key metrics include latency, error rates, throughput, and resource utilization.",
    "The design patterns we choose today become the constraints we live with tomorrow. Choosing the right abstraction layer early saves tremendous pain later.",
    "Asynchronous processing enables systems to remain responsive under heavy load. Message queues, event-driven architectures, and background workers are common patterns for achieving this.",
    "Data consistency in distributed systems is a hard problem. CAP theorem reminds us that we cannot have simultaneously perfect consistency and availability in the presence of network partitions.",
]


def rand_article_content() -> str:
    paragraphs = random.sample(ARTICLE_CONTENT_POOL, random.randint(3, 6))
    return "\n\n".join(paragraphs)


def rand_url() -> str:
    paths = ["api", "v1", "v2", "users", "products", "orders", "search", "analytics"]
    return f"https://{rand_str(10).lower()}.com/{random.choice(paths)}/{rand_int(1,1000)}"

def rand_user_agent() -> str:
    browsers = ["Chrome", "Firefox", "Safari", "Edge"]
    os_list = ["Windows NT 10.0", "Macintosh", "X11; Linux x86_64"]
    return f"Mozilla/5.0 ({random.choice(os_list)}) AppleWebKit/537.36 (KHTML, like Gecko) {random.choice(browsers)}/120.0.0.0"

# ─── Schema definitions ───────────────────────────────────────────────────────

SCHEMAS = {
    "standard": [
        ("id", "int", lambda: rand_int(1, 1000000)),
        ("name", "str", lambda: rand_str(30)),
        ("email", "str", lambda: rand_email()),
        ("phone", "str", lambda: rand_phone()),
        ("city", "str", lambda: rand_city()),
        ("country", "str", lambda: rand_country()),
        ("status", "str", lambda: random.choice(["active", "inactive", "pending", "suspended"])),
        ("balance", "float", lambda: rand_float(0, 1000000)),
        ("created_at", "date", lambda: rand_date()),
        ("updated_at", "datetime", lambda: rand_datetime()),
        ("is_verified", "bool", lambda: rand_bool()),
        ("tags", "tags", lambda: rand_tags(1, 5)),
    ],

    "ecommerce": [
        ("order_id", "int", lambda: rand_int(1000000, 9999999)),
        ("customer_id", "int", lambda: rand_int(1, 100000)),
        ("product_name", "str", lambda: rand_str(50)),
        ("category", "str", lambda: random.choice(["electronics", "clothing", "food", "books", "home"])),
        ("price", "float", lambda: rand_float(0.01, 10000)),
        ("quantity", "int", lambda: rand_int(1, 100)),
        ("city", "str", lambda: rand_city()),
        ("country", "str", lambda: rand_country()),
        ("order_date", "date", lambda: rand_date()),
        ("status", "str", lambda: random.choice(["pending", "shipped", "delivered", "cancelled", "returned"])),
        ("payment_method", "str", lambda: random.choice(["credit_card", "debit_card", "ewallet", "bank_transfer"])),
        ("is_express", "bool", lambda: rand_bool()),
    ],

    "logs": [
        ("timestamp", "datetime", lambda: rand_datetime()),
        ("level", "str", lambda: random.choice(["DEBUG", "INFO", "WARN", "ERROR", "FATAL"])),
        ("service", "str", lambda: random.choice(["api-gateway", "auth-service", "user-service", "payment-service", "notification-service"])),
        ("message", "str", lambda: rand_str(100)),
        ("ip_address", "str", lambda: rand_ip()),
        ("user_id", "int", lambda: rand_int(1, 100000)),
        ("request_id", "str", lambda: rand_str(32)),
        ("duration_ms", "int", lambda: rand_int(1, 5000)),
        ("status_code", "int", lambda: random.choice([200, 201, 400, 401, 403, 404, 500, 502, 503])),
        ("endpoint", "str", lambda: rand_url()),
        ("user_agent", "str", lambda: rand_user_agent()),
    ],

    "nested": [
        ("id", "int", lambda: rand_int(1, 100000)),
        ("name", "str", lambda: rand_str(30)),
        ("profile", "nested", lambda: {
            "bio": rand_str(100),
            "age": rand_int(18, 80),
            "location": {"city": rand_city(), "country": rand_country()},
            "social": {"twitter": f"@{rand_str(10)}", "github": rand_str(15)}
        }),
        ("metadata", "nested", lambda: rand_nested_object()),
        ("preferences", "nested", lambda: {
            "theme": random.choice(["dark", "light", "auto"]),
            "notifications": rand_bool(),
            "tags": rand_tags(2, 6),
            "filters": rand_array_of_strings(2, 4)
        }),
        ("created_at", "date", lambda: rand_date()),
    ],

    "sparse": [
        ("id", "int", lambda: rand_int(1, 1000000)),
        ("name", "str", lambda: rand_str(30)),
        ("field_a", "str", lambda: rand_str(20) if random.random() > 0.3 else ""),
        ("field_b", "int", lambda: rand_int(1, 1000) if random.random() > 0.4 else 0),
        ("field_c", "float", lambda: rand_float(0, 1000) if random.random() > 0.5 else 0.0),
        ("field_d", "tags", lambda: rand_tags(1, 3) if random.random() > 0.6 else []),
        ("field_e", "str", lambda: rand_str(15) if random.random() > 0.7 else ""),
        ("field_f", "date", lambda: rand_date() if random.random() > 0.5 else ""),
        ("field_g", "bool", lambda: rand_bool() if random.random() > 0.8 else False),
        ("field_h", "str", lambda: rand_str(25) if random.random() > 0.3 else ""),
        ("field_i", "int", lambda: rand_int(1, 500) if random.random() > 0.6 else 0),
        ("field_j", "float", lambda: rand_float(0, 500) if random.random() > 0.4 else 0.0),
    ],

    "article": [
        ("id", "int", lambda: rand_int(1, 1000000)),
        ("title", "str", lambda: rand_article_title()),
        ("content", "str", lambda: rand_article_content()),
        ("tags", "tags", lambda: rand_tags(1, 8)),
        ("views", "int", lambda: rand_int(0, 100000)),
        ("published_at", "date", lambda: rand_date(2020, 2026)),
        ("author", "str", lambda: rand_str(20)),
    ],

    # Flatdata datasets (mirrors flatdata/generate.py)
    "adsb": [
        ("icao_address", "str", lambda: hashlib.md5(str(rand_int()).encode()).hexdigest()[:6].upper()),
        ("aircraft_type", "str", lambda: random.choice(["A20N", "A321", "A320", "B738", "B739", "E195", "AT76", "B38T"])),
        ("callsign", "str", lambda: f"{rand_str(5).upper()}{rand_int(100,999)}"),
        ("flight", "str", lambda: f"{rand_str(3).upper()}{rand_int(100,9999)}"),
        ("origin", "str", lambda: random.choice(["WIII", "WSSS", "WAAA", "WMKK", "VTBS", "RKSI", "RJTT", "ZBAA", "OMDB", "LFPG", "EGLL", "KJFK"])),
        ("origin_name", "str", lambda: rand_str(15)),
        ("destination", "str", lambda: random.choice(["WIII", "WSSS", "WAAA", "WMKK", "VTBS", "RKSI", "RJTT", "ZBAA", "OMDB", "LFPG", "EGLL", "KJFK"])),
        ("destination_name", "str", lambda: rand_str(15)),
        ("altitude", "int", lambda: rand_int(28000, 41000)),
        ("speed", "int", lambda: rand_int(380, 540)),
        ("heading", "int", lambda: rand_int(0, 359)),
        ("lat", "float", lambda: round(rand_float(-90, 90), 4)),
        ("lon", "float", lambda: round(rand_float(-180, 180), 4)),
        ("timestamp", "datetime", lambda: rand_datetime()),
        ("country", "str", lambda: random.choice(["ID", "SG", "MY", "TH", "JP", "KR", "CN", "AE", "FR", "GB", "US"])),
        ("status", "str", lambda: random.choice(["active", "active", "active", "landed", "on ground"])),
    ],

    "campaign": [
        ("campaign_id", "str", lambda: f"cmp_{rand_int():08d}"),
        ("advertiser", "str", lambda: random.choice(["ShopTokoID", "TravelEase", "GadgetProID", "KreditCepat", "CryptoVest", "AsuransiKita", "InvestaMandiri"])),
        ("campaign", "str", lambda: random.choice(["summer_sale", "ramadan_promo", "black_friday", "end_year_bash", "flash_deal_48h", "new_user_bonus"])),
        ("platform", "str", lambda: random.choice(["facebook", "instagram", "google", "tiktok", "twitter", "linkedin"])),
        ("country", "str", lambda: random.choice(["ID", "ID", "ID", "SG", "MY", "TH", "PH", "VN", "US", "IN"])),
        ("status", "str", lambda: weighted_choice(["active", "paused", "completed", "pending", "rejected"], [50, 15, 20, 10, 5])),
        ("bid", "float", lambda: round(rand_float(0.05, 15.0), 2)),
        ("impressions", "int", lambda: rand_int(0, 2000000)),
        ("clicks", "int", lambda: rand_int(0, 100000)),
        ("conversions", "int", lambda: rand_int(0, 50000)),
        ("spend_usd", "float", lambda: round(rand_float(1, 5000), 2)),
        ("ctr", "float", lambda: round(rand_float(0.1, 8.0), 3)),
        ("cpc", "float", lambda: round(rand_float(0.01, 5.0), 4)),
        ("roas", "float", lambda: round(rand_float(0, 10.0), 2)),
        ("budget_daily", "float", lambda: round(rand_float(50, 5000), 2)),
        ("timestamp", "datetime", lambda: rand_datetime()),
        ("frequency", "float", lambda: round(rand_float(1.0, 5.0), 2)),
    ],

    "devops": [
        ("timestamp", "datetime", lambda: rand_datetime()),
        ("level", "str", lambda: weighted_choice(["INFO", "WARN", "ERROR", "DEBUG"], [55, 25, 12, 8])),
        ("service", "str", lambda: random.choice(["api-gateway", "auth-service", "user-service", "payment-service", "notification-service", "order-service", "inventory-service", "search-service"])),
        ("region", "str", lambda: random.choice(["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1", "ap-northeast-1"])),
        ("message", "str", lambda: rand_str(100)),
        ("trace_id", "str", lambda: rand_str(32)),
        ("duration_ms", "int", lambda: rand_int(1, 5000)),
        ("status_code", "int", lambda: random.choice([200, 200, 200, 201, 204, 400, 401, 403, 404, 500, 502, 503])),
        ("host", "str", lambda: f"host-{rand_int(1,50):03d}.internal"),
        ("request_id", "str", lambda: rand_str(32)),
    ],

    "sosmed": [
        ("post_id", "str", lambda: str(uuid.uuid4())),
        ("user_id", "int", lambda: rand_int(1, 50000)),
        ("username", "str", lambda: f"user_{rand_int(1,50000):06d}"),
        ("platform", "str", lambda: random.choice(["twitter", "instagram", "facebook", "tiktok"])),
        ("content", "str", lambda: rand_str(140)),
        ("timestamp", "datetime", lambda: rand_datetime()),
        ("likes", "int", lambda: rand_int(0, 80000)),
        ("shares", "int", lambda: rand_int(0, 30000)),
        ("comments", "int", lambda: rand_int(0, 2000)),
        ("impressions", "int", lambda: rand_int(0, 500000)),
        ("followers", "int", lambda: random.choice([100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000])),
    ],

    "blockchain": [
        ("signature", "str", lambda: rand_str(44)),
        ("slot", "int", lambda: rand_int(250000000, 260000000)),
        ("timestamp", "datetime", lambda: rand_datetime()),
        ("fee", "int", lambda: random.choice([5000, 10000])),
        ("status", "str", lambda: random.choice(["success", "failed", "success", "success"])),
        ("signer", "str", lambda: rand_str(32)),
        ("num_accounts", "int", lambda: rand_int(3, 15)),
        ("compute_units", "int", lambda: rand_int(120000, 280000)),
        ("instructions", "int", lambda: rand_int(1, 12)),
        ("has_error_log", "str", lambda: random.choice(["true", "false", "false", "false"])),
        ("programs", "str", lambda: "|".join(random.sample(["raydium", "jupiter", "orca", "meteora", "phosphor", "marinade"], rand_int(1, 4)))),
        ("first_program", "str", lambda: random.choice(["raydium", "jupiter", "orca", "meteora", "phosphor"])),
        ("first_instruction_data", "str", lambda: random.choice(["close_account", "swap", "initialize", "transfer", "delegate", "withdraw"])),
    ],
}

# ─── Main generator ──────────────────────────────────────────────────────────

def generate_row(schema_name: str) -> dict:
    """Generate a single row based on schema."""
    if schema_name not in SCHEMAS:
        raise ValueError(f"Unknown schema: {schema_name}. Available: {list(SCHEMAS.keys())}")

    row = {}
    for field_name, field_type, generator in SCHEMAS[schema_name]:
        if field_type == "tags":
            row[field_name] = generator()
        elif field_type == "nested":
            row[field_name] = json.dumps(generator(), ensure_ascii=False)
        else:
            row[field_name] = generator()
    return row

def generate_dataset(
    schema: str,
    rows: int,
    output_path: str,
    format: str = "csv",
    seed: int | None = None,
):
    """Generate a benchmark dataset.

    Args:
        schema: Schema name (standard, ecommerce, logs, nested, sparse)
        rows: Number of rows to generate
        output_path: Output file/directory path
        format: Output format (csv, jsonl)
        seed: Random seed for reproducibility (optional)
    """
    if seed is not None:
        random.seed(seed)

    print(f"Generating {rows:,} rows with schema '{schema}'...")
    print(f"Output: {output_path}")
    if seed is not None:
        print(f"Random seed: {seed}")

    fieldnames = [name for name, _, _ in SCHEMAS[schema]]

    # Anchor summary: collect expected spike counts
    anchors = CONTROL_ANCHORS.get(schema, {})
    anchor_summary: list[tuple[str, str, int]] = []  # (category, key, expected_count)

    for tag, interval in anchors.get("tags_spikes", []):
        expected = rows // interval
        anchor_summary.append(("tags", tag, expected))
        print(f"  Anchor: tag='{tag}' every {interval} rows → ~{expected} rows")

    for views_val, interval in anchors.get("views_spikes", []):
        expected = rows // interval
        anchor_summary.append(("views", str(views_val), expected))
        print(f"  Anchor: views={views_val} every {interval} rows → ~{expected} rows")

    for author_prefix, interval in anchors.get("author_spikes", []):
        expected = rows // interval
        anchor_summary.append(("author", author_prefix, expected))
        print(f"  Anchor: author='{author_prefix}_*' every {interval} rows → ~{expected} rows")

    for keyword, interval in anchors.get("content_spikes", []):
        expected = rows // interval
        anchor_summary.append(("content", keyword, expected))
        print(f"  Anchor: content contains '{keyword}' every {interval} rows → ~{expected} rows")

    for year, interval in anchors.get("year_spikes", []):
        expected = rows // interval
        anchor_summary.append(("year", str(year), expected))
        print(f"  Anchor: published_at year={year} every {interval} rows → ~{expected} rows")

    for platform, interval in anchors.get("platform_spikes", []):
        expected = rows // interval
        anchor_summary.append(("platform", platform, expected))
        print(f"  Anchor: platform='{platform}' every {interval} rows → ~{expected} rows")

    for likes_val, interval in anchors.get("likes_spikes", []):
        expected = rows // interval
        anchor_summary.append(("likes", str(likes_val), expected))
        print(f"  Anchor: likes={likes_val} every {interval} rows → ~{expected} rows")

    for level, interval in anchors.get("level_spikes", []):
        expected = rows // interval
        anchor_summary.append(("level", level, expected))
        print(f"  Anchor: level='{level}' every {interval} rows → ~{expected} rows")

    for service, interval in anchors.get("service_spikes", []):
        expected = rows // interval
        anchor_summary.append(("service", service, expected))
        print(f"  Anchor: service='{service}' every {interval} rows → ~{expected} rows")

    for dur_val, interval in anchors.get("duration_spikes", []):
        expected = rows // interval
        anchor_summary.append(("duration", str(dur_val), expected))
        print(f"  Anchor: duration_ms={dur_val} every {interval} rows → ~{expected} rows")

    if not anchor_summary:
        print("  No controlled anchors defined for this schema.")

    if format == "csv":
        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_NONNUMERIC)
            writer.writeheader()
            for i in range(rows):
                if i % 50000 == 0 and i > 0:
                    print(f"  Generated {i:,} / {rows:,} rows...")
                row = _generate_article_anchor(i, rows) if schema == "article" else None
                if row is None:
                    row = generate_row(schema)
                    for field_name, field_type, _ in SCHEMAS[schema]:
                        if field_type == "tags" and field_name in row:
                            row[field_name] = json.dumps(row[field_name])
                writer.writerow(row)
        print(f"  Done: {rows:,} rows written to {output_path}")

    elif format == "jsonl":
        with open(output_path, "w") as f:
            for i in range(rows):
                if i % 50000 == 0 and i > 0:
                    print(f"  Generated {i:,} / {rows:,} rows...")
                row = _generate_article_anchor(i, rows) if schema == "article" else None
                if row is None:
                    row = generate_row(schema)
                f.write(json.dumps(row) + "\n")
        print(f"  Done: {rows:,} rows written to {output_path}")

    else:
        raise ValueError(f"Unknown format: {format}. Use 'csv' or 'jsonl'")

    # Return anchor summary so benchmarks can verify counts
    return anchor_summary

def main():
    parser = argparse.ArgumentParser(description="Generate benchmark datasets")
    parser.add_argument("--schema", "-s", default="standard",
                        choices=list(SCHEMAS.keys()),
                        help="Schema to use (default: standard)")
    parser.add_argument("--rows", "-r", type=int, default=100000,
                        help="Number of rows to generate (default: 100,000)")
    parser.add_argument("--output", "-o", required=True,
                        help="Output file path")
    parser.add_argument("--format", "-f", default="csv",
                        choices=["csv", "jsonl"],
                        help="Output format (default: csv)")
    parser.add_argument("--sizes", nargs="+", type=int,
                        help="Generate multiple sizes (e.g. --sizes 1000 10000 100000)")

    args = parser.parse_args()

    if args.sizes:
        # Generate multiple sizes
        base, ext = os.path.splitext(args.output)
        for size in args.sizes:
            output = f"{base}_{size}{ext}"
            generate_dataset(args.schema, size, output, args.format)
    else:
        generate_dataset(args.schema, args.rows, args.output, args.format)

if __name__ == "__main__":
    main()