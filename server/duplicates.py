"""Duplicate detection engine using column-name Jaccard similarity, schema structure
matching, and fuzzy table-name comparison. Groups tables into duplicate clusters
and scores them for gold-standard recommendation."""

from __future__ import annotations
import re
from dataclasses import dataclass, field, asdict
from itertools import combinations

from server.scanner import TableInfo


def _normalize_col(name: str) -> str:
    """Normalize a column name for comparison: lowercase, strip underscores/prefixes."""
    name = name.lower().strip("_")
    for prefix in ("raw_", "dim_", "fact_", "_"):
        if name.startswith(prefix):
            name = name[len(prefix):]
    return name


# Maps common synonym pairs so renamed columns still match
_SYNONYMS = {
    "student_id": {"learner_id", "pupil_id"},
    "first_name": {"given_name", "pupil_first_name", "firstname"},
    "last_name": {"family_name", "pupil_last_name", "lastname"},
    "date_of_birth": {"dob", "pupil_dob"},
    "is_sen": {"has_send"},
    "fsm_eligible": {"pupil_premium"},
    "school_id": {"establishment_id"},
    "year_group": {"national_curriculum_year"},
    "score": {"mark", "rawscore", "percentage_score"},
    "grade": {"result_grade", "final_grade"},
    "exam_board": {"awarding_body"},
    "attendance_id": {"record_id", "recordid"},
    "status": {"attendancecode", "attendance_mark"},
    "session": {"am_pm"},
    "term": {"half_term"},
    "student_id": {"studentid", "learner_id", "pupil_id"},
    "school_name": {"schoolname"},
    "result_id": {"resultid"},
}

_REVERSE_SYNONYMS: dict[str, str] = {}
for canonical, synonyms in _SYNONYMS.items():
    for syn in synonyms:
        _REVERSE_SYNONYMS[syn] = canonical
    _REVERSE_SYNONYMS[canonical] = canonical


def _canonical_col(name: str) -> str:
    n = _normalize_col(name)
    return _REVERSE_SYNONYMS.get(n, n)


def column_similarity(cols_a: list[str], cols_b: list[str]) -> float:
    """Jaccard similarity on canonical column names, excluding metadata columns."""
    skip = {"_source_file", "_ingestion_ts", "_source", "_data_source", "_source_system"}
    set_a = {_canonical_col(c) for c in cols_a if c.lower() not in skip}
    set_b = {_canonical_col(c) for c in cols_b if c.lower() not in skip}
    if not set_a or not set_b:
        return 0.0
    intersection = set_a & set_b
    union = set_a | set_b
    return len(intersection) / len(union)


def _type_str(t) -> str:
    """Convert a type_name to a lowercase string, handling enums."""
    if hasattr(t, 'value'):
        return str(t.value).lower()
    return str(t).lower()


def type_similarity(table_a: TableInfo, table_b: TableInfo) -> float:
    """Compare column types for columns that match by canonical name."""
    map_a = {_canonical_col(c.name): _type_str(c.type_name) for c in table_a.columns}
    map_b = {_canonical_col(c.name): _type_str(c.type_name) for c in table_b.columns}
    shared = set(map_a.keys()) & set(map_b.keys())
    if not shared:
        return 0.0
    matches = sum(1 for k in shared if _types_compatible(map_a[k], map_b[k]))
    return matches / len(shared)


def _types_compatible(t1: str, t2: str) -> bool:
    if t1 == t2:
        return True
    numeric = {"int", "long", "bigint", "double", "float", "decimal", "short", "integer"}
    if t1 in numeric and t2 in numeric:
        return True
    string_like = {"string", "varchar", "char", "text"}
    if t1 in string_like and t2 in string_like:
        return True
    return False


def _tokenize_name(name: str) -> set[str]:
    parts = re.split(r"[_\s-]+", name.lower())
    return {p for p in parts if p not in ("raw", "dim", "fact", "agg")}


def name_similarity(name_a: str, name_b: str) -> float:
    """Token-based name similarity, stripping common prefixes."""
    tokens_a = _tokenize_name(name_a)
    tokens_b = _tokenize_name(name_b)
    if not tokens_a or not tokens_b:
        return 0.0
    intersection = tokens_a & tokens_b
    union = tokens_a | tokens_b
    return len(intersection) / len(union)


@dataclass
class DuplicatePair:
    table_a: str
    table_b: str
    column_similarity: float
    type_similarity: float
    name_similarity: float
    composite_score: float


@dataclass
class DuplicateGroup:
    group_id: int
    tables: list[str]
    pairs: list[DuplicatePair]
    gold_standard: str | None = None
    gold_scores: dict = field(default_factory=dict)

    def to_dict(self):
        return {
            "group_id": self.group_id,
            "tables": self.tables,
            "pairs": [asdict(p) for p in self.pairs],
            "gold_standard": self.gold_standard,
            "gold_scores": self.gold_scores,
        }


def detect_duplicates(
    tables: list[TableInfo],
    threshold: float = 0.5,
    col_weight: float = 0.50,
    type_weight: float = 0.30,
    name_weight: float = 0.20,
) -> list[DuplicateGroup]:
    """Find duplicate table groups based on composite similarity."""
    pairs: list[DuplicatePair] = []

    for ta, tb in combinations(tables, 2):
        # Skip tables in the same schema (bronze vs silver of same entity is expected)
        if ta.schema == tb.schema:
            continue

        cols_a = [c.name for c in ta.columns]
        cols_b = [c.name for c in tb.columns]
        col_sim = column_similarity(cols_a, cols_b)
        typ_sim = type_similarity(ta, tb)
        nm_sim = name_similarity(ta.name, tb.name)

        composite = col_sim * col_weight + typ_sim * type_weight + nm_sim * name_weight
        if composite >= threshold:
            pairs.append(DuplicatePair(
                table_a=ta.full_name,
                table_b=tb.full_name,
                column_similarity=round(col_sim, 3),
                type_similarity=round(typ_sim, 3),
                name_similarity=round(nm_sim, 3),
                composite_score=round(composite, 3),
            ))

    groups = _cluster_pairs(pairs)

    table_map = {t.full_name: t for t in tables}
    for group in groups:
        group_tables = [table_map[n] for n in group.tables if n in table_map]
        scores = score_gold_standard(group_tables)
        group.gold_scores = scores
        if scores:
            group.gold_standard = max(scores, key=scores.get)

    return groups


def _cluster_pairs(pairs: list[DuplicatePair]) -> list[DuplicateGroup]:
    """Union-find clustering of table pairs into groups."""
    parent: dict[str, str] = {}

    def find(x):
        while parent.get(x, x) != x:
            parent[x] = parent.get(parent[x], parent[x])
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for p in pairs:
        parent.setdefault(p.table_a, p.table_a)
        parent.setdefault(p.table_b, p.table_b)
        union(p.table_a, p.table_b)

    clusters: dict[str, list[str]] = {}
    for node in parent:
        root = find(node)
        clusters.setdefault(root, []).append(node)

    groups = []
    for i, (_, members) in enumerate(sorted(clusters.items())):
        group_pairs = [p for p in pairs if p.table_a in members or p.table_b in members]
        groups.append(DuplicateGroup(
            group_id=i + 1,
            tables=sorted(members),
            pairs=sorted(group_pairs, key=lambda p: -p.composite_score),
        ))

    return sorted(groups, key=lambda g: -max(p.composite_score for p in g.pairs) if g.pairs else 0)


def score_gold_standard(tables: list[TableInfo]) -> dict[str, float]:
    """Score each table for gold-standard recommendation."""
    if not tables:
        return {}

    scores: dict[str, float] = {}
    for t in tables:
        s = 0.0

        # Column completeness: more columns = more comprehensive
        max_cols = max(len(tt.columns) for tt in tables) or 1
        s += (len(t.columns) / max_cols) * 25

        # Has documentation
        if t.comment:
            s += 20

        # Naming convention: dim_/fact_ prefix indicates well-governed
        if t.name.startswith(("dim_", "fact_")):
            s += 15

        # In gold schema
        if t.schema == "gold":
            s += 20

        # Freshness: more recently updated
        if t.updated_at:
            all_updated = [tt.updated_at for tt in tables if tt.updated_at]
            if all_updated:
                max_updated = max(all_updated)
                min_updated = min(all_updated)
                span = max_updated - min_updated
                if span > 0:
                    s += ((t.updated_at - min_updated) / span) * 10
                else:
                    s += 10

        # Row count: larger often means more complete
        if t.row_count:
            all_counts = [tt.row_count for tt in tables if tt.row_count]
            if all_counts:
                max_count = max(all_counts) or 1
                s += (t.row_count / max_count) * 10

        scores[t.full_name] = round(s, 1)

    return scores
