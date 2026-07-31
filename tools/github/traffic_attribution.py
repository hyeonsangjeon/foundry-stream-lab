#!/usr/bin/env python3
"""Capture and classify GitHub repository traffic across a repository rename."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


API_VERSION = "2022-11-28"
POPULAR_PATH_LIMIT = 10
ROLLING_WINDOW_DAYS = 14
_REPOSITORY_RE = re.compile(r"^[A-Za-z0-9.-]+/[A-Za-z0-9_.-]+$")


class TrafficAttributionError(RuntimeError):
    """Raised when traffic inputs cannot be collected or safely interpreted."""


def parse_timestamp(value: str, *, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise TrafficAttributionError(f"{field} must be a non-empty ISO-8601 timestamp")
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise TrafficAttributionError(
            f"{field} is not a valid ISO-8601 timestamp: {value}"
        ) from exc
    if parsed.tzinfo is None:
        raise TrafficAttributionError(f"{field} must include a UTC offset")
    return parsed.astimezone(timezone.utc)


def format_timestamp(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def normalize_repository(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not _REPOSITORY_RE.fullmatch(value):
        raise TrafficAttributionError(f"{field} must have the form OWNER/REPOSITORY")
    return value


def _require_mapping(value: Any, *, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TrafficAttributionError(f"{field} must be a JSON object")
    return value


def _require_list(value: Any, *, field: str) -> list[Any]:
    if not isinstance(value, list):
        raise TrafficAttributionError(f"{field} must be a JSON array")
    return value


def _non_negative_integer(value: Any, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise TrafficAttributionError(f"{field} must be a non-negative integer")
    return value


def _string(value: Any, *, field: str) -> str:
    if not isinstance(value, str):
        raise TrafficAttributionError(f"{field} must be a string")
    return value


def _read_json(path: Path, *, field: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise TrafficAttributionError(f"{field} does not exist: {path}") from exc
    except OSError as exc:
        raise TrafficAttributionError(
            f"could not read {field} at {path}: {exc}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise TrafficAttributionError(f"{field} is not valid JSON: {exc}") from exc


def _gh_api(endpoint: str) -> Any:
    command = [
        "gh",
        "api",
        "--method",
        "GET",
        "-H",
        "Accept: application/vnd.github+json",
        "-H",
        f"X-GitHub-Api-Version: {API_VERSION}",
        endpoint,
    ]
    try:
        completed = subprocess.run(
            command,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=30,
        )
    except FileNotFoundError as exc:
        raise TrafficAttributionError(
            "GitHub CLI (gh) is required for live collection"
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise TrafficAttributionError(
            f"GitHub API request timed out: {endpoint}"
        ) from exc
    if completed.returncode != 0:
        detail = completed.stderr.strip() or f"exit status {completed.returncode}"
        raise TrafficAttributionError(
            f"GitHub API request failed for {endpoint}: {detail}"
        )
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise TrafficAttributionError(
            f"GitHub API returned invalid JSON for {endpoint}"
        ) from exc


def collect_live(repository: str) -> tuple[Any, Any, Any, Any]:
    base = f"repos/{repository}/traffic"
    return (
        _gh_api(f"{base}/views"),
        _gh_api(f"{base}/clones"),
        _gh_api(f"{base}/popular/paths"),
        _gh_api(f"{base}/popular/referrers"),
    )


def repository_path_matches(path: str, repository: str) -> bool:
    """Match one GitHub repository path without accepting prefix collisions."""

    prefix = f"/{repository}".casefold()
    candidate = path.casefold()
    return candidate == prefix or candidate.startswith(f"{prefix}/")


def classify_path(path: str, canonical: str, legacy: Sequence[str]) -> str:
    if repository_path_matches(path, canonical):
        return "canonical"
    if any(repository_path_matches(path, repository) for repository in legacy):
        return "legacy"
    return "other"


def _normalize_daily_views(views_payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    daily_payload = _require_list(views_payload.get("views"), field="views.views")
    daily: list[dict[str, Any]] = []
    seen_timestamps: set[str] = set()
    for index, raw_row in enumerate(daily_payload):
        row = _require_mapping(raw_row, field=f"views.views[{index}]")
        parsed = parse_timestamp(
            _string(row.get("timestamp"), field=f"views.views[{index}].timestamp"),
            field=f"views.views[{index}].timestamp",
        )
        timestamp = format_timestamp(parsed)
        if timestamp in seen_timestamps:
            raise TrafficAttributionError(
                f"views.views contains duplicate timestamp {timestamp}"
            )
        seen_timestamps.add(timestamp)
        daily.append(
            {
                "timestamp": timestamp,
                "views": _non_negative_integer(
                    row.get("count"), field=f"views.views[{index}].count"
                ),
                "uniqueVisitors": _non_negative_integer(
                    row.get("uniques"), field=f"views.views[{index}].uniques"
                ),
            }
        )
    return sorted(daily, key=lambda row: row["timestamp"])


def _normalize_daily_clones(clones_payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    daily_payload = _require_list(clones_payload.get("clones"), field="clones.clones")
    daily: list[dict[str, Any]] = []
    seen_timestamps: set[str] = set()
    for index, raw_row in enumerate(daily_payload):
        row = _require_mapping(raw_row, field=f"clones.clones[{index}]")
        parsed = parse_timestamp(
            _string(row.get("timestamp"), field=f"clones.clones[{index}].timestamp"),
            field=f"clones.clones[{index}].timestamp",
        )
        timestamp = format_timestamp(parsed)
        if timestamp in seen_timestamps:
            raise TrafficAttributionError(
                f"clones.clones contains duplicate timestamp {timestamp}"
            )
        seen_timestamps.add(timestamp)
        daily.append(
            {
                "timestamp": timestamp,
                "clones": _non_negative_integer(
                    row.get("count"), field=f"clones.clones[{index}].count"
                ),
                "uniqueCloners": _non_negative_integer(
                    row.get("uniques"), field=f"clones.clones[{index}].uniques"
                ),
            }
        )
    return sorted(daily, key=lambda row: row["timestamp"])


def _normalize_paths(
    paths_payload: Any,
    *,
    canonical: str,
    legacy: Sequence[str],
) -> list[dict[str, Any]]:
    rows = _require_list(paths_payload, field="popular paths")
    normalized: list[dict[str, Any]] = []
    seen_paths: set[str] = set()
    for index, raw_row in enumerate(rows):
        row = _require_mapping(raw_row, field=f"popular paths[{index}]")
        path = _string(row.get("path"), field=f"popular paths[{index}].path")
        if not path.startswith("/"):
            raise TrafficAttributionError(
                f"popular paths[{index}].path must start with /"
            )
        dedupe_key = path.casefold()
        if dedupe_key in seen_paths:
            raise TrafficAttributionError(
                f"popular paths contains duplicate path {path}"
            )
        seen_paths.add(dedupe_key)
        normalized.append(
            {
                "attribution": classify_path(path, canonical, legacy),
                "path": path,
                "title": _string(
                    row.get("title"), field=f"popular paths[{index}].title"
                ),
                "views": _non_negative_integer(
                    row.get("count"), field=f"popular paths[{index}].count"
                ),
                "uniqueVisitors": _non_negative_integer(
                    row.get("uniques"), field=f"popular paths[{index}].uniques"
                ),
            }
        )
    normalized.sort(
        key=lambda row: (-row["views"], row["path"].casefold(), row["title"].casefold())
    )
    return normalized


def _normalize_referrers(referrers_payload: Any) -> list[dict[str, Any]]:
    rows = _require_list(referrers_payload, field="popular referrers")
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw_row in enumerate(rows):
        row = _require_mapping(raw_row, field=f"popular referrers[{index}]")
        referrer = _string(
            row.get("referrer"), field=f"popular referrers[{index}].referrer"
        )
        dedupe_key = referrer.casefold()
        if dedupe_key in seen:
            raise TrafficAttributionError(
                f"popular referrers contains duplicate referrer {referrer}"
            )
        seen.add(dedupe_key)
        normalized.append(
            {
                "referrer": referrer,
                "views": _non_negative_integer(
                    row.get("count"), field=f"popular referrers[{index}].count"
                ),
                "uniqueVisitors": _non_negative_integer(
                    row.get("uniques"), field=f"popular referrers[{index}].uniques"
                ),
            }
        )
    normalized.sort(key=lambda row: (-row["views"], row["referrer"].casefold()))
    return normalized


def _window(
    daily_views: Sequence[Mapping[str, Any]], *, now: datetime, renamed_at: datetime
) -> dict[str, Any]:
    if daily_views:
        dates = [
            parse_timestamp(row["timestamp"], field="daily timestamp").date()
            for row in daily_views
        ]
        start_date = min(dates)
        end_date = max(dates)
        source = "observed-daily-buckets"
    else:
        end_date = now.date()
        start_date = end_date - timedelta(days=ROLLING_WINDOW_DAYS - 1)
        source = "inferred-from-analysis-time"
    rename_date = renamed_at.date()
    if end_date < rename_date:
        rename_relation = "pre-rename"
    elif start_date <= rename_date <= end_date:
        rename_relation = "overlaps-rename"
    else:
        rename_relation = "post-rename"
    return {
        "days": ROLLING_WINDOW_DAYS,
        "endDateUtc": end_date.isoformat(),
        "observedDailyBuckets": len(daily_views),
        "renameRelation": rename_relation,
        "source": source,
        "startDateUtc": start_date.isoformat(),
        "type": "rolling",
        "windowOverlapsRename": rename_relation == "overlaps-rename",
    }


def _clone_date_relation_counts(
    daily_clones: Sequence[Mapping[str, Any]], *, renamed_at: datetime
) -> dict[str, int]:
    rename_date = renamed_at.date()
    counts = {
        "preRenameDateClones": 0,
        "renameDateClones": 0,
        "postRenameDateClones": 0,
    }
    for row in daily_clones:
        clone_date = parse_timestamp(
            row["timestamp"], field="daily clone timestamp"
        ).date()
        if clone_date < rename_date:
            key = "preRenameDateClones"
        elif clone_date == rename_date:
            key = "renameDateClones"
        else:
            key = "postRenameDateClones"
        counts[key] += row["clones"]
    return counts


def _percentage(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return 0.0 if numerator == 0 else None
    return round(100.0 * numerator / denominator, 1)


def _ratio(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return 1.0 if numerator == 0 else None
    return round(numerator / denominator, 4)


def build_report(
    *,
    repository: str,
    legacy_repositories: Sequence[str],
    renamed_at: datetime,
    now: datetime,
    views_payload: Any,
    paths_payload: Any,
    referrers_payload: Any,
    clones_payload: Any | None = None,
) -> dict[str, Any]:
    canonical = normalize_repository(repository, field="repository")
    if renamed_at > now:
        raise TrafficAttributionError(
            "renamed-at must not be later than the analysis timestamp"
        )
    legacy = [
        normalize_repository(value, field="legacy repository")
        for value in legacy_repositories
    ]
    deduplicated_legacy: list[str] = []
    seen_repositories = {canonical.casefold()}
    for value in legacy:
        if value.casefold() in seen_repositories:
            raise TrafficAttributionError(
                "canonical and legacy repositories must be distinct and non-duplicated"
            )
        seen_repositories.add(value.casefold())
        deduplicated_legacy.append(value)
    if not deduplicated_legacy:
        raise TrafficAttributionError("at least one legacy repository is required")

    views = _require_mapping(views_payload, field="views")
    total_views = _non_negative_integer(views.get("count"), field="views.count")
    total_uniques = _non_negative_integer(views.get("uniques"), field="views.uniques")
    daily_views = _normalize_daily_views(views)
    if clones_payload is None:
        total_clones = None
        total_unique_cloners = None
        daily_clones: list[dict[str, Any]] = []
        daily_clone_sum = None
        clone_window = None
        clone_date_relation_counts = None
    else:
        clones = _require_mapping(clones_payload, field="clones")
        total_clones = _non_negative_integer(clones.get("count"), field="clones.count")
        total_unique_cloners = _non_negative_integer(
            clones.get("uniques"), field="clones.uniques"
        )
        daily_clones = _normalize_daily_clones(clones)
        daily_clone_sum = sum(row["clones"] for row in daily_clones)
        clone_window = _window(daily_clones, now=now, renamed_at=renamed_at)
        clone_date_relation_counts = _clone_date_relation_counts(
            daily_clones, renamed_at=renamed_at
        )
    paths = _normalize_paths(
        paths_payload, canonical=canonical, legacy=deduplicated_legacy
    )
    referrers = _normalize_referrers(referrers_payload)

    listed_views = sum(row["views"] for row in paths)
    group_views = {
        group: sum(row["views"] for row in paths if row["attribution"] == group)
        for group in ("canonical", "legacy", "other")
    }
    groups = {
        group: {
            "shareOfListedPercent": _percentage(count, listed_views),
            "shareOfTotalPercent": _percentage(count, total_views),
            "views": count,
        }
        for group, count in group_views.items()
    }
    unattributed_views = max(total_views - listed_views, 0)
    over_attributed_views = max(listed_views - total_views, 0)
    if listed_views == total_views:
        coverage_status = "exact"
    elif listed_views < total_views:
        coverage_status = "partial"
    else:
        coverage_status = "over-total"

    window = _window(daily_views, now=now, renamed_at=renamed_at)
    fully_canonical = (
        group_views["legacy"] == 0
        and group_views["other"] == 0
        and unattributed_views == 0
        and over_attributed_views == 0
    )
    if window["renameRelation"] == "pre-rename":
        status = "pre-rename-window"
    elif fully_canonical:
        status = "canonical-only"
    elif window["windowOverlapsRename"]:
        status = "rename-window-ambiguous"
    elif group_views["legacy"] > 0:
        status = "legacy-paths-after-rename-window"
    elif group_views["other"] > 0 or coverage_status != "exact":
        status = "incomplete-path-attribution"
    else:
        status = "canonical-only"

    daily_sum = sum(row["views"] for row in daily_views)

    return {
        "analyzedAt": format_timestamp(now),
        "apiVersion": API_VERSION,
        "cloneTraffic": {
            "available": clones_payload is not None,
            "byDay": daily_clones,
            "clones": total_clones,
            "dailyClonesMatchTotal": (
                daily_clone_sum == total_clones if clones_payload is not None else None
            ),
            "dailyClonesSum": daily_clone_sum,
            "dailyUniqueClonersAdditive": False,
            "dateRelationCounts": clone_date_relation_counts,
            "fetchesIncluded": False if clones_payload is not None else None,
            "fullClonesOnly": True if clones_payload is not None else None,
            "requestedRepositorySlugAvailable": (
                False if clones_payload is not None else None
            ),
            "scope": "repository-aggregate" if clones_payload is not None else None,
            "slugAttributionAvailable": (False if clones_payload is not None else None),
            "uniqueClonerAuthority": (
                "clones.uniques" if clones_payload is not None else None
            ),
            "uniqueCloners": total_unique_cloners,
            "window": clone_window,
        },
        "interpretation": {
            "canonicalKnownTopPathViews": group_views["canonical"],
            "cloneDemandInferred": False,
            "cloneSlugAttributionAvailable": (
                False if clones_payload is not None else None
            ),
            "contentInterestInferred": False,
            "pathByDateAvailable": False,
            "status": status,
            "totalRepositoryViewsFullyAttributedToCanonicalPaths": fully_canonical,
        },
        "limitations": [
            "popular paths returns at most 10 rows",
            "path-level unique visitors overlap and are not additive",
            "GitHub does not expose path-by-date traffic",
            "rename-day views cannot be split into pre-rename and post-rename traffic",
            "clone traffic does not expose the requested canonical or legacy repository slug",
            "daily unique cloners overlap and are not additive",
        ],
        "metricDefinitions": {
            "canonical_known_top_path_views": (
                "views in returned popular paths matching the canonical repository boundary"
            ),
            "legacy_known_top_path_views": (
                "views in returned popular paths matching a legacy repository boundary"
            ),
            "not_in_top_paths_views": (
                "total repository views minus views covered by returned popular paths"
            ),
            "other_known_top_path_views": (
                "views in returned popular paths matching neither canonical nor legacy boundaries"
            ),
            "top_path_coverage": "returned popular-path views divided by total repository views",
            "total_clones": "rolling full-clone count from traffic/clones; fetches are excluded",
            "total_views": "rolling repository view count from traffic/views",
            "unique_cloners": "authoritative rolling unique count from traffic/clones uniques",
            "unique_visitors": "authoritative rolling unique count from traffic/views uniques",
        },
        "metrics": {
            "canonical_known_top_path_views": group_views["canonical"],
            "legacy_known_top_path_views": group_views["legacy"],
            "not_in_top_paths_views": unattributed_views,
            "other_known_top_path_views": group_views["other"],
            "top_path_coverage": _ratio(listed_views, total_views),
            "total_clones": total_clones,
            "total_views": total_views,
            "unique_cloners": total_unique_cloners,
            "unique_visitors": total_uniques,
        },
        "pathAttribution": {
            "coverageComplete": coverage_status == "exact",
            "coveragePercent": _percentage(listed_views, total_views),
            "coverageStatus": coverage_status,
            "groups": groups,
            "listedViews": listed_views,
            "overAttributedViews": over_attributed_views,
            "pathRowsMayBeTruncated": len(paths) >= POPULAR_PATH_LIMIT,
            "paths": paths,
            "rowLimit": POPULAR_PATH_LIMIT,
            "scope": "github-popular-paths",
            "unattributedViews": unattributed_views,
            "uniqueVisitorsAdditiveAcrossPaths": False,
        },
        "referrers": referrers,
        "rawResponses": {
            "clones": clones_payload,
            "popularPaths": paths_payload,
            "popularReferrers": referrers_payload,
            "views": views_payload,
        },
        "rename": {
            "legacyRepositories": deduplicated_legacy,
            "renamedAt": format_timestamp(renamed_at),
        },
        "repository": canonical,
        "schemaVersion": 2,
        "sourceAvailability": {
            "clones": clones_payload is not None,
            "popularPaths": True,
            "popularReferrers": True,
            "views": True,
        },
        "totals": {
            "clones": total_clones,
            "dailyClonesMatchTotal": (
                daily_clone_sum == total_clones if clones_payload is not None else None
            ),
            "dailyClonesSum": daily_clone_sum,
            "dailyViewsMatchTotal": daily_sum == total_views,
            "dailyViewsSum": daily_sum,
            "uniqueClonerAuthority": (
                "clones.uniques" if clones_payload is not None else None
            ),
            "uniqueCloners": total_unique_cloners,
            "uniqueVisitorAuthority": "views.uniques",
            "uniqueVisitors": total_uniques,
            "views": total_views,
        },
        "viewsByDay": daily_views,
        "window": window,
    }


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def _markdown_cell(value: Any) -> str:
    return str(value).replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ")


def _percent(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.1f}%"


def _decision_text(status: str) -> str:
    return {
        "pre-rename-window": (
            "The observed traffic window ends before the configured rename. Treat it as "
            "historical repository traffic, not canonical-name content interest."
        ),
        "rename-window-ambiguous": (
            "The rolling window overlaps the rename. Total repository views must not be "
            "interpreted as canonical content interest because GitHub does not expose a "
            "path-by-date cross-tab."
        ),
        "legacy-paths-after-rename-window": (
            "Legacy-labelled paths remain after the rename window. GitHub does not document "
            "whether these labels represent redirected requests or another analytics mapping, "
            "so keep the mapping unverified and investigate backlinks separately."
        ),
        "incomplete-path-attribution": (
            "Popular paths do not fully and exclusively attribute the total. Keep canonical, "
            "legacy, other, and unattributed views separate."
        ),
        "canonical-only": (
            "Every repository view in this snapshot is covered by canonical popular paths. "
            "This is path attribution only and does not infer content interest."
        ),
    }[status]


def render_markdown(report: Mapping[str, Any]) -> str:
    totals = report["totals"]
    attribution = report["pathAttribution"]
    clone_traffic = report["cloneTraffic"]
    window = report["window"]
    rename = report["rename"]
    interpretation = report["interpretation"]
    groups = attribution["groups"]
    lines = [
        "# GitHub traffic attribution",
        "",
        f"Repository: `{report['repository']}`  ",
        f"Analyzed at: `{report['analyzedAt']}`  ",
        f"Rename: `{', '.join(rename['legacyRepositories'])}` → `{report['repository']}` at "
        f"`{rename['renamedAt']}`",
        "",
        "## Decision",
        "",
        f"**{interpretation['status']}** — {_decision_text(interpretation['status'])}",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Total repository views | {totals['views']} |",
        f"| Overall unique visitors | {totals['uniqueVisitors']} |",
        f"| Total full clones | {totals['clones'] if totals['clones'] is not None else 'n/a'} |",
        f"| Overall unique cloners | "
        f"{totals['uniqueCloners'] if totals['uniqueCloners'] is not None else 'n/a'} |",
        f"| Canonical popular-path views | {groups['canonical']['views']} |",
        f"| Legacy popular-path views | {groups['legacy']['views']} |",
        f"| Other popular-path views | {groups['other']['views']} |",
        f"| Unattributed views outside returned popular paths | {attribution['unattributedViews']} |",
        f"| Popular-path coverage | {_percent(attribution['coveragePercent'])} |",
        "",
        "Overall unique visitors come only from `views.uniques`. Path-level unique counts "
        "overlap and are intentionally not summed.",
        "",
        "## Popular paths",
        "",
        "| Attribution | Path | Title | Views | Path uniques |",
        "| --- | --- | --- | ---: | ---: |",
    ]
    if attribution["paths"]:
        for row in attribution["paths"]:
            lines.append(
                "| {attribution} | {path} | {title} | {views} | {uniques} |".format(
                    attribution=_markdown_cell(row["attribution"]),
                    path=_markdown_cell(row["path"]),
                    title=_markdown_cell(row["title"]),
                    views=row["views"],
                    uniques=row["uniqueVisitors"],
                )
            )
    else:
        lines.append("| — | — | — | 0 | 0 |")

    lines.extend(
        [
            "",
            f"GitHub returns at most {attribution['rowLimit']} popular paths. Coverage is "
            f"`{attribution['coverageStatus']}`; a full top-10 response may still be truncated.",
            "",
            "## Rename window",
            "",
            f"- Rolling window: `{window['startDateUtc']}` through `{window['endDateUtc']}` UTC",
            f"- Rename relation: `{window['renameRelation']}`",
            f"- Window overlaps rename: `{'yes' if window['windowOverlapsRename'] else 'no'}`",
            "- Path-by-date attribution: unavailable from the GitHub Traffic API",
            "",
        ]
    )
    lines.extend(["## Clone traffic", ""])
    if clone_traffic["available"]:
        clone_window = clone_traffic["window"]
        relation_counts = clone_traffic["dateRelationCounts"]
        lines.extend(
            [
                f"- Rolling window: `{clone_window['startDateUtc']}` through "
                f"`{clone_window['endDateUtc']}` UTC",
                f"- Full clones: `{clone_traffic['clones']}`",
                f"- Unique cloners: `{clone_traffic['uniqueCloners']}` from `clones.uniques`",
                "- Fetches included: `no`",
                "- Requested canonical/legacy clone URL available: `no`",
                "- Canonical/legacy clone attribution: `unavailable`",
                "",
            ]
        )
        if not clone_traffic["dailyClonesMatchTotal"]:
            lines.extend(
                [
                    "**Warning:** Daily clone buckets sum to "
                    f"`{clone_traffic['dailyClonesSum']}`, but the endpoint total is "
                    f"`{clone_traffic['clones']}`. Treat the date-relation breakdown as "
                    "temporarily inconsistent.",
                    "",
                ]
            )
        lines.extend(
            [
                "| Clone date relation | Clones |",
                "| --- | ---: |",
                f"| Before rename UTC date | {relation_counts['preRenameDateClones']} |",
                "| Rename UTC date (ambiguous within the day) | "
                f"{relation_counts['renameDateClones']} |",
                f"| After rename UTC date | {relation_counts['postRenameDateClones']} |",
                "",
                "Date relation is not slug attribution. GitHub does not return the repository "
                "URL used for a clone, and daily unique-cloner counts are intentionally not summed.",
                "",
            ]
        )
    else:
        lines.extend(
            [
                "Clone input was not supplied. Clone totals and slug-attribution constraints "
                "cannot be evaluated in this offline compatibility mode.",
                "",
            ]
        )
    lines.extend(
        [
            "## Popular referrers",
            "",
            "| Referrer | Views | Referrer uniques |",
            "| --- | ---: | ---: |",
        ]
    )
    if report["referrers"]:
        for row in report["referrers"]:
            lines.append(
                f"| {_markdown_cell(row['referrer'])} | {row['views']} | "
                f"{row['uniqueVisitors']} |"
            )
    else:
        lines.append("| — | 0 | 0 |")
    lines.extend(
        [
            "",
            "## Interpretation constraints",
            "",
            "- `views` is the rolling repository total; it is not automatically a content-interest KPI.",
            "- Canonical, legacy, and other counts cover only the popular paths returned by GitHub.",
            "- Path-level unique counts are non-additive; use the overall unique visitor count above.",
            "- A rename-day view cannot be split into pre-rename and post-rename traffic.",
            "- Clone totals are repository-level full clones and cannot be split by canonical "
            "versus legacy clone URL.",
            "",
        ]
    )
    return "\n".join(lines)


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=str(path.parent), text=True
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
        os.replace(temporary_name, path)
    finally:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass


def _verify_archive(destination: Path, expected: Mapping[str, str]) -> None:
    for name, content in expected.items():
        path = destination / name
        try:
            existing = path.read_text(encoding="utf-8")
        except FileNotFoundError as exc:
            raise TrafficAttributionError(
                f"archive directory is incomplete: {destination}"
            ) from exc
        except OSError as exc:
            raise TrafficAttributionError(
                f"could not read archive file {path}: {exc}"
            ) from exc
        if existing != content:
            raise TrafficAttributionError(
                f"archive file already exists with different content: {path}"
            )


def _archive_report(
    directory: Path,
    report: Mapping[str, Any],
    *,
    json_content: str,
    markdown_content: str,
) -> Path:
    analyzed_at = parse_timestamp(report["analyzedAt"], field="analyzedAt")
    archive_key = analyzed_at.strftime("%Y-%m-%dT%H%M%SZ")
    destination = directory / archive_key
    checksums = (
        f"{hashlib.sha256(json_content.encode('utf-8')).hexdigest()}  snapshot.json\n"
        f"{hashlib.sha256(markdown_content.encode('utf-8')).hexdigest()}  summary.md\n"
    )
    expected = {
        "snapshot.json": json_content,
        "summary.md": markdown_content,
        "checksums.sha256": checksums,
    }
    directory.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        _verify_archive(destination, expected)
        return destination

    staging = Path(tempfile.mkdtemp(prefix=f".{archive_key}.", dir=str(directory)))
    try:
        for name, content in expected.items():
            _atomic_write(staging / name, content)
        try:
            os.rename(staging, destination)
        except OSError as exc:
            if not destination.exists():
                raise TrafficAttributionError(
                    f"could not publish archive directory {destination}: {exc}"
                ) from exc
            _verify_archive(destination, expected)
        else:
            staging = None
    finally:
        if staging is not None:
            shutil.rmtree(staging, ignore_errors=True)
    return destination


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Classify GitHub traffic across canonical and renamed repository paths."
    )
    parser.add_argument(
        "--repository", required=True, help="Canonical OWNER/REPOSITORY"
    )
    parser.add_argument(
        "--legacy-repository",
        action="append",
        required=True,
        help="Legacy OWNER/REPOSITORY; repeat for multiple old names",
    )
    parser.add_argument(
        "--renamed-at", required=True, help="Rename timestamp with UTC offset"
    )
    parser.add_argument("--output-json", required=True, type=Path)
    parser.add_argument("--output-markdown", required=True, type=Path)
    parser.add_argument(
        "--archive-directory",
        type=Path,
        help="Optional private directory for immutable timestamped output and checksums",
    )
    parser.add_argument(
        "--views-file", type=Path, help="Offline raw traffic/views JSON"
    )
    parser.add_argument(
        "--clones-file", type=Path, help="Offline raw traffic/clones JSON"
    )
    parser.add_argument(
        "--paths-file", type=Path, help="Offline raw traffic/popular/paths JSON"
    )
    parser.add_argument(
        "--referrers-file", type=Path, help="Offline raw traffic/popular/referrers JSON"
    )
    parser.add_argument(
        "--now",
        help="Analysis timestamp with UTC offset; freeze this for deterministic offline output",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    repository = normalize_repository(args.repository, field="repository")
    renamed_at = parse_timestamp(args.renamed_at, field="renamed-at")
    now = (
        parse_timestamp(args.now, field="now")
        if args.now
        else datetime.now(timezone.utc)
    )
    if renamed_at > now:
        raise TrafficAttributionError(
            "renamed-at must not be later than the analysis timestamp"
        )
    required_offline_paths = [
        args.views_file,
        args.paths_file,
        args.referrers_file,
    ]
    if any(path is not None for path in required_offline_paths) and not all(
        path is not None for path in required_offline_paths
    ):
        raise TrafficAttributionError(
            "--views-file, --paths-file, and --referrers-file must be provided together"
        )
    if args.clones_file is not None and not all(
        path is not None for path in required_offline_paths
    ):
        raise TrafficAttributionError(
            "--clones-file requires --views-file, --paths-file, and --referrers-file"
        )
    output_paths = {args.output_json.resolve(), args.output_markdown.resolve()}
    if len(output_paths) != 2:
        raise TrafficAttributionError(
            "JSON and Markdown outputs must use different paths"
        )
    if all(path is not None for path in required_offline_paths):
        supplied_input_paths = [
            *required_offline_paths,
            *([args.clones_file] if args.clones_file is not None else []),
        ]
        input_paths = {path.resolve() for path in supplied_input_paths}
        if output_paths & input_paths:
            raise TrafficAttributionError(
                "output paths must not overwrite offline input files"
            )
        views_payload = _read_json(args.views_file, field="views file")
        clones_payload = (
            _read_json(args.clones_file, field="clones file")
            if args.clones_file is not None
            else None
        )
        paths_payload = _read_json(args.paths_file, field="paths file")
        referrers_payload = _read_json(args.referrers_file, field="referrers file")
    else:
        views_payload, clones_payload, paths_payload, referrers_payload = collect_live(
            repository
        )

    report = build_report(
        repository=repository,
        legacy_repositories=args.legacy_repository,
        renamed_at=renamed_at,
        now=now,
        views_payload=views_payload,
        paths_payload=paths_payload,
        referrers_payload=referrers_payload,
        clones_payload=clones_payload,
    )
    json_content = render_json(report)
    markdown_content = render_markdown(report)
    if args.archive_directory is not None:
        archive_path = _archive_report(
            args.archive_directory,
            report,
            json_content=json_content,
            markdown_content=markdown_content,
        )
        print(f"Archived private traffic evidence: {archive_path}", file=sys.stderr)
    _atomic_write(args.output_json, json_content)
    _atomic_write(args.output_markdown, markdown_content)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except TrafficAttributionError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
