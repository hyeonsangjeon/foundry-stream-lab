from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "github_traffic_attribution", ROOT / "traffic_attribution.py"
)
assert SPEC and SPEC.loader
TRAFFIC = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(TRAFFIC)


def utc(value: str) -> datetime:
    return TRAFFIC.parse_timestamp(value, field="test timestamp")


def views_payload(*, count: int = 11, uniques: int = 1) -> dict:
    daily = []
    for day in range(7, 21):
        daily.append(
            {
                "timestamp": f"2026-07-{day:02d}T00:00:00Z",
                "count": 10 if day == 17 else 1 if day == 19 else 0,
                "uniques": 1 if day in {17, 19} else 0,
            }
        )
    return {"count": count, "uniques": uniques, "views": daily}


def current_paths() -> list[dict]:
    return [
        {
            "path": "/hyeonsangjeon/kafka-metric-example",
            "title": "Overview",
            "count": 4,
            "uniques": 1,
        },
        {
            "path": "/hyeonsangjeon/foundry-stream-lab",
            "title": "Overview",
            "count": 2,
            "uniques": 1,
        },
        {
            "path": (
                "/hyeonsangjeon/kafka-metric-example/blob/master/docs/design/"
                "compare-run-simulator-desktop.jpg"
            ),
            "title": "/blob/master/docs/design/compare-run-simulator-desktop.jpg",
            "count": 2,
            "uniques": 1,
        },
        {
            "path": "/hyeonsangjeon/kafka-metric-example/pulls",
            "title": "/pulls",
            "count": 2,
            "uniques": 1,
        },
        {
            "path": "/hyeonsangjeon/kafka-metric-example/releases/tag/v1.2.0",
            "title": "/releases/tag/v1.2.0",
            "count": 1,
            "uniques": 1,
        },
    ]


def followup_paths() -> list[dict]:
    paths = current_paths()
    paths[1] = paths[1] | {"count": 3}
    return paths


def clones_payload(*, count: int = 332, uniques: int = 109) -> dict:
    return {
        "count": count,
        "uniques": uniques,
        "clones": [
            {
                "timestamp": "2026-07-16T00:00:00Z",
                "count": 99,
                "uniques": 30,
            },
            {
                "timestamp": "2026-07-17T00:00:00Z",
                "count": 177,
                "uniques": 56,
            },
            {
                "timestamp": "2026-07-18T00:00:00Z",
                "count": 56,
                "uniques": 40,
            },
        ],
    }


def build(**overrides):
    arguments = {
        "repository": "hyeonsangjeon/foundry-stream-lab",
        "legacy_repositories": ["hyeonsangjeon/kafka-metric-example"],
        "renamed_at": utc("2026-07-17T05:39:55Z"),
        "now": utc("2026-07-21T16:12:09Z"),
        "views_payload": views_payload(),
        "clones_payload": clones_payload(),
        "paths_payload": current_paths(),
        "referrers_payload": [{"referrer": "github.com", "count": 11, "uniques": 1}],
    }
    arguments.update(overrides)
    return TRAFFIC.build_report(**arguments)


class TrafficAttributionTests(unittest.TestCase):
    def test_current_snapshot_splits_nine_legacy_and_two_canonical_views(self):
        report = build()

        self.assertEqual(report["totals"]["views"], 11)
        self.assertEqual(report["totals"]["uniqueVisitors"], 1)
        self.assertEqual(report["pathAttribution"]["groups"]["legacy"]["views"], 9)
        self.assertEqual(report["pathAttribution"]["groups"]["canonical"]["views"], 2)
        self.assertEqual(
            report["pathAttribution"]["groups"]["legacy"]["shareOfTotalPercent"], 81.8
        )
        self.assertEqual(report["pathAttribution"]["coveragePercent"], 100.0)
        self.assertEqual(report["pathAttribution"]["unattributedViews"], 0)
        self.assertEqual(report["metrics"]["top_path_coverage"], 1.0)
        self.assertEqual(report["metrics"]["legacy_known_top_path_views"], 9)
        self.assertEqual(report["rawResponses"]["views"]["count"], 11)
        self.assertEqual(report["rawResponses"]["clones"]["count"], 332)
        self.assertIn("total_views", report["metricDefinitions"])
        self.assertEqual(report["interpretation"]["status"], "rename-window-ambiguous")
        self.assertFalse(report["interpretation"]["contentInterestInferred"])
        self.assertFalse(
            report["interpretation"][
                "totalRepositoryViewsFullyAttributedToCanonicalPaths"
            ]
        )
        self.assertFalse(report["interpretation"]["cloneDemandInferred"])
        self.assertFalse(report["interpretation"]["cloneSlugAttributionAvailable"])

    def test_followup_snapshot_splits_nine_legacy_and_three_canonical_views(self):
        report = build(
            views_payload=views_payload(count=12),
            paths_payload=followup_paths(),
            referrers_payload=[{"referrer": "github.com", "count": 12, "uniques": 1}],
        )

        self.assertEqual(report["metrics"]["total_views"], 12)
        self.assertEqual(report["metrics"]["canonical_known_top_path_views"], 3)
        self.assertEqual(report["metrics"]["legacy_known_top_path_views"], 9)
        self.assertEqual(report["metrics"]["not_in_top_paths_views"], 0)
        self.assertEqual(report["referrers"][0]["views"], 12)

    def test_repository_matching_requires_an_exact_path_boundary(self):
        paths = [
            {
                "path": "/hyeonsangjeon/kafka-metric-example-copy",
                "title": "Prefix collision",
                "count": 3,
                "uniques": 2,
            },
            {
                "path": "/HYEONSANGJEON/FOUNDRY-STREAM-LAB/issues",
                "title": "Canonical case variant",
                "count": 2,
                "uniques": 1,
            },
        ]
        payload = {"count": 5, "uniques": 2, "views": []}
        report = build(views_payload=payload, paths_payload=paths)

        self.assertEqual(report["pathAttribution"]["groups"]["other"]["views"], 3)
        self.assertEqual(report["pathAttribution"]["groups"]["canonical"]["views"], 2)
        self.assertTrue(
            TRAFFIC.repository_path_matches(
                "/hyeonsangjeon/kafka-metric-example/pulls",
                "hyeonsangjeon/kafka-metric-example",
            )
        )
        self.assertFalse(
            TRAFFIC.repository_path_matches(
                "/hyeonsangjeon/kafka-metric-example-copy",
                "hyeonsangjeon/kafka-metric-example",
            )
        )

    def test_top_ten_shortfall_is_reported_as_unattributed_not_canonical(self):
        report = build(
            views_payload=views_payload(count=20), paths_payload=current_paths()
        )

        attribution = report["pathAttribution"]
        self.assertEqual(attribution["listedViews"], 11)
        self.assertEqual(attribution["unattributedViews"], 9)
        self.assertEqual(attribution["coveragePercent"], 55.0)
        self.assertEqual(attribution["coverageStatus"], "partial")
        self.assertFalse(attribution["coverageComplete"])

    def test_path_uniques_are_preserved_but_never_summed_into_a_group(self):
        report = build()
        attribution = report["pathAttribution"]

        self.assertFalse(attribution["uniqueVisitorsAdditiveAcrossPaths"])
        self.assertEqual(report["totals"]["uniqueVisitorAuthority"], "views.uniques")
        self.assertNotIn("uniqueVisitors", attribution["groups"]["legacy"])
        self.assertEqual(sum(row["uniqueVisitors"] for row in attribution["paths"]), 5)
        self.assertEqual(report["totals"]["uniqueVisitors"], 1)

    def test_clone_totals_are_preserved_without_slug_attribution(self):
        report = build()
        clone_traffic = report["cloneTraffic"]

        self.assertTrue(clone_traffic["available"])
        self.assertEqual(clone_traffic["clones"], 332)
        self.assertEqual(clone_traffic["uniqueCloners"], 109)
        self.assertEqual(clone_traffic["dailyClonesSum"], 332)
        self.assertTrue(clone_traffic["dailyClonesMatchTotal"])
        self.assertFalse(clone_traffic["dailyUniqueClonersAdditive"])
        self.assertFalse(clone_traffic["slugAttributionAvailable"])
        self.assertFalse(clone_traffic["requestedRepositorySlugAvailable"])
        self.assertEqual(
            clone_traffic["dateRelationCounts"],
            {
                "preRenameDateClones": 99,
                "renameDateClones": 177,
                "postRenameDateClones": 56,
            },
        )
        self.assertEqual(report["metrics"]["total_clones"], 332)
        self.assertEqual(report["metrics"]["unique_cloners"], 109)
        self.assertIn(
            "Canonical/legacy clone attribution", TRAFFIC.render_markdown(report)
        )

    def test_clone_daily_total_mismatch_is_reported_without_rejection(self):
        report = build(clones_payload=clones_payload(count=333))

        self.assertEqual(report["cloneTraffic"]["dailyClonesSum"], 332)
        self.assertFalse(report["cloneTraffic"]["dailyClonesMatchTotal"])
        self.assertIn(
            "Daily clone buckets sum to `332`, but the endpoint total is `333`",
            TRAFFIC.render_markdown(report),
        )

    def test_duplicate_clone_timestamp_is_rejected(self):
        payload = clones_payload()
        payload["clones"].append(payload["clones"][0])

        with self.assertRaisesRegex(
            TRAFFIC.TrafficAttributionError, "duplicate timestamp"
        ):
            build(clones_payload=payload)

    def test_invalid_clone_count_is_rejected(self):
        payload = clones_payload()
        payload["clones"][0]["count"] = True

        with self.assertRaisesRegex(
            TRAFFIC.TrafficAttributionError, "must be a non-negative integer"
        ):
            build(clones_payload=payload)

    def test_invalid_clone_timestamp_is_rejected(self):
        payload = clones_payload()
        payload["clones"][0]["timestamp"] = "not-a-timestamp"

        with self.assertRaisesRegex(
            TRAFFIC.TrafficAttributionError, "not a valid ISO-8601 timestamp"
        ):
            build(clones_payload=payload)

    def test_legacy_paths_after_window_receive_a_distinct_status(self):
        daily = [
            {"timestamp": "2026-08-07T00:00:00Z", "count": 1, "uniques": 1},
            {"timestamp": "2026-08-08T00:00:00Z", "count": 0, "uniques": 0},
        ]
        report = build(
            now=utc("2026-08-09T00:00:00Z"),
            views_payload={"count": 1, "uniques": 1, "views": daily},
            paths_payload=[current_paths()[0] | {"count": 1}],
            referrers_payload=[],
        )

        self.assertFalse(report["window"]["windowOverlapsRename"])
        self.assertEqual(report["window"]["renameRelation"], "post-rename")
        self.assertEqual(
            report["interpretation"]["status"], "legacy-paths-after-rename-window"
        )

    def test_pre_rename_window_is_not_described_as_post_rename_legacy_traffic(self):
        daily = [
            {"timestamp": "2026-07-01T00:00:00Z", "count": 1, "uniques": 1},
            {"timestamp": "2026-07-02T00:00:00Z", "count": 0, "uniques": 0},
        ]
        report = build(
            views_payload={"count": 1, "uniques": 1, "views": daily},
            paths_payload=[current_paths()[0] | {"count": 1}],
            referrers_payload=[],
        )

        self.assertEqual(report["window"]["renameRelation"], "pre-rename")
        self.assertEqual(report["interpretation"]["status"], "pre-rename-window")
        self.assertIn("historical repository traffic", TRAFFIC.render_markdown(report))

    def test_complete_canonical_paths_are_attributed_without_inferring_interest(self):
        paths = [
            {
                "path": "/hyeonsangjeon/foundry-stream-lab",
                "title": "Overview",
                "count": 11,
                "uniques": 1,
            }
        ]
        report = build(paths_payload=paths)

        self.assertEqual(report["interpretation"]["status"], "canonical-only")
        self.assertEqual(report["window"]["renameRelation"], "overlaps-rename")
        self.assertTrue(
            report["interpretation"][
                "totalRepositoryViewsFullyAttributedToCanonicalPaths"
            ]
        )
        self.assertFalse(report["interpretation"]["contentInterestInferred"])
        self.assertNotIn("outside the rename", TRAFFIC.render_markdown(report))

    def test_cli_rejects_a_future_rename_timestamp(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(
                TRAFFIC.TrafficAttributionError, "must not be later"
            ):
                TRAFFIC.main(
                    [
                        "--repository",
                        "hyeonsangjeon/foundry-stream-lab",
                        "--legacy-repository",
                        "hyeonsangjeon/kafka-metric-example",
                        "--renamed-at",
                        "2026-07-22T00:00:01Z",
                        "--now",
                        "2026-07-22T00:00:00Z",
                        "--output-json",
                        str(root / "out.json"),
                        "--output-markdown",
                        str(root / "out.md"),
                    ]
                )

    def test_json_and_markdown_rendering_are_deterministic(self):
        report = build()

        self.assertEqual(TRAFFIC.render_json(report), TRAFFIC.render_json(report))
        self.assertEqual(
            TRAFFIC.render_markdown(report), TRAFFIC.render_markdown(report)
        )
        parsed = json.loads(TRAFFIC.render_json(report))
        self.assertEqual(parsed, report)
        markdown = TRAFFIC.render_markdown(report)
        self.assertIn("**rename-window-ambiguous**", markdown)
        self.assertIn("Path-level unique counts overlap", markdown)
        self.assertTrue(markdown.endswith("\n"))

    def test_offline_cli_writes_stable_json_and_markdown(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inputs = {
                "views.json": views_payload(),
                "clones.json": clones_payload(),
                "paths.json": current_paths(),
                "referrers.json": [
                    {"referrer": "github.com", "count": 11, "uniques": 1}
                ],
            }
            for name, payload in inputs.items():
                (root / name).write_text(json.dumps(payload), encoding="utf-8")
            output_json = root / "out" / "traffic.json"
            output_markdown = root / "out" / "traffic.md"
            archive_root = root / "archive"
            arguments = [
                "--repository",
                "hyeonsangjeon/foundry-stream-lab",
                "--legacy-repository",
                "hyeonsangjeon/kafka-metric-example",
                "--renamed-at",
                "2026-07-17T05:39:55Z",
                "--now",
                "2026-07-21T16:12:09Z",
                "--views-file",
                str(root / "views.json"),
                "--clones-file",
                str(root / "clones.json"),
                "--paths-file",
                str(root / "paths.json"),
                "--referrers-file",
                str(root / "referrers.json"),
                "--output-json",
                str(output_json),
                "--output-markdown",
                str(output_markdown),
                "--archive-directory",
                str(archive_root),
            ]

            self.assertEqual(TRAFFIC.main(arguments), 0)
            first_json = output_json.read_bytes()
            first_markdown = output_markdown.read_bytes()
            self.assertEqual(TRAFFIC.main(arguments), 0)
            self.assertEqual(output_json.read_bytes(), first_json)
            self.assertEqual(output_markdown.read_bytes(), first_markdown)
            self.assertEqual(json.loads(first_json)["schemaVersion"], 2)
            archive = archive_root / "2026-07-21T161209Z"
            self.assertEqual((archive / "snapshot.json").read_bytes(), first_json)
            self.assertEqual((archive / "summary.md").read_bytes(), first_markdown)
            expected_checksums = (
                f"{hashlib.sha256(first_json).hexdigest()}  snapshot.json\n"
                f"{hashlib.sha256(first_markdown).hexdigest()}  summary.md\n"
            )
            self.assertEqual(
                (archive / "checksums.sha256").read_text(encoding="utf-8"),
                expected_checksums,
            )

    def test_three_file_offline_mode_remains_compatible_without_clone_data(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inputs = {
                "views.json": views_payload(),
                "paths.json": current_paths(),
                "referrers.json": [
                    {"referrer": "github.com", "count": 11, "uniques": 1}
                ],
            }
            for name, payload in inputs.items():
                (root / name).write_text(json.dumps(payload), encoding="utf-8")
            output_json = root / "traffic.json"
            output_markdown = root / "traffic.md"

            self.assertEqual(
                TRAFFIC.main(
                    [
                        "--repository",
                        "hyeonsangjeon/foundry-stream-lab",
                        "--legacy-repository",
                        "hyeonsangjeon/kafka-metric-example",
                        "--renamed-at",
                        "2026-07-17T05:39:55Z",
                        "--now",
                        "2026-07-21T16:12:09Z",
                        "--views-file",
                        str(root / "views.json"),
                        "--paths-file",
                        str(root / "paths.json"),
                        "--referrers-file",
                        str(root / "referrers.json"),
                        "--output-json",
                        str(output_json),
                        "--output-markdown",
                        str(output_markdown),
                    ]
                ),
                0,
            )

            report = json.loads(output_json.read_text(encoding="utf-8"))
            self.assertFalse(report["sourceAvailability"]["clones"])
            self.assertIsNone(report["metrics"]["total_clones"])
            self.assertIn(
                "Clone input was not supplied",
                output_markdown.read_text(encoding="utf-8"),
            )

    def test_archive_timestamp_is_immutable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = build()
            TRAFFIC._archive_report(
                root,
                report,
                json_content=TRAFFIC.render_json(report),
                markdown_content=TRAFFIC.render_markdown(report),
            )
            changed = build(views_payload=views_payload(count=12))

            with self.assertRaisesRegex(
                TRAFFIC.TrafficAttributionError,
                "archive file already exists with different content",
            ):
                TRAFFIC._archive_report(
                    root,
                    changed,
                    json_content=TRAFFIC.render_json(changed),
                    markdown_content=TRAFFIC.render_markdown(changed),
                )

    def test_concurrent_archive_collision_never_mixes_evidence_sets(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reports = [
                build(),
                build(views_payload=views_payload(count=12)),
            ]

            def publish(report):
                try:
                    return (
                        "ok",
                        TRAFFIC._archive_report(
                            root,
                            report,
                            json_content=TRAFFIC.render_json(report),
                            markdown_content=TRAFFIC.render_markdown(report),
                        ),
                    )
                except TRAFFIC.TrafficAttributionError as exc:
                    return ("error", str(exc))

            with ThreadPoolExecutor(max_workers=2) as executor:
                results = list(executor.map(publish, reports))

            self.assertEqual([status for status, _ in results].count("ok"), 1)
            self.assertEqual([status for status, _ in results].count("error"), 1)
            destination = root / "2026-07-21T161209Z"
            archived_pair = (
                (destination / "snapshot.json").read_text(encoding="utf-8"),
                (destination / "summary.md").read_text(encoding="utf-8"),
            )
            expected_pairs = {
                (TRAFFIC.render_json(report), TRAFFIC.render_markdown(report))
                for report in reports
            }
            self.assertIn(archived_pair, expected_pairs)
            checksums = (destination / "checksums.sha256").read_text(encoding="utf-8")
            self.assertEqual(
                checksums,
                f"{hashlib.sha256(archived_pair[0].encode()).hexdigest()}  snapshot.json\n"
                f"{hashlib.sha256(archived_pair[1].encode()).hexdigest()}  summary.md\n",
            )
            self.assertEqual(
                [path for path in root.iterdir() if path.name.startswith(".")], []
            )

    def test_live_collection_uses_four_versioned_gh_api_requests(self):
        responses = [
            subprocess.CompletedProcess([], 0, json.dumps(views_payload()), ""),
            subprocess.CompletedProcess([], 0, json.dumps(clones_payload()), ""),
            subprocess.CompletedProcess([], 0, json.dumps(current_paths()), ""),
            subprocess.CompletedProcess(
                [],
                0,
                json.dumps([{"referrer": "github.com", "count": 11, "uniques": 1}]),
                "",
            ),
        ]
        with mock.patch.object(TRAFFIC.subprocess, "run", side_effect=responses) as run:
            views, clones, paths, referrers = TRAFFIC.collect_live(
                "hyeonsangjeon/foundry-stream-lab"
            )

        self.assertEqual(views["count"], 11)
        self.assertEqual(clones["count"], 332)
        self.assertEqual(len(paths), 5)
        self.assertEqual(referrers[0]["referrer"], "github.com")
        self.assertEqual(run.call_count, 4)
        commands = [call.args[0] for call in run.call_args_list]
        self.assertTrue(commands[0][-1].endswith("/traffic/views"))
        self.assertTrue(commands[1][-1].endswith("/traffic/clones"))
        self.assertTrue(commands[2][-1].endswith("/traffic/popular/paths"))
        self.assertTrue(commands[3][-1].endswith("/traffic/popular/referrers"))
        self.assertIn(f"X-GitHub-Api-Version: {TRAFFIC.API_VERSION}", commands[0])

    def test_partial_offline_input_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "views.json").write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(
                TRAFFIC.TrafficAttributionError, "must be provided together"
            ):
                TRAFFIC.main(
                    [
                        "--repository",
                        "hyeonsangjeon/foundry-stream-lab",
                        "--legacy-repository",
                        "hyeonsangjeon/kafka-metric-example",
                        "--renamed-at",
                        "2026-07-17T05:39:55Z",
                        "--views-file",
                        str(root / "views.json"),
                        "--output-json",
                        str(root / "out.json"),
                        "--output-markdown",
                        str(root / "out.md"),
                    ]
                )

    def test_clones_only_offline_input_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "clones.json").write_text(
                json.dumps(clones_payload()), encoding="utf-8"
            )
            with self.assertRaisesRegex(
                TRAFFIC.TrafficAttributionError, "--clones-file requires"
            ):
                TRAFFIC.main(
                    [
                        "--repository",
                        "hyeonsangjeon/foundry-stream-lab",
                        "--legacy-repository",
                        "hyeonsangjeon/kafka-metric-example",
                        "--renamed-at",
                        "2026-07-17T05:39:55Z",
                        "--clones-file",
                        str(root / "clones.json"),
                        "--output-json",
                        str(root / "out.json"),
                        "--output-markdown",
                        str(root / "out.md"),
                    ]
                )

    def test_output_cannot_overwrite_clone_input(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inputs = {
                "views.json": views_payload(),
                "clones.json": clones_payload(),
                "paths.json": current_paths(),
                "referrers.json": [],
            }
            for name, payload in inputs.items():
                (root / name).write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(
                TRAFFIC.TrafficAttributionError,
                "output paths must not overwrite offline input files",
            ):
                TRAFFIC.main(
                    [
                        "--repository",
                        "hyeonsangjeon/foundry-stream-lab",
                        "--legacy-repository",
                        "hyeonsangjeon/kafka-metric-example",
                        "--renamed-at",
                        "2026-07-17T05:39:55Z",
                        "--views-file",
                        str(root / "views.json"),
                        "--clones-file",
                        str(root / "clones.json"),
                        "--paths-file",
                        str(root / "paths.json"),
                        "--referrers-file",
                        str(root / "referrers.json"),
                        "--output-json",
                        str(root / "clones.json"),
                        "--output-markdown",
                        str(root / "out.md"),
                    ]
                )


if __name__ == "__main__":
    unittest.main()
