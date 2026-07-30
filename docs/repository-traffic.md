# Repository traffic attribution

GitHub repository traffic is a short operational signal, not a product
analytics system. This repository was renamed from
`hyeonsangjeon/kafka-metric-example` to
`hyeonsangjeon/foundry-stream-lab` on 2026-07-17. GitHub redirects the old web
paths, while the [Traffic API](https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28)
reports a rolling 14-day window and only the top 10 popular paths. A snapshot
taken near the rename can therefore contain both slugs without representing
traffic from two independent repositories. See GitHub's
[repository rename behavior](https://docs.github.com/en/repositories/creating-and-managing-repositories/renaming-a-repository)
for the redirect contract.

Use [`tools/github/traffic_attribution.py`](../tools/github/traffic_attribution.py)
to preserve the raw API response and classify each returned path. The tool does
not modify repository settings, commit generated data, or sum unique visitors
across paths.

## Metric contract

| Metric | Meaning |
| --- | --- |
| `total_views` | Complete view count returned by `/traffic/views` for GitHub's rolling window. |
| `unique_visitors` | Window-level unique count from `/traffic/views`; this is the authoritative unique metric. |
| `canonical_known_top_path_views` | Views among returned top paths whose exact repository prefix is `/hyeonsangjeon/foundry-stream-lab`. |
| `legacy_known_top_path_views` | Views among returned top paths whose exact repository prefix is `/hyeonsangjeon/kafka-metric-example`. |
| `other_known_top_path_views` | Views in returned top paths that belong to neither configured prefix. |
| `not_in_top_paths_views` | `total_views` minus the sum of returned top paths; these views cannot be attributed by this API. |
| `top_path_coverage` | Returned top-path views divided by `total_views`; `1.0` means the returned rows cover this snapshot, not that the endpoint is generally complete. |
| `total_clones` | Complete full-clone count returned by `/traffic/clones` for GitHub's rolling 14-day window; fetches are excluded. |
| `unique_cloners` | Window-level unique count from `/traffic/clones`; daily unique-cloner values must not be added. |

The `*_known_top_path_views` names are deliberate. Popular paths is capped at
10 rows, so canonical and legacy counts can be lower bounds. Only when
`not_in_top_paths_views` is zero does the path split cover the complete total
for that snapshot.

Path-level `uniques` are retained only as raw row metadata. The same visitor can
appear on several paths, so those values must never be added together.

Clone totals have no equivalent path attribution. The clones endpoint returns
only repository-level totals and UTC daily buckets; it does not expose whether
the requested remote used the canonical or legacy repository slug. Never apply
the popular-path 9:3 view ratio to clones.

## Rename transition baseline

The snapshot captured at `2026-07-21T16:12:09Z` returned:

- 11 total views from one unique visitor;
- 9 legacy known-top-path views and 2 canonical known-top-path views;
- zero views outside the five returned popular paths;
- 10 views on 2026-07-17 UTC and 1 view on 2026-07-19 UTC;
- `github.com` as the referrer for all 11 views.

This does not support interpreting “11 views” as 11 users showing interest in
the rebuilt content. It is a rename-attribution snapshot from one unique
visitor. GitHub does not expose a path-by-date cross-tab, so the 2026-07-17
views cannot be divided into pre-rename and post-rename requests.

The current README, workflows, container references, OCI source label, and Git
remote use the canonical name. Remaining old-name strings are an intentional
cleanup ownership sentinel or immutable historical evidence. Do not rewrite
those records to make a rolling traffic report look cleaner.

The audit also issued HTTP `HEAD` requests to old URLs after capturing the
snapshot. GitHub does not document whether those probes count as views. Use a
snapshot taken on or after 2026-08-06 KST as the first clean post-audit check.

## Operational checkpoint: 2026-07-30

The incoming high-priority signal reported 7-day views of 12, 14-day clones of
331, 9 legacy-path views, 3 canonical Overview views with 1 path unique, and
`github.com` as the 12-view referrer. That signal did not include a capture
timestamp or the source of its 7-day view rollup. Preserve it as an upstream
observation rather than joining it directly to the 14-day popular-path and
referrer tables.

A live API capture at `2026-07-30T14:46:05Z` verified the following native
rolling 14-day snapshot for `2026-07-16` through `2026-07-29` UTC:

| Signal | Count | Unique value and scope | Slug attribution |
| --- | ---: | ---: | --- |
| Repository views | 12 | 1 window-level visitor | canonical 3 · legacy 9 · other 0 · unattributed 0 |
| Canonical Overview popular path | 3 | 1 path unique; non-additive | canonical |
| Full clones | 332 | 109 window-level cloners | unavailable |
| `github.com` referrer | 12 | 1 referrer unique | cannot be joined to a path or clone |

The one-clone difference from the incoming 331 is consistent with rolling-window
snapshot drift; the verified response includes one clone in the 2026-07-29 UTC
bucket. Capture time and window must accompany every reported total.

The ignored private evidence is archived at
`tmp/repository-traffic/archive/2026-07-30T144605Z/`. Its SHA-256 checksums are:

- `snapshot.json`:
  `b49ee648ab4ba4537cad7e0cd6a8c5d5094831064875ee5d47b0c4e24749faa8`
- `summary.md`:
  `7012bfd2b4e10acae78974fdce60cd3342e9ee633e2c65ca5e2f52b8ef89f69a`

Repository identity resolution and traffic labels are separate concerns:

- Repository API lookups for both slugs resolved to repository ID `176433281`
  with canonical `full_name` `hyeonsangjeon/foundry-stream-lab`.
- `git ls-remote` against both old and new clone URLs returned the same HEAD,
  `248d943b7d6029d4c5b3a8a25916c6d24e8adb5f`.
- The canonical Traffic API still returned 9 views labelled with legacy paths
  and 3 views labelled with the canonical path.
- The clone response contains no requested URL or slug, so its 332 full clones
  cannot be divided into legacy and canonical clone demand.

The operating status therefore remains `rename-window-ambiguous`. Keep this
high item open until the clean snapshot on or after 2026-08-06 KST confirms
whether legacy path labels persist outside the audit and rename windows.

## Local capture

The live path uses the authenticated GitHub CLI. It requires repository
administration read access because that is the permission required by the
Traffic API.

```bash
gh auth status
make repository-traffic
```

The command writes ignored latest outputs to:

- `tmp/repository-traffic/snapshot.json`
- `tmp/repository-traffic/summary.md`

It also writes an immutable timestamped private evidence set to
`tmp/repository-traffic/archive/<UTC timestamp>/`, containing `snapshot.json`,
`summary.md`, and `checksums.sha256`. Repeating the same timestamp with
different content fails instead of overwriting evidence.

Run only the transformation tests with:

```bash
make repository-traffic-test
```

For a fully offline reproduction, also pass `--clones-file` with
`--views-file`, `--paths-file`, and `--referrers-file`. Existing three-file
offline inputs remain supported, but clone fields are explicitly marked
unavailable. See
[`tools/github/README.md`](../tools/github/README.md) for the exact interface.

## Automation boundary

This public repository intentionally does not run the capture in GitHub
Actions. The workflow `GITHUB_TOKEN` cannot receive the Traffic API's
`Administration: read` permission, and supplying a separate token would make
the otherwise administrator-only raw paths and referrers visible through a
public run summary or artifact.

Keep live snapshots local and ignored. If long-term automation becomes
necessary, send the result to a separately reviewed private store using a
GitHub App or fine-grained token limited to this repository and
`Administration: Read-only`. Do not commit snapshots to `master`, copy a broad
local OAuth token into Actions, or publish raw referrers merely to retain a
history.

## Interpretation rules

- Report total, canonical-known, legacy-known, other-known, and unattributed
  views separately.
- Exclude rename-ambiguous totals from claims about interest in the canonical
  content.
- Treat a continuing legacy count after the clean-check date as
  `legacy-labelled / mapping-unverified`. Inspect releases and external
  backlinks without claiming redirect demand or changing historical evidence.
- Treat canonical-known views as confirmed canonical-path traffic, not as a
  user count and not automatically as positive engagement.
- Report clone totals as repository-level full clones only. Do not infer
  canonical-name demand or distribute clones using popular-path shares.
- Keep the raw snapshot with every published interpretation so the top-path
  coverage, clone availability, capture time, and unique denominators remain
  auditable.
