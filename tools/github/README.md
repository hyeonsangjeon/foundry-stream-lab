# GitHub repository utilities

`traffic_attribution.py` captures four private repository Traffic API surfaces.
Popular paths distinguish canonical, legacy, other, and unattributed page
views, while clones remain a separate repository-level aggregate:

- `/traffic/views`
- `/traffic/clones`
- `/traffic/popular/paths`
- `/traffic/popular/referrers`

It depends only on Python's standard library and the authenticated `gh` CLI.
It never prints or persists an authentication token.

## Live capture

```bash
python3 tools/github/traffic_attribution.py \
  --repository hyeonsangjeon/foundry-stream-lab \
  --legacy-repository hyeonsangjeon/kafka-metric-example \
  --renamed-at 2026-07-17T05:39:55Z \
  --archive-directory tmp/repository-traffic/archive \
  --output-json tmp/repository-traffic/snapshot.json \
  --output-markdown tmp/repository-traffic/summary.md
```

`--repository` is both the API target and canonical path prefix. Repeat
`--legacy-repository` if more than one historical slug must be classified.
`--renamed-at` controls whether the rolling UTC window is marked rename
ambiguous. `--archive-directory` preserves an immutable timestamped copy of the
JSON, Markdown, and SHA-256 checksums while the fixed output paths remain the
latest snapshot.

## Offline analysis

Saved API responses can be analyzed without network access:

```bash
python3 tools/github/traffic_attribution.py \
  --repository hyeonsangjeon/foundry-stream-lab \
  --legacy-repository hyeonsangjeon/kafka-metric-example \
  --renamed-at 2026-07-17T05:39:55Z \
  --views-file /path/to/views.json \
  --clones-file /path/to/clones.json \
  --paths-file /path/to/paths.json \
  --referrers-file /path/to/referrers.json \
  --output-json /tmp/traffic.json \
  --output-markdown /tmp/traffic.md
```

Views, paths, and referrers are required together. `--clones-file` is optional
only for compatibility with snapshots captured before schema version 2; when
omitted, clone fields are `null` and source availability is `false`. JSON output
contains the raw responses, classification rows, metric definitions, window
metadata, and limitations. Markdown is a compact operator summary of the same
snapshot.

GitHub does not return the clone URL used by a client. The report therefore
keeps full-clone totals and unique cloners at repository scope and never
allocates them using canonical/legacy popular-path shares.

## Tests

```bash
python3 -m unittest discover -s tools/github/tests -p 'test_*.py'
```
