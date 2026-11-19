# Abstraction-police routine

Weekly **Cursor Cloud Agent** automation (not a GitHub Actions workflow). Each run
opens a branch `routine/abstraction-police-YYYY-MM-DD` and a pull request with one
small, behavior-preserving deduplication when the agent finds byte-identical helpers.

## PR dedupe (required before opening a PR)

Before creating a branch or pull request for a refactor that matches a known
fingerprint, run:

```bash
GITHUB_REPOSITORY=Actalux/actalux GITHUB_TOKEN="$GITHUB_TOKEN" \
  uv run python scripts/routine/find_open_duplicate_pr.py --fingerprint dese-ssrs-money-cell
```

- **Exit 0** — an open PR already covers this change; do not open another. Comment
  on or update the existing PR if needed.
- **Exit 1** — no duplicate; safe to open a new PR.

Fingerprints live in `scripts/routine/fingerprints.json`. Add a new entry when a
recurring refactor is identified so later runs do not spam duplicate PRs.