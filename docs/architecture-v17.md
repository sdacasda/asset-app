# v17 Duplicate Merge and Risk Priority

## Goal

Under the same data-source label, the same address should appear only once, even if it was collected from an old URL and a new URL.

## Rules

1. Deduplicate by `source_id + content_text`.
2. If duplicate statuses conflict, risk/danger wins over pure/safe.
3. If priorities are the same, keep the newest `last_checked` record.
4. Duplicate loser records are soft-deleted, not permanently removed.
5. API/export also perform display-level dedupe as a safety net.

## Why

When a source URL changes, old results may remain under the old URL while new results arrive under the new URL. Without dedupe, the same address can appear twice under the same label, one safe and one risk. v17 makes the risk state authoritative.
