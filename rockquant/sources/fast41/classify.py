from __future__ import annotations

def classify_fast41_subtype(title: str) -> str:
    t = (title or "").strip().lower()

    # Common patterns in permitting.gov press releases
    if "fast-41" in t and ("adds" in t or "added" in t or "latest to gain" in t or "gain fast-41" in t):
        return "FAST41_ADDED"

    if "milestone" in t:
        return "MILESTONE_UPDATE"

    if "significant progress" in t or "progress achieved" in t:
        return "PROGRESS_UPDATE"

    if "federal permitting approval" in t or "completes federal permitting" in t:
        return "PERMITTING_APPROVAL"

    if "executive director tours" in t or "tours" in t:
        return "SITE_VISIT"

    return "FAST41_NEWS"
