"""The edge reads its sources (slug, scheme, secrets, rate limit) from a JSON file so it never needs a database.
The database is the source of truth: every change to a Source rewrites that file atomically, so adding a source or
rotating a secret in the admin reaches the edge within about a second. No restart."""
import json
import os
import tempfile

from django.conf import settings


def export_sources() -> None:
    path = settings.EDGE_SOURCES_FILE
    if not path:
        return
    from .models import Source
    data = {"sources": {s.slug: {
        "scheme": s.scheme, "secret": s.secret, "previous_secret": s.previous_secret,
        "rate_per_second": s.rate_per_second, "rate_burst": s.rate_burst,
    } for s in Source.objects.filter(active=True)}}
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".tmp")
    with os.fdopen(fd, "w") as f:
        json.dump(data, f)
    os.chmod(tmp, 0o644)
    os.replace(tmp, path)          # atomic: the edge never reads a half-written file
