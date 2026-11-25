import json
import os

from django.contrib.auth.models import User
from django.core.management.base import BaseCommand

from events.models import Consumer, Source, Subscription


class Command(BaseCommand):
    help = "Create/update Sources, Consumers and Subscriptions from the dev json files (the edge reads sources.json too)."

    def handle(self, *args, **opts):
        # DEV ONLY: a known admin login so the admin works out of the box. Never ship this.
        if not User.objects.filter(username="admin").exists():
            User.objects.create_superuser("admin", "admin@example.com", "admin")
        d = os.path.dirname(os.environ.get("SOURCES_FILE", "/dev/sources.json"))
        for slug, c in json.load(open(os.path.join(d, "sources.json")))["sources"].items():
            Source.objects.update_or_create(slug=slug, defaults={
                "scheme": c["scheme"], "secret": c["secret"], "previous_secret": c.get("previous_secret", ""),
                "rate_per_second": c.get("rate_per_second"), "rate_burst": c.get("rate_burst"),
                "event_id_header": c.get("event_id_header", ""),
                "event_id_json_key": c.get("event_id_json_key", ""),
                "event_type_header": c.get("event_type_header", ""),
                "event_type_json_key": c.get("event_type_json_key", "")})
            self.stdout.write(f"source {slug}")
        path = os.path.join(d, "consumers.json")
        if not os.path.exists(path):
            return
        cfg = json.load(open(path))
        secret = os.environ.get("CONSUMER_SECRET", "consumer-secret")
        for c in cfg["consumers"]:
            Consumer.objects.update_or_create(name=c["name"], defaults={
                "url": c["url"], "secret": secret, "timeout_seconds": c.get("timeout", 10),
                "max_in_flight": c.get("max_in_flight", 4)})
            self.stdout.write(f"consumer {c['name']}")
        for s in cfg["subscriptions"]:
            Subscription.objects.update_or_create(
                source=Source.objects.get(slug=s["source"]), consumer=Consumer.objects.get(name=s["consumer"]),
                defaults={"event_type_pattern": s.get("pattern", "*"), "ordered": s.get("ordered", False),
                          "ordering_key_json_key": s.get("ordering_key", "")})
