from datetime import datetime, timezone

from django.core.management.base import BaseCommand, CommandError

from events.replay import (DEFAULT_RATE_PER_SECOND, ReplayRefused, execute_bulk_replay, plan_bulk_replay,
                           replay_delivery, skip_delivery)


class Command(BaseCommand):
    help = ("Replay deliveries. One: --delivery ID. Many: filters (--consumer, --status, --source, --since, --until). "
            "Bulk replay is a DRY RUN that prints the count unless --yes is given, and is rate limited (--rate/s).")

    def add_arguments(self, p):
        p.add_argument("--actor", required=True, help="who is doing this (recorded in the audit log)")
        p.add_argument("--delivery", type=int)
        p.add_argument("--skip", action="store_true", help="skip --delivery instead of replaying it")
        p.add_argument("--consumer")
        p.add_argument("--status", choices=["dead", "failed", "delivered", "skipped"])
        p.add_argument("--source")
        p.add_argument("--since", help="ISO date/time")
        p.add_argument("--until", help="ISO date/time")
        p.add_argument("--rate", type=float, default=DEFAULT_RATE_PER_SECOND, help="deliveries per second")
        p.add_argument("--yes", action="store_true", help="actually do a bulk replay")

    def handle(self, *a, **o):
        try:
            if o["delivery"]:
                fn = skip_delivery if o["skip"] else replay_delivery
                d = fn(o["delivery"], o["actor"])
                self.stdout.write(f"delivery {d.pk}: {d.status}")
                return
            parse = lambda v: datetime.fromisoformat(v).replace(tzinfo=timezone.utc) if v else None
            filters = dict(consumer=o["consumer"], status=o["status"], source=o["source"],
                           since=parse(o["since"]), until=parse(o["until"]))
            if not any(filters.values()):
                raise CommandError("refusing a bulk replay with no filter")
            plan = plan_bulk_replay(rate_per_second=o["rate"], **filters)
            self.stdout.write(f"DRY RUN: {plan.count} deliveries match; at {plan.rate_per_second}/s "
                              f"this takes {plan.duration_seconds:.0f}s")
            if o["yes"]:
                n = execute_bulk_replay(plan, o["actor"], filters_text=str({k: str(v) for k, v in filters.items() if v}))
                self.stdout.write(f"replayed {n}")
            else:
                self.stdout.write("nothing changed; add --yes to replay")
        except ReplayRefused as e:
            raise CommandError(str(e))
