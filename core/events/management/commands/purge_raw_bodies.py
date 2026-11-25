from django.conf import settings
from django.core.management.base import BaseCommand

from events.retention import purge_raw_bodies


class Command(BaseCommand):
    help = "Delete raw bodies older than N days (metadata is kept). Dry run unless --yes."

    def add_arguments(self, parser):
        parser.add_argument("--days", type=int, default=settings.RETENTION_DAYS)
        parser.add_argument("--yes", action="store_true")
        parser.add_argument("--actor", default="system")

    def handle(self, *args, days, yes, actor, **opts):
        n = purge_raw_bodies(days, actor, dry_run=not yes)
        self.stdout.write(f"{'purged' if yes else 'would purge'} {n} raw bodies older than {days} days")
