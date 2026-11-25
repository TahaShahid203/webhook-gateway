from django.apps import AppConfig


class EventsConfig(AppConfig):
    name = "events"

    def ready(self):
        from django.db.models.signals import post_delete, post_save

        from .models import Source
        from .sources_export import export_sources

        post_save.connect(lambda **kw: export_sources(), sender=Source, weak=False, dispatch_uid="export_on_save")
        post_delete.connect(lambda **kw: export_sources(), sender=Source, weak=False, dispatch_uid="export_on_delete")
