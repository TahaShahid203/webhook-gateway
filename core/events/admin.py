import csv

from django.contrib import admin, messages
from django.http import HttpResponse
from django.template.response import TemplateResponse
from django.utils.html import format_html

from .corrections import add_correction
from .explain import explain_event
from .models import (Alert, AuditLog, Consumer, DedupKey, Delivery, DeliveryAttempt, Event, EventConflict,
                     EventCorrection, Source, Subscription)
from .replay import (DEFAULT_RATE_PER_SECOND, REPLAYABLE, ReplayRefused, BulkPlan, execute_bulk_replay, skip_delivery)


def csv_response(name, header, rows):
    resp = HttpResponse(content_type="text/csv")
    resp["Content-Disposition"] = f'attachment; filename="{name}.csv"'
    w = csv.writer(resp)
    w.writerow(header)
    w.writerows(rows)
    return resp


class ReadOnly:
    """The raw layer and the logs are never edited or deleted through the admin."""
    def has_add_permission(self, request, obj=None):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


class DeliveryAttemptInline(ReadOnly, admin.TabularInline):
    model = DeliveryAttempt
    extra = 0
    fields = ("replay_round", "number", "started_at", "finished_at", "duration", "status_code", "error", "response_body")
    readonly_fields = fields

    @admin.display(description="took")
    def duration(self, obj):
        return f"{(obj.finished_at - obj.started_at).total_seconds() * 1000:.0f} ms" if obj.finished_at else "no result"


class DeliveryInline(ReadOnly, admin.TabularInline):
    model = Delivery
    extra = 0
    fields = ("link", "consumer", "status", "attempts", "replayed", "replay_count", "next_attempt_at", "delivered_at")
    readonly_fields = fields

    @admin.display(description="delivery")
    def link(self, obj):
        return format_html('<a href="/admin/events/delivery/{}/change/">#{}</a>', obj.pk, obj.pk)


@admin.register(Event)
class EventAdmin(ReadOnly, admin.ModelAdmin):
    list_display = ("short_id", "source", "event_type", "provider_event_id", "status", "duplicate_count", "received_at")
    list_filter = ("source", "status", "event_type")
    date_hierarchy = "received_at"
    search_fields = ("provider_event_id", "id", "body_sha256")
    inlines = [DeliveryInline]
    fields = ("id", "source", "event_type", "provider_event_id", "status", "received_at", "duplicate_count",
              "last_seen_at", "body_sha256", "headers", "raw_purged_at", "story", "raw_body_text")
    readonly_fields = fields
    actions = ["export_csv"]

    @admin.action(description="Export selected events to CSV (no raw bodies)")
    def export_csv(self, request, queryset):
        return csv_response("events", ["id", "source", "event_type", "provider_event_id", "status", "received_at",
                                       "duplicate_count", "body_sha256"],
                            [(e.id, e.source.slug, e.event_type, e.provider_event_id, e.status, e.received_at,
                              e.duplicate_count, e.body_sha256) for e in queryset.select_related("source")])

    @admin.display(description="what happened, in plain words")
    def story(self, obj):
        return format_html("<ol style='margin:0;padding-left:1.2em'>{}</ol>",
                           format_html("".join(f"<li>{{}}</li>" for _ in explain_event(obj)), *explain_event(obj)))

    @admin.display(description="event")
    def short_id(self, obj):
        return str(obj.id)[:8]

    @admin.display(description="raw body (exactly as received)")
    def raw_body_text(self, obj):
        if obj.raw_purged_at:
            return f"(purged by retention on {obj.raw_purged_at:%Y-%m-%d}; metadata kept)"
        return format_html("<pre style='white-space:pre-wrap'>{}</pre>", bytes(obj.raw_body).decode(errors="replace")[:20000])


@admin.register(Delivery)
class DeliveryAdmin(admin.ModelAdmin):
    list_display = ("id", "event_link", "consumer", "status", "attempts", "replayed", "ordering_key", "created_at",
                    "delivered_at")
    list_filter = ("status", "consumer", "replayed", "event__source")
    date_hierarchy = "created_at"
    search_fields = ("event__provider_event_id", "event__id", "ordering_key")
    inlines = [DeliveryAttemptInline]
    actions = ["replay_selected", "skip_selected", "export_csv"]
    readonly_fields = [f.name for f in Delivery._meta.fields]

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    @admin.action(description="Export selected deliveries to CSV (dead-letter list, etc.)")
    def export_csv(self, request, queryset):
        return csv_response("deliveries", ["id", "event", "consumer", "status", "attempts", "replayed", "ordering_key",
                                           "created_at", "delivered_at"],
                            [(d.id, d.event_id, d.consumer.name, d.status, d.attempts, d.replayed, d.ordering_key,
                              d.created_at, d.delivered_at) for d in queryset.select_related("consumer")])

    @admin.display(description="event")
    def event_link(self, obj):
        return format_html('<a href="/admin/events/event/{}/change/">{}</a>', obj.event_id, str(obj.event_id)[:8])

    @admin.action(description="Replay selected deliveries (dry run first, rate limited)")
    def replay_selected(self, request, queryset):
        ids = list(queryset.filter(status__in=REPLAYABLE, event__raw_purged_at__isnull=True)
                   .order_by("id").values_list("id", flat=True))
        rate = float(request.POST.get("rate") or DEFAULT_RATE_PER_SECOND)
        if request.POST.get("confirm") == "yes":
            n = execute_bulk_replay(BulkPlan(len(ids), ids, rate), request.user.get_username(),
                                    filters_text="admin selection")
            self.message_user(request, f"Replayed {n} deliveries at {rate}/s.", messages.SUCCESS)
            return None
        return TemplateResponse(request, "admin/events/replay_confirm.html", {
            **self.admin_site.each_context(request), "ids": ids, "count": len(ids), "rate": rate,
            "skipped": queryset.count() - len(ids), "seconds": round(len(ids) / rate),
            "actor": request.user.get_username(), "back": request.path})

    @admin.action(description="Skip selected dead/failed deliveries (releases an ordered key)")
    def skip_selected(self, request, queryset):
        n = 0
        for d in queryset:
            try:
                skip_delivery(d.pk, request.user.get_username(), reason="admin")
                n += 1
            except ReplayRefused:
                continue
        self.message_user(request, f"Skipped {n} deliveries.", messages.SUCCESS)


@admin.register(AuditLog)
class AuditLogAdmin(ReadOnly, admin.ModelAdmin):
    list_display = ("created_at", "actor", "action", "object_type", "object_id", "old_value", "new_value")
    list_filter = ("action", "actor")
    date_hierarchy = "created_at"
    search_fields = ("object_id", "actor")


@admin.register(Alert)
class AlertAdmin(admin.ModelAdmin):
    list_display = ("created_at", "kind", "detail", "resolved", "notified_at")
    list_filter = ("kind", "resolved")
    readonly_fields = ("kind", "detail", "created_at", "notified_at")


@admin.register(Consumer)
class ConsumerAdmin(admin.ModelAdmin):
    list_display = ("name", "url", "active", "breaker_state", "max_in_flight", "timeout_seconds")
    exclude = ("secret",)


@admin.register(EventCorrection)
class EventCorrectionAdmin(admin.ModelAdmin):
    """Fix a held event WITHOUT touching its raw body: the patch is stored here and merged in at delivery time."""
    list_display = ("created_at", "event", "patch", "reason", "actor")
    fields = ("event", "patch", "reason")
    autocomplete_fields = ()
    raw_id_fields = ("event",)

    def has_change_permission(self, request, obj=None):
        return obj is None

    def has_delete_permission(self, request, obj=None):
        return False

    def save_model(self, request, obj, form, change):
        add_correction(obj.event, obj.patch, request.user.get_username(), obj.reason)


for m in (Source, DedupKey, EventConflict, Subscription):
    admin.site.register(m)
