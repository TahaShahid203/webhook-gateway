from django.http import HttpResponse, JsonResponse

from . import metrics


def prometheus(request):
    return HttpResponse(metrics.prometheus_text(metrics.snapshot()), content_type="text/plain; version=0.0.4")


def stats(request):
    return JsonResponse(metrics.snapshot())
