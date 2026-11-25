from django.contrib import admin
from django.conf import settings
from django.urls import include, path

from events import views

urlpatterns = [
    path("admin/", admin.site.urls),
    path("metrics", views.prometheus),
    path("stats.json", views.stats),
]

if settings.PLAYGROUND:
    urlpatterns.append(path("play/", include("playground.urls")))
