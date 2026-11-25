from django.urls import path

from . import views

urlpatterns = [
    path("", views.index),
    path("api/fire", views.fire),
    path("api/poll", views.poll),
    path("api/control", views.control),
    path("api/replay_dead", views.replay_dead),
    path("api/chaos", views.chaos),
]
