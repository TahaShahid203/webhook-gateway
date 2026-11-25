import os

SECRET_KEY = os.getenv("DJANGO_SECRET_KEY", "dev-only-not-a-secret")
DEBUG = os.getenv("DEBUG", "0") == "1"
ALLOWED_HOSTS = ["*"]
INSTALLED_APPS = [
    "django.contrib.admin", "django.contrib.auth", "django.contrib.contenttypes",
    "django.contrib.sessions", "django.contrib.messages", "django.contrib.staticfiles", "events.apps.EventsConfig", "playground",
]
MIDDLEWARE = [
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
]
ROOT_URLCONF = "gateway.urls"
TEMPLATES = [{"BACKEND": "django.template.backends.django.DjangoTemplates", "APP_DIRS": True,
              "OPTIONS": {"context_processors": [
                  "django.template.context_processors.request",
                  "django.contrib.auth.context_processors.auth",
                  "django.contrib.messages.context_processors.messages"]}}]
DATABASES = {"default": {
    "ENGINE": "django.db.backends.postgresql",
    "NAME": os.getenv("POSTGRES_DB", "gateway"),
    "USER": os.getenv("POSTGRES_USER", "gateway"),
    "PASSWORD": os.getenv("POSTGRES_PASSWORD", "gateway"),
    "HOST": os.getenv("POSTGRES_HOST", "db"),
    "PORT": os.getenv("POSTGRES_PORT", "5432"),
}}
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
USE_TZ = True
TIME_ZONE = "UTC"
STATIC_URL = "static/"
STATIC_ROOT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "staticfiles")
CSRF_TRUSTED_ORIGINS = ["http://localhost:8080", "http://127.0.0.1:8080"]
# a hash-only duplicate (provider sent no event id) only counts within this window (Taha, Nov 23)
HASH_DEDUP_WINDOW_SECONDS = 24 * 3600

# Celery executes deliveries; the database (Delivery rows) is the source of truth.
CELERY_BROKER_URL = os.getenv("BROKER_URL", "amqp://guest:guest@broker:5672/")
CELERY_TASK_DEFAULT_QUEUE = "deliver"
CELERY_WORKER_PREFETCH_MULTIPLIER = 1
CELERY_TASK_ACKS_LATE = True
CELERY_TASK_REJECT_ON_WORKER_LOST = True
CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP = True
# the dispatcher re-sends a still-pending due delivery to the queue after this many seconds
ENQUEUE_TTL_SECONDS = float(os.getenv("ENQUEUE_TTL_SECONDS", "2"))
# an in_flight delivery older than consumer timeout + this grace is considered orphaned by a dead worker
STUCK_GRACE_SECONDS = float(os.getenv("DELIVERY_STUCK_GRACE", "30"))

# Tier 3: alerts and retention
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "")
ALERT_DEAD_THRESHOLD = int(os.getenv("ALERT_DEAD_THRESHOLD", "5"))        # dead deliveries per consumer
ALERT_QUEUE_AGE_SECONDS = float(os.getenv("ALERT_QUEUE_AGE_SECONDS", "300"))
ALERT_CHECK_EVERY_SECONDS = float(os.getenv("ALERT_CHECK_EVERY_SECONDS", "30"))
# PROPOSAL (Taha to confirm): raw bodies live 30 days; ids, hashes, statuses and attempt logs are kept
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "30"))

# where the edge reads its sources from (a file on a volume shared with the edge); empty = don't export
EDGE_SOURCES_FILE = os.getenv("EDGE_SOURCES_FILE", "")

# The playground front end (dev only: it signs webhooks with the dev secrets in the Source table)
PLAYGROUND = os.getenv("PLAYGROUND", "0") == "1"
EDGE_URL = os.getenv("EDGE_URL", "http://edge:8000")
FAKE_CONSUMER_URL = os.getenv("FAKE_CONSUMER_URL", "http://fake-consumer:9000")
RABBIT_API = os.getenv("RABBIT_API", "http://broker:15672")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
REDIS_VOLATILE_URL = os.getenv("REDIS_VOLATILE_URL", "redis://redis-volatile:6379/0")
# "crash the queue" really kills a broker container, so it is opt-in and needs the Docker socket (docker-compose.chaos.yml)
PLAYGROUND_CHAOS = os.getenv("PLAYGROUND_CHAOS", "0") == "1"
