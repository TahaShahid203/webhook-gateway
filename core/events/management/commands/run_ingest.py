"""Ingest worker: consume the durable ingest queue (RabbitMQ or Redis), store via ingest_message, THEN ack."""
import logging
import os

from django.core.management.base import BaseCommand

from events import brokers


class Command(BaseCommand):
    help = "Run the ingest consumer"

    def add_arguments(self, parser):
        parser.add_argument("--broker", choices=["rabbitmq", "redis"], default="rabbitmq")

    def handle(self, *args, broker, **opts):
        logging.basicConfig(level=logging.INFO)
        if broker == "redis":
            brokers.run_redis(os.environ["REDIS_URL"])
        else:
            brokers.run_rabbit(os.environ["BROKER_URL"])
