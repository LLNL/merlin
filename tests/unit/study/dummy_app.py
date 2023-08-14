from celery import Celery


dummy_app = Celery("dummy_app", broker="redis://localhost:6379/0")
