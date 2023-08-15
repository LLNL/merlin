from celery import Celery


PORT = 6380

dummy_app = Celery("dummy_app", broker=f"redis://localhost:{PORT}/0", result_backend=f"redis://localhost:{PORT}/1")
