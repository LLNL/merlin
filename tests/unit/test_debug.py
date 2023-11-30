from typing import Dict
from time import sleep

from celery import Celery
from celery.canvas import Signature


class TestOne:

    def test_one(self, celery_app: Celery, launch_workers: "Fixture", sleep_sig: Signature):  # noqa: F821
        # queue_for_signature = "test_queue_0"
        # sleep_sig.set(queue=queue_for_signature)
        # result = sleep_sig.delay()

        # sleep(1)

        # print(f"active: {celery_app.control.inspect().active()}")
        # result.get()
        pass
        

class TestTwo:

    def test_one(self, launch_workers: "Fixture", worker_queue_map: Dict[str, str]):  # noqa: F821
        pass