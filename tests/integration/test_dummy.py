
from tests.integration.conftest import launch_workers

class TestTheTest:
    def test_worker_launch(self, launch_workers):
        print("inside test worker launch")
        assert True

    # def test_second(self, launch_workers):
    #     print("inside test second")
    #     assert True


# def test_d(celery_app):
#     print(f"inside test d; celery_app: {celery_app}")