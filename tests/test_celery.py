import re

def test_broker_url():
    """
    Ensure the celery application 'broker_url' roughly matches the required pattern.
    """
    from merlin.celery import app
    assert re.match(r"amqps:\/\/\w+:.+@jackalope\.llnl\.gov:\d+\/\w+", app.conf.broker_url)


def test_result_backend():
    """
    Ensure the celery application 'result_backend' roughly matches the required pattern.
    """
    from merlin.celery import app
    assert re.match(r"redis:\/\/\w+:\w+@jackalope\.llnl\.gov:\d+\/\w+", app.conf.broker_url)

