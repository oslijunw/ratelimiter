from ratelimiter.ratelimiter import RateLimiter


def test_limit():
    import time
    import uuid
    rate_limiter = RateLimiter(max_workers=6)

    def restricted_task():
        time.sleep(5)
        return 'restrict work done'

    def unlimited_tasks(i):
        _i = i
        return 'unlimited work done'

    rate_limiter.register_semaphore(restricted_task, 2)

    for _i in range(10):
        rate_limiter.submit(str(uuid.uuid4()), restricted_task)
        # rate_limiter.submit(str(uuid.uuid4()), unlimited_tasks, i=_i)
    time.sleep(100)


test_limit()
