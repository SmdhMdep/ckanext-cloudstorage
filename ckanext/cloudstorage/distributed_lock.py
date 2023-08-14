from contextlib import contextmanager
import logging

from ckan.lib.redis import connect_to_redis, is_redis_available
from redis.exceptions import LockError as RedisLockError, LockNotOwnedError as RedisLockNotOwnedError


logger = logging.getLogger(__name__)


class LockError(Exception): pass


@contextmanager
def distributed_lock(name: str, blocking_timeout=1, timeout=2):
    if not is_redis_available():
        raise LockError("redis is required to acquire a distributed lock")

    try:
        lock_name = f"cloudstorage:lock-{name}"
        logger.debug("acquiring lock %s", lock_name)
        with connect_to_redis().lock(lock_name, blocking_timeout=blocking_timeout, timeout=timeout) as lock:
            try:
                logger.debug("acquired lock %s", lock_name)
                yield lock
            finally:
                logger.debug("releasing lock %s", lock_name)
    except (RedisLockError, RedisLockNotOwnedError) as e:
        raise LockError(f"unable to acquire lock") from e
    finally:
        logger.debug("released lock %s", lock_name)
