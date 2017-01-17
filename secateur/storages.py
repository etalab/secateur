import redis
from nameko.extensions import DependencyProvider


REDIS_URI_KEY = 'REDIS_URI'
REDIS_DEFAULT_URI = 'redis://localhost:6379/0'


class RedisStorage(DependencyProvider):

    def setup(self):
        super(RedisStorage, self).setup()
        redis_uri = self.container.config.get(REDIS_URI_KEY, REDIS_DEFAULT_URI)

        self.database = redis.StrictRedis.from_url(redis_uri,
                                                   decode_responses=True,
                                                   charset='utf-8')

    def get_dependency(self, worker_ctx):
        return self

    def get_status(self, url_hash):
        return self.database.get('status-' + url_hash)

    def set_status(self, url_hash, status, delay=60):  # Seconds.
        key = 'status-' + url_hash
        self.database.expire(key, delay)
        return self.database.set(key, status)
