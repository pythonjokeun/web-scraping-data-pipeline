"""config."""

from huey import RedisHuey
import os

huey = RedisHuey("scraper", host=os.environ["REDIS_URL"], port=os.environ["REDIS_PORT"])
