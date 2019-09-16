"""Model."""
import os
from datetime import datetime

from pony import orm

database = orm.Database()


class Job(database.Entity):
    """Job entity."""

    id = orm.PrimaryKey(str, auto=False)
    created_at = orm.Required(datetime)
    published_at = orm.Required(datetime)
    expires_at = orm.Required(datetime)
    description = orm.Required(str)
    title = orm.Required(str)
    city = orm.Required("City")
    company = orm.Required("Company")
    skills = orm.Set("Skill")


class City(database.Entity):
    """City entity."""

    id = orm.PrimaryKey(str, auto=False)
    created_at = orm.Required(datetime)
    name = orm.Required(str)
    jobs = orm.Set(Job)


class Company(database.Entity):
    """Company entity."""

    id = orm.PrimaryKey(str, auto=False)
    created_at = orm.Required(datetime)
    name = orm.Required(str)
    avatar = orm.Required(str)
    jobs = orm.Set(Job)


class Skill(database.Entity):
    """Skill entity."""

    id = orm.PrimaryKey(str, auto=False)
    created_at = orm.Required(datetime)
    name = orm.Required(str)
    jobs = orm.Set(Job)


database.bind(
    provider="postgres",
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
    host=os.environ["DB_HOST"],
    port=int(os.environ["DB_PORT"]),
    database=os.environ["DB_NAME"],
)

database.generate_mapping(create_tables=True)
