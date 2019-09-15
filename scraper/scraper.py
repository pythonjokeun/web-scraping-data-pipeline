"""Scraper module."""

import json
import os
from datetime import datetime

import fire
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from logzero import logger
from pony import orm

db = orm.Database()


def fetch(filter_object_id=[]):
    """Fetch raw data."""

    data = {
        "requests": [
            {
                "indexName": "job_postings",
                "params": f'query=&hitsPerPage=1000&facetFilters=[["city.work_country_name:Indonesia"]]',
            }
        ]
    }

    if not filter_object_id:
        pass
    else:
        # Construct filter to exclude passed objectID
        filter = f"&filters=NOT objectID:{filter_object_id[0]}"

        for x in filter_object_id[1:]:
            filter += f" AND NOT objectID:{x}"

        data["requests"][0]["params"] += filter

    req = requests.post(os.environ["REQUEST_URL"], data=json.dumps(data))
    res = req.json()["results"][0]["hits"]

    return res


class Job(db.Entity):
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


class City(db.Entity):
    """City entity."""

    id = orm.PrimaryKey(str, auto=False)
    created_at = orm.Required(datetime)
    name = orm.Required(str)
    jobs = orm.Set(Job)


class Company(db.Entity):
    """Company entity."""

    id = orm.PrimaryKey(str, auto=False)
    created_at = orm.Required(datetime)
    name = orm.Required(str)
    avatar = orm.Required(str)
    jobs = orm.Set(Job)


class Skill(db.Entity):
    """Skill entity."""

    id = orm.PrimaryKey(str, auto=False)
    created_at = orm.Required(datetime)
    name = orm.Required(str)
    jobs = orm.Set(Job)


db.bind(
    provider="postgres",
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
    host=os.environ["DB_HOST"],
    database=os.environ["DB_NAME"],
)

db.generate_mapping(create_tables=True)


@orm.db_session
def ingest(data):
    """Save to database."""

    city = City.get(id=data["city"]["id"])

    if city is None:
        city = City(
            id=data["city"]["id"],
            name=data["city"]["name"],
            created_at=datetime.utcnow(),
        )

    company = Company.get(id=data["company"]["id"])

    if company is None:
        company = Company(
            id=data["company"]["id"],
            avatar=data["company"]["avatar"],
            name=data["company"]["name"],
            created_at=datetime.utcnow(),
        )

    skills = []
    for y in data["job_skills"]:
        skill = Skill.get(id=y["id"])

        if skill is None:
            skill = Skill(id=y["id"], name=y["name"], created_at=datetime.utcnow())

        skills.append(skill)

    Job(
        id=data["objectID"],
        created_at=datetime.utcnow(),
        published_at=data["published_at"],
        expires_at=data["expires_at"],
        description=data["description"],
        title=data["title"],
        city=city,
        company=company,
        skills=skills,
    )

    return True


@orm.db_session
def get_job_ids():
    """Retrieve existing job ids from database."""

    return list(orm.select(j.id for j in Job)[:])


def main(cron="*/5 * * * *"):
    """Run program."""

    sched = BlockingScheduler()

    def run():
        exclude = get_job_ids()
        result = []
        count = 0
        while True:
            temp = fetch(exclude)
            if not temp:
                break
            else:
                result += temp
                exclude += [x["objectID"] for x in temp]
                count += 1

        logger.info(f"Got {len(result)} new data.")

        for x in result:
            logger.info("Processing objectID: " + x["objectID"])
            ingest(x)

    # Run at start
    run()

    # Run scheduled
    logger.info(f"Schedule to run with crontab {cron}")
    sched.add_job(func=run, trigger=CronTrigger.from_crontab(cron))
    sched.start()


if __name__ == "__main__":
    try:
        fire.Fire(main)
    except KeyboardInterrupt:
        pass
