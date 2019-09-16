"""Task."""


from datetime import datetime

from database import orm, City, Company, Skill, Job
from config import huey
from logzero import logger
from tenacity import retry, retry_if_exception_type


@huey.task()
@retry(retry=retry_if_exception_type(orm.TransactionIntegrityError))
def ingest(data):
    """Save to database."""

    logger.info("Processing objectID: " + data["objectID"])

    with orm.db_session:
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
