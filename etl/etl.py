"""ETL module."""

import os

import fire
import pandas as pd
import prefect


@prefect.task
def compute_skills_popularity():
    """Compute skills popularity over time."""

    q = """
	SELECT
		DATE(j.published_at) AS date,
		s.name AS skill,
		COUNT(1)
	FROM
		job_skill AS js
		LEFT JOIN job AS j ON js.job = j.id
		LEFT JOIN skill AS s ON js.skill = s.id
	GROUP BY
		DATE(j.published_at),
		s. "name"
	ORDER BY
		DATE(j.published_at)
		DESC;
	"""

    return pd.read_sql(q, os.environ["DB_SCRAPER_URI"])


@prefect.task
def compute_most_active_city():
    """Compute most active city over time."""

    q = """
	SELECT
		DATE(published_at) AS date,
		c. "name",
		COUNT(1)
	FROM
		job AS j
		LEFT JOIN city AS c ON j.city = c.id
	GROUP BY
		DATE(published_at),
		c. "name"
	ORDER BY
		DATE(published_at)
		DESC;
	"""

    return pd.read_sql(q, os.environ["DB_SCRAPER_URI"])


@prefect.task
def save_result(data, table_name):
    """Save computation result."""

    return data.to_sql(
        table_name, os.environ["DB_ETL_URI"], if_exists="replace", index=False
    )


def main(cron="*/5 * * * *"):
    """Run."""

    schedule = prefect.schedules.CronSchedule(cron)
    with prefect.Flow("ETL Pipeline", schedule=schedule) as flow:

        skills_popularity = compute_skills_popularity()
        save_result(skills_popularity, "skills_popularity")

        most_active_city = compute_most_active_city()
        save_result(most_active_city, "most_active_city")

    flow.run()


if __name__ == "__main__":
    try:
        fire.Fire(main)
    except KeyboardInterrupt:
        pass
