"""Scraper module."""

from fire import Fire
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from config import huey
from logzero import logger
from task import ingest
from tool import fetch, get_job_ids

huey  # NOTE: TO SILENT THE STUPID LINTER!


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
        Fire(main)
    except KeyboardInterrupt:
        pass
