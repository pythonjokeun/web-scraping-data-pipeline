"""Tool."""

import json
import os

import requests
from database import orm, Job


def fetch(filter_object_id=[]):
    """Fetch raw data."""

    data = {
        "requests": [
            {
                "indexName": "job_postings",
                "params": 'query=&hitsPerPage=1000&facetFilters=[["city.work_country_name:Indonesia"]]',
            }
        ]
    }

    if filter_object_id:
        # Construct filter to exclude passed objectID
        filter = f"&filters=NOT objectID:{filter_object_id[0]}"

        for x in filter_object_id[1:]:
            filter += f" AND NOT objectID:{x}"

        data["requests"][0]["params"] += filter

    req = requests.post(os.environ["REQUEST_URL"], data=json.dumps(data))
    return req.json()["results"][0]["hits"]


def get_job_ids():
    """Retrieve existing job ids from database."""

    with orm.db_session:
        return list(orm.select(j.id for j in Job)[:])
