from compose import compose

from db.bigquery import load
from enerflo import enerflo_repo
from enerflo.pipeline import customers


def enerflo_service():
    return compose(
        lambda x: {
            "table": customers.NAME,
            "output_rows": x,
        },
        load(customers.NAME, customers.SCHEMA),
        customers.transform,
        enerflo_repo.get(customers.ENDPOINT),
    )()
