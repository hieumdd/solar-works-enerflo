from typing import Any

from google.cloud import bigquery


DATASET = "Enerflo"

client = bigquery.Client()


def load(table: str, schema: list[dict[str, Any]]):
    def _load(data: list[dict[str, Any]]) -> int:
        if len(data) == 0:
            return 0

        output_rows = (
            client.load_table_from_json(  # type: ignore
                data,
                f"{DATASET}.{table}",
                job_config=bigquery.LoadJobConfig(
                    create_disposition="CREATE_IF_NEEDED",
                    write_disposition="WRITE_TRUNCATE",
                    schema=schema,
                ),
            )
            .result()
            .output_rows
        )

        return output_rows

    return _load
