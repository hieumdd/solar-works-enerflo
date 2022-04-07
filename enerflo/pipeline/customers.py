NAME = "Customers"
ENDPOINT = "customers"


def transform(rows: list[dict]) -> list[dict]:
    return [
        {
            "company_id": row.get("company_id"),
            "office_id": row.get("office_id"),
            "creator_id": row.get("creator_id"),
            "agent_id": row.get("agent_id"),
            "setter_id": row.get("setter_id"),
            "first_name": row.get("first_name"),
            "last_name": row.get("last_name"),
            "email": row.get("email"),
            "mobile": row.get("mobile"),
            "secondary_first_name": row.get("secondary_first_name"),
            "secondary_last_name": row.get("secondary_last_name"),
            "secondary_mobile": row.get("secondary_mobile"),
            "secondary_email": row.get("secondary_email"),
            "address": row.get("address"),
            "city": row.get("city"),
            "state": row.get("state"),
            "zip": row.get("zip"),
            "lat": row.get("lat"),
            "lng": row.get("lng"),
            "timezone": row.get("timezone"),
            "details": row.get("details"),
            "external_id": row.get("external_id"),
            "lead_source": row.get("lead_source"),
            "status_id": row.get("status_id"),
            "lead_router_id": row.get("lead_router_id"),
            "referral_id": row.get("referral_id"),
            "deleted_at": row.get("deleted_at"),
            "language": row.get("language"),
            "fullName": row.get("fullName"),
            "id": row.get("id"),
            "created": row.get("created"),
            "updated": row.get("updated"),
            "fullState": row.get("fullState"),
            "fullAddress": row.get("fullAddress"),
            "status_name": row.get("status_name"),
            "created_by_name": row.get("created_by_name"),
            "created_by_email": row.get("created_by_email"),
            "created_by_phone": row.get("created_by_phone"),
            "creator": {
                "first_name": row["creator"].get("first_name"),
                "last_name": row["creator"].get("last_name"),
                "email": row["creator"].get("email"),
                "phone": row["creator"].get("phone"),
                "timezone": row["creator"].get("timezone"),
                "id": row["creator"].get("id"),
            }
            if row.get("creator")
            else {},
            "integrations": {
                "JobNimbus": {
                    "Lead": {
                        "integration_record_id": row["integrations"]["JobNimbus"][
                            "Lead"
                        ].get("integration_record_id"),
                        "record_type": row["integrations"]["JobNimbus"]["Lead"].get(
                            "record_type"
                        ),
                        "enerflo_id": row["integrations"]["JobNimbus"]["Lead"].get(
                            "enerflo_id"
                        ),
                        "last_synced": row["integrations"]["JobNimbus"]["Lead"].get(
                            "last_synced"
                        ),
                        "updated_at": row["integrations"]["JobNimbus"]["Lead"].get(
                            "updated_at"
                        ),
                        "created_at": row["integrations"]["JobNimbus"]["Lead"].get(
                            "created_at"
                        ),
                    }
                    if row["integrations"]["JobNimbus"].get("Lead")
                    else {},
                }
                if row["integrations"].get("JobNimbus")
                else {}
            }
            if row.get("integrations")
            else {},
            "owner": {
                "first_name": row["owner"].get("first_name"),
                "last_name": row["owner"].get("last_name"),
                "email": row["owner"].get("email"),
                "valid_email": row["owner"].get("valid_email"),
                "phone": row["owner"].get("phone"),
                "timezone": row["owner"].get("timezone"),
                "id": row["owner"].get("id"),
                "meta": {
                    "inactive": row["owner"]["meta"].get("inactive"),
                    "intercom_id": row["owner"]["meta"].get("intercom_id"),
                    "is_view_only": row["owner"]["meta"].get("is_view_only"),
                    "allow_optimus": row["owner"]["meta"].get("allow_optimus"),
                    "valid_email_ts": row["owner"]["meta"].get("valid_email_ts"),
                    "sales_rep_license": row["owner"]["meta"].get("sales_rep_license"),
                    "can_create_customers": row["owner"]["meta"].get(
                        "can_create_customers"
                    ),
                }
                if row["owner"].get("meta")
                else {},
            }
            if row.get("owner")
            else {},
            "office": {
                "office_name": row["office"].get("office_name"),
                "office_id": row["office"].get("office_id"),
                "office_city": row["office"].get("office_city"),
                "office_address": row["office"].get("office_address"),
                "office_zip": row["office"].get("office_zip"),
                "office_tz": row["office"].get("office_tz"),
                "office_state": row["office"].get("office_state"),
                "sms_number": row["office"].get("sms_number"),
            },
            "company": {
                "company_name": row["company"].get("company_name"),
                "company_phone": row["company"].get("company_phone"),
                "company_address": row["company"].get("company_address"),
                "company_city": row["company"].get("company_city"),
                "company_state": row["company"].get("company_state"),
                "company_zip": row["company"].get("company_zip"),
                "company_email": row["company"].get("company_email"),
                "company_timezone": row["company"].get("company_timezone"),
            },
            "customer_notes": row.get("customer_notes"),
            "customer_portal_url": row.get("customer_portal_url"),
            "surveys": [
                {
                    "key": key,
                    "value": {
                        "name": value.get("name"),
                        "type": value.get("type"),
                        "id": value.get("id"),
                        "install": {
                            "id": value["install"].get("id"),
                            "status": value["install"].get("status"),
                            "created_at": value["install"].get("created_at"),
                            "updated_at": value["install"].get("updated_at"),
                        }
                        if value.get("install")
                        else {},
                    },
                }
                for key, value in row["surveys"].items()
            ]
            if row.get("surveys")
            else [],
        }
        for row in rows
    ]


SCHEMA = [
    {"name": "company_id", "type": "INTEGER"},
    {"name": "office_id", "type": "INTEGER"},
    {"name": "creator_id", "type": "INTEGER"},
    {"name": "agent_id", "type": "INTEGER"},
    {"name": "setter_id", "type": "INTEGER"},
    {"name": "first_name", "type": "STRING"},
    {"name": "last_name", "type": "STRING"},
    {"name": "email", "type": "STRING"},
    {"name": "mobile", "type": "STRING"},
    {"name": "secondary_first_name", "type": "STRING"},
    {"name": "secondary_last_name", "type": "STRING"},
    {"name": "secondary_mobile", "type": "STRING"},
    {"name": "secondary_email", "type": "STRING"},
    {"name": "address", "type": "STRING"},
    {"name": "city", "type": "STRING"},
    {"name": "state", "type": "STRING"},
    {"name": "zip", "type": "STRING"},
    {"name": "lat", "type": "STRING"},
    {"name": "lng", "type": "STRING"},
    {"name": "timezone", "type": "STRING"},
    {"name": "details", "type": "STRING"},
    {"name": "external_id", "type": "STRING"},
    {"name": "lead_source", "type": "STRING"},
    {"name": "status_id", "type": "INTEGER"},
    {"name": "lead_router_id", "type": "STRING"},
    {"name": "referral_id", "type": "STRING"},
    {"name": "deleted_at", "type": "STRING"},
    {"name": "language", "type": "STRING"},
    {"name": "fullName", "type": "STRING"},
    {"name": "id", "type": "INTEGER"},
    {"name": "created", "type": "TIMESTAMP"},
    {"name": "updated", "type": "TIMESTAMP"},
    {"name": "fullState", "type": "STRING"},
    {"name": "fullAddress", "type": "STRING"},
    {"name": "status_name", "type": "STRING"},
    {"name": "created_by_name", "type": "STRING"},
    {"name": "created_by_email", "type": "STRING"},
    {"name": "created_by_phone", "type": "STRING"},
    {
        "name": "creator",
        "type": "record",
        "fields": [
            {"name": "first_name", "type": "STRING"},
            {"name": "last_name", "type": "STRING"},
            {"name": "email", "type": "STRING"},
            {"name": "phone", "type": "STRING"},
            {"name": "timezone", "type": "STRING"},
            {"name": "id", "type": "INTEGER"},
        ],
    },
    {
        "name": "integrations",
        "type": "record",
        "fields": [
            {
                "name": "JobNimbus",
                "type": "record",
                "fields": [
                    {
                        "name": "Lead",
                        "type": "record",
                        "fields": [
                            {"name": "integration_record_id", "type": "STRING"},
                            {"name": "record_type", "type": "STRING"},
                            {"name": "enerflo_id", "type": "INTEGER"},
                            {"name": "last_synced", "type": "TIMESTAMP"},
                            {"name": "updated_at", "type": "TIMESTAMP"},
                            {"name": "created_at", "type": "TIMESTAMP"},
                        ],
                    }
                ],
            }
        ],
    },
    {
        "name": "owner",
        "type": "record",
        "fields": [
            {"name": "first_name", "type": "STRING"},
            {"name": "last_name", "type": "STRING"},
            {"name": "email", "type": "STRING"},
            {"name": "valid_email", "type": "INTEGER"},
            {"name": "phone", "type": "STRING"},
            {"name": "timezone", "type": "STRING"},
            {"name": "id", "type": "INTEGER"},
            {
                "name": "meta",
                "type": "record",
                "fields": [
                    {"name": "inactive", "type": "STRING"},
                    {"name": "intercom_id", "type": "STRING"},
                    {"name": "is_view_only", "type": "STRING"},
                    {"name": "allow_optimus", "type": "STRING"},
                    {"name": "valid_email_ts", "type": "TIMESTAMP"},
                    {"name": "sales_rep_license", "type": "STRING"},
                    {"name": "can_create_customers", "type": "STRING"},
                ],
            },
        ],
    },
    {
        "name": "office",
        "type": "record",
        "fields": [
            {"name": "office_name", "type": "STRING"},
            {"name": "office_id", "type": "INTEGER"},
            {"name": "office_city", "type": "STRING"},
            {"name": "office_address", "type": "STRING"},
            {"name": "office_zip", "type": "STRING"},
            {"name": "office_tz", "type": "STRING"},
            {"name": "office_state", "type": "STRING"},
            {"name": "sms_number", "type": "STRING"},
        ],
    },
    {
        "name": "company",
        "type": "record",
        "fields": [
            {"name": "company_name", "type": "STRING"},
            {"name": "company_phone", "type": "STRING"},
            {"name": "company_address", "type": "STRING"},
            {"name": "company_city", "type": "STRING"},
            {"name": "company_state", "type": "STRING"},
            {"name": "company_zip", "type": "STRING"},
            {"name": "company_email", "type": "STRING"},
            {"name": "company_timezone", "type": "STRING"},
        ],
    },
    {"name": "customer_notes", "type": "STRING"},
    {"name": "customer_portal_url", "type": "STRING"},
    {
        "name": "surveys",
        "type": "record",
        "mode": "repeated",
        "fields": [
            {"name": "key", "type": "STRING"},
            {
                "name": "value",
                "type": "record",
                "fields": [
                    {"name": "name", "type": "STRING"},
                    {"name": "type", "type": "STRING"},
                    {"name": "id", "type": "INTEGER"},
                    {
                        "name": "install",
                        "type": "record",
                        "fields": [
                            {"name": "id", "type": "INTEGER"},
                            {"name": "status", "type": "STRING"},
                            {"name": "created_at", "type": "INTEGER"},
                            {"name": "updated_at", "type": "INTEGER"},
                        ],
                    },
                ],
            },
        ],
    },
]
