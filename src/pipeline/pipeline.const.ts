import JoiDefault, { NumberSchema, Schema } from 'joi';
import dayjs from 'dayjs';

import { CreateLoadStreamOptions } from '../bigquery.service';

const Joi = JoiDefault.defaults((schema) => {
    if (schema.type === 'number') {
        return (schema as NumberSchema)
            .allow(null)
            .unsafe()
            .custom((value) => (value ? parseFloat((value as number).toFixed(6)) : null));
    }

    return schema.allow(null).allow('');
});

const timestampSchema = Joi.string().custom((value) => {
    const parsed = dayjs(value);
    return parsed.isValid() ? parsed.toISOString() : null;
});

export type Pipeline = {
    getConfig: { url: string };
    schema: Schema;
    loadConfig: CreateLoadStreamOptions;
};

export const Customer: Pipeline = {
    getConfig: {
        url: '/customers',
    },
    schema: Joi.object({
        company_id: Joi.number(),
        office_id: Joi.number(),
        creator_id: Joi.number(),
        agent_id: Joi.number(),
        setter_id: Joi.number(),
        first_name: Joi.string(),
        last_name: Joi.string(),
        email: Joi.string(),
        mobile: Joi.string(),
        secondary_first_name: Joi.string(),
        secondary_last_name: Joi.string(),
        secondary_mobile: Joi.string(),
        secondary_email: Joi.string(),
        address: Joi.string(),
        city: Joi.string(),
        state: Joi.string(),
        zip: Joi.string(),
        lat: Joi.string(),
        lng: Joi.string(),
        timezone: Joi.string(),
        details: Joi.string(),
        external_id: Joi.string(),
        lead_source: Joi.string(),
        status_id: Joi.number(),
        lead_router_id: Joi.string(),
        referral_id: Joi.string(),
        deleted_at: Joi.string(),
        language: Joi.string(),
        fullName: Joi.string(),
        id: Joi.number(),
        created: timestampSchema,
        updated: timestampSchema,
        fullState: Joi.string(),
        fullAddress: Joi.string(),
        status_name: Joi.string(),
        created_by_name: Joi.string(),
        created_by_email: Joi.string(),
        created_by_phone: Joi.string(),
        creator: Joi.object({
            first_name: Joi.string(),
            last_name: Joi.string(),
            email: Joi.string(),
            phone: Joi.string(),
            timezone: Joi.string(),
            id: Joi.number(),
        }),
        integrations: Joi.alternatives().try(
            Joi.object({
                JobNimbus: Joi.object({
                    Lead: Joi.object({
                        integration_record_id: Joi.string(),
                        record_type: Joi.string(),
                        enerflo_id: Joi.number(),
                        last_synced: timestampSchema,
                        updated_at: timestampSchema,
                        created_at: timestampSchema,
                    }),
                }),
            }),
            Joi.array().strip(),
        ),
        owner: Joi.object({
            first_name: Joi.string(),
            last_name: Joi.string(),
            email: Joi.string(),
            valid_email: Joi.number(),
            phone: Joi.string(),
            timezone: Joi.string(),
            id: Joi.number(),
            meta: Joi.object({
                inactive: Joi.string(),
                intercom_id: Joi.string(),
                is_view_only: Joi.string(),
                allow_optimus: Joi.string(),
                valid_email_ts: timestampSchema,
                sales_rep_license: Joi.string(),
                can_create_customers: Joi.string(),
            }),
        }),
        setter: Joi.object({
            first_name: Joi.string(),
            last_name: Joi.string(),
            email: Joi.string(),
            valid_email: Joi.number(),
            phone: Joi.string(),
            timezone: Joi.string(),
            id: Joi.number(),
            meta: {
                intercom_id: Joi.string(),
                valid_email_ts: timestampSchema,
            },
        }),
        office: Joi.object({
            office_name: Joi.string(),
            office_id: Joi.number(),
            office_city: Joi.string(),
            office_address: Joi.string(),
            office_zip: Joi.string(),
            office_tz: Joi.string(),
            office_state: Joi.string(),
            sms_number: Joi.string(),
        }),
        company: Joi.object({
            company_name: Joi.string(),
            company_phone: Joi.string(),
            company_address: Joi.string(),
            company_city: Joi.string(),
            company_state: Joi.string(),
            company_zip: Joi.string(),
            company_email: Joi.string(),
            company_timezone: Joi.string(),
        }),
        customer_notes: Joi.string(),
        customer_portal_url: Joi.string(),
        surveys: Joi.custom((value) => {
            return Object.entries(value).map(([key, value]: [any, any]) => ({
                key,
                value: {
                    name: value.name,
                    type: value.type,
                    id: value.id,
                    install: {
                        id: value.install?.id,
                        status: value.install?.status,
                        created_at: value.install?.created_at,
                        updated_at: value.install?.updated_at,
                    },
                },
            }));
        }),
    }),
    loadConfig: {
        table: 'Customers',
        schema: [
            { name: 'company_id', type: 'INTEGER' },
            { name: 'office_id', type: 'INTEGER' },
            { name: 'creator_id', type: 'INTEGER' },
            { name: 'agent_id', type: 'INTEGER' },
            { name: 'setter_id', type: 'INTEGER' },
            { name: 'first_name', type: 'STRING' },
            { name: 'last_name', type: 'STRING' },
            { name: 'email', type: 'STRING' },
            { name: 'mobile', type: 'STRING' },
            { name: 'secondary_first_name', type: 'STRING' },
            { name: 'secondary_last_name', type: 'STRING' },
            { name: 'secondary_mobile', type: 'STRING' },
            { name: 'secondary_email', type: 'STRING' },
            { name: 'address', type: 'STRING' },
            { name: 'city', type: 'STRING' },
            { name: 'state', type: 'STRING' },
            { name: 'zip', type: 'STRING' },
            { name: 'lat', type: 'STRING' },
            { name: 'lng', type: 'STRING' },
            { name: 'timezone', type: 'STRING' },
            { name: 'details', type: 'STRING' },
            { name: 'external_id', type: 'STRING' },
            { name: 'lead_source', type: 'STRING' },
            { name: 'status_id', type: 'INTEGER' },
            { name: 'lead_router_id', type: 'STRING' },
            { name: 'referral_id', type: 'STRING' },
            { name: 'deleted_at', type: 'STRING' },
            { name: 'language', type: 'STRING' },
            { name: 'fullName', type: 'STRING' },
            { name: 'id', type: 'INTEGER' },
            { name: 'created', type: 'TIMESTAMP' },
            { name: 'updated', type: 'TIMESTAMP' },
            { name: 'fullState', type: 'STRING' },
            { name: 'fullAddress', type: 'STRING' },
            { name: 'status_name', type: 'STRING' },
            { name: 'created_by_name', type: 'STRING' },
            { name: 'created_by_email', type: 'STRING' },
            { name: 'created_by_phone', type: 'STRING' },
            {
                name: 'creator',
                type: 'record',
                fields: [
                    { name: 'first_name', type: 'STRING' },
                    { name: 'last_name', type: 'STRING' },
                    { name: 'email', type: 'STRING' },
                    { name: 'phone', type: 'STRING' },
                    { name: 'timezone', type: 'STRING' },
                    { name: 'id', type: 'INTEGER' },
                ],
            },
            {
                name: 'integrations',
                type: 'record',
                fields: [
                    {
                        name: 'JobNimbus',
                        type: 'record',
                        fields: [
                            {
                                name: 'Lead',
                                type: 'record',
                                fields: [
                                    { name: 'integration_record_id', type: 'STRING' },
                                    { name: 'record_type', type: 'STRING' },
                                    { name: 'enerflo_id', type: 'INTEGER' },
                                    { name: 'last_synced', type: 'TIMESTAMP' },
                                    { name: 'updated_at', type: 'TIMESTAMP' },
                                    { name: 'created_at', type: 'TIMESTAMP' },
                                ],
                            },
                        ],
                    },
                ],
            },
            {
                name: 'owner',
                type: 'record',
                fields: [
                    { name: 'first_name', type: 'STRING' },
                    { name: 'last_name', type: 'STRING' },
                    { name: 'email', type: 'STRING' },
                    { name: 'valid_email', type: 'INTEGER' },
                    { name: 'phone', type: 'STRING' },
                    { name: 'timezone', type: 'STRING' },
                    { name: 'id', type: 'INTEGER' },
                    {
                        name: 'meta',
                        type: 'record',
                        fields: [
                            { name: 'inactive', type: 'STRING' },
                            { name: 'intercom_id', type: 'STRING' },
                            { name: 'is_view_only', type: 'STRING' },
                            { name: 'allow_optimus', type: 'STRING' },
                            { name: 'valid_email_ts', type: 'TIMESTAMP' },
                            { name: 'sales_rep_license', type: 'STRING' },
                            { name: 'can_create_customers', type: 'STRING' },
                        ],
                    },
                ],
            },
            {
                name: 'setter',
                type: 'record',
                fields: [
                    { name: 'first_name', type: 'STRING' },
                    { name: 'last_name', type: 'STRING' },
                    { name: 'email', type: 'STRING' },
                    { name: 'valid_email', type: 'INTEGER' },
                    { name: 'phone', type: 'STRING' },
                    { name: 'timezone', type: 'STRING' },
                    { name: 'id', type: 'INTEGER' },
                    {
                        name: 'meta',
                        type: 'record',
                        fields: [
                            { name: 'intercom_id', type: 'STRING' },
                            { name: 'valid_email_ts', type: 'TIMESTAMP' },
                        ],
                    },
                ],
            },
            {
                name: 'office',
                type: 'record',
                fields: [
                    { name: 'office_name', type: 'STRING' },
                    { name: 'office_id', type: 'INTEGER' },
                    { name: 'office_city', type: 'STRING' },
                    { name: 'office_address', type: 'STRING' },
                    { name: 'office_zip', type: 'STRING' },
                    { name: 'office_tz', type: 'STRING' },
                    { name: 'office_state', type: 'STRING' },
                    { name: 'sms_number', type: 'STRING' },
                ],
            },
            {
                name: 'company',
                type: 'record',
                fields: [
                    { name: 'company_name', type: 'STRING' },
                    { name: 'company_phone', type: 'STRING' },
                    { name: 'company_address', type: 'STRING' },
                    { name: 'company_city', type: 'STRING' },
                    { name: 'company_state', type: 'STRING' },
                    { name: 'company_zip', type: 'STRING' },
                    { name: 'company_email', type: 'STRING' },
                    { name: 'company_timezone', type: 'STRING' },
                ],
            },
            { name: 'customer_notes', type: 'STRING' },
            { name: 'customer_portal_url', type: 'STRING' },
            {
                name: 'surveys',
                type: 'record',
                mode: 'repeated',
                fields: [
                    { name: 'key', type: 'STRING' },
                    {
                        name: 'value',
                        type: 'record',
                        fields: [
                            { name: 'name', type: 'STRING' },
                            { name: 'type', type: 'STRING' },
                            { name: 'id', type: 'INTEGER' },
                            {
                                name: 'install',
                                type: 'record',
                                fields: [
                                    { name: 'id', type: 'INTEGER' },
                                    { name: 'status', type: 'STRING' },
                                    { name: 'created_at', type: 'INTEGER' },
                                    { name: 'updated_at', type: 'INTEGER' },
                                ],
                            },
                        ],
                    },
                ],
            },
        ],
    },
};
