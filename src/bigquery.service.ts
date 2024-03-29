import { BigQuery, TableSchema } from '@google-cloud/bigquery';

import { logger } from './logging.service';

const client = new BigQuery();

const DATASET = 'Enerflo';

export type CreateLoadStreamOptions = {
    table: string;
    schema: Record<string, any>[];
};

export const createLoadStream = (options: CreateLoadStreamOptions) => {
    return client
        .dataset(DATASET)
        .table(options.table)
        .createWriteStream({
            schema: { fields: options.schema } as TableSchema,
            sourceFormat: 'NEWLINE_DELIMITED_JSON',
            createDisposition: 'CREATE_IF_NEEDED',
            writeDisposition: 'WRITE_TRUNCATE',
        })
        .on('job', (job) => {
            logger.info({ action: 'load', table: options.table, id: job.id });
        });
};
