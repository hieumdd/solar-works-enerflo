import { Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import Joi from 'joi';
import ndjson from 'ndjson';

import { logger } from '../logging.service';
import { createLoadStream } from '../bigquery.service';
import { get } from '../enerflo/enerflo.service';
import { Pipeline } from './pipeline.const';

const transformValidation = (schema: Joi.Schema) => {
    return new Transform({
        objectMode: true,
        transform: (row: any, _, callback) => {
            const { value, error } = schema.validate(row, {
                stripUnknown: true,
                abortEarly: false,
            });
            if (error) {
                callback(error);
                return;
            }
            callback(null, value);
        },
    });
};

export const runPipeline = async (pipeline_: Pipeline) => {
    logger.info({ action: 'start', pipeline: pipeline_.loadConfig.table });

    const stream = await get(pipeline_.getConfig.url);

    return pipeline(
        stream,
        transformValidation(pipeline_.schema),
        ndjson.stringify(),
        createLoadStream({
            table: `p_${pipeline_.loadConfig.table}`,
            schema: pipeline_.loadConfig.schema,
        }),
    ).then(() => ({ table: pipeline_.loadConfig.table }));
};
