import express from 'express';
import { http } from '@google-cloud/functions-framework';

import { logger } from './logging.service';
import { Customer } from './pipeline/pipeline.const';
import { runPipeline } from './pipeline/pipeline.service';

const app = express();

app.use(({ path, params, body }, _, next) => {
    logger.debug({ path, params, body });
    next();
});

app.post('/', (_, res) => {
    runPipeline(Customer)
        .then((result) => {
            logger.info({ action: 'done' });
            res.status(200).json({ result });
        })
        .catch((error) => {
            logger.error({ error });
            res.status(500).json({ error });
        });
});

http('main', app);
