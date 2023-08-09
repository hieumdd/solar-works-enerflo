import * as pipelines from './pipeline.const';
import { runPipeline } from './pipeline.service';

it('run-pipeline', async () => {
    return runPipeline(pipelines.Customer)
        .then((result) => {
            console.log({ result });
        })
        .catch((error) => {
            console.error({ error });
            throw error;
        });
}, 100_000_000);
