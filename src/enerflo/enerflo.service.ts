import { Readable } from 'node:stream';
import axios, { AxiosInstance } from 'axios';
import { range } from 'lodash';

import { logger } from '../logging.service';

const getClient = () => {
    return axios.create({
        baseURL: 'https://enerflo.io/api/v1',
        headers: { 'api-key': process.env.ENERFLO_API_KEY },
    });
};

const pageSize = 500;

type GetOneOptions = {
    url: string;
    page?: number;
};

type GetOneResponse = {
    dataCount: number;
    data: object[];
};

const getMany = async (client: AxiosInstance, { url, page }: GetOneOptions) => {
    return client
        .request<GetOneResponse>({ method: 'GET', url, params: { pageSize, page } })
        .then((response) => response.data);
};

export const get = async (url: string) => {
    const client = getClient();

    const { dataCount } = await getMany(client, { url });

    const pages = Math.ceil(dataCount / pageSize);

    const stream = new Readable({ objectMode: true, read: () => {} });

    Promise.all(
        range(1, pages + 1).map(async (page) => {
            return getMany(client, { url, page }).then(({ data }) => {
                data.forEach((row) => stream.push(row));
            });
        }),
    )
        .then(() => {
            logger.info({ action: 'get', url });
            stream.push(null);
        })
        .catch((error) => {
            logger.error({ error });
            stream.emit('error', error);
        });

    return stream;
};
