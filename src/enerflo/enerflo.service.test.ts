import { get } from './enerflo.service';

it('get', (done) => {
    get('/customers').then((stream) => {
        stream.on('data', (data) => {
            console.log({ data });
        });
        stream.on('error', (error) => {
            console.error(error);
            done(error);
        });
        stream.on('end', () => {
            done();
        });
    });
});
