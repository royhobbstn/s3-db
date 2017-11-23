'use strict';

const fs = require('fs');
const path = require('path');
const Papa = require('papaparse');
const Promise = require('bluebird');
const AWS = require('aws-sdk');

const s3 = new AWS.S3();
const myBucket = 's3db-acs1115';
const folder = path.join(__dirname, 'acs_temp_cproj/ready/');


// loop through all sequence files
fs.readdir(folder, (err, files) => {
    if (err) {
        console.log('error: ', err);
        process.exit();
    }

    // https://codeburst.io/javascript-async-await-with-foreach-b6ba62bbf404
    const asyncForEach = async(array, callback) => {
        for (let index = 0; index < array.length; index++) {
            await callback(array[index], index, array);
        }
    };

    // parse estimate files
    const start = async() => {
        await asyncForEach(files, async(file) => {
            console.log(`reading: ${file}`);
            const file_data = fs.readFileSync(path.join(__dirname, `acs_temp_cproj/ready/${file}`), { encoding: 'binary' });
            console.log(`parsing: ${file}`);
            await parseFile(file_data, file);
            console.log(`done with: ${file}`);
        });
        console.log('Done');
    };

    start();

});


function parseFile(file_data, file) {
    return new Promise((resolve, reject) => {
        const data_cache = {};

        Papa.parse(file_data, {
            header: true,
            complete: function () {

                let put_object_array = [];

                Object.keys(data_cache).forEach(sequence => {
                    Object.keys(data_cache[sequence]).forEach(sumlev => {
                        Object.keys(data_cache[sequence][sumlev]).forEach(aggregator => {
                            const filename = `${sequence}/${sumlev}/${aggregator}.json`;
                            console.log(`insert: ${filename}`);
                            const data = data_cache[sequence][sumlev][aggregator];
                            put_object_array.push({ filename, data });
                        });
                    });
                });

                // run up to 5 AWS PutObject calls concurrently
                Promise.map(put_object_array, function (obj) {
                    return putObject(obj.filename, obj.data);
                }, { concurrency: 5 }).then(d => {
                    console.log(`inserted: ${d.length} objects into S3`);
                    console.log("Finished");
                    resolve(`finished: ${file}`);
                }).catch(err => {
                    reject(err);
                });

            },
            step: function (results) {

                if (results.errors.length) {
                    console.log('E: ', results.errors);
                    // TODO ending blank line causing issues
                    reject(results.errors);
                    process.exit();
                }

                // only tracts, bg, county, place, state right now
                const sumlev = results.data[0].SUMLEVEL;

                const component = results.data[0].COMPONENT;
                if (sumlev !== '140' && sumlev !== '150' && sumlev !== '050' && sumlev !== '160' && sumlev !== '040') {
                    return;
                }
                if (component !== '00') {
                    return;
                }

                const geoid = results.data[0].GEOID;
                const statecounty = `${results.data[0].STATE}${results.data[0].COUNTY}`;
                const state = results.data[0].STATE;
                const sequence = file.slice(0, 1) + file.split('.')[0].slice(-3);

                let aggregator;

                // aggregation level of each geography
                switch (sumlev) {
                case '140':
                case '150':
                    aggregator = statecounty;
                    break;
                case '160':
                case '050':
                    aggregator = state;
                    break;
                case '040':
                    aggregator = component;
                    break;
                default:
                    console.log(sumlev);
                    console.error('unknown summary level');
                    break;
                }

                if (!data_cache[sequence]) {
                    data_cache[sequence] = {};
                }

                if (!data_cache[sequence][sumlev]) {
                    data_cache[sequence][sumlev] = {};
                }

                if (!data_cache[sequence][sumlev][aggregator]) {
                    data_cache[sequence][sumlev][aggregator] = {};
                }

                // this is how the data will be modeled in S3
                data_cache[sequence][sumlev][aggregator][geoid] = results.data[0];

            }
        });
    });

}


function putObject(key, value) {
    return new Promise((resolve, reject) => {
        const params = { Bucket: myBucket, Key: key, Body: JSON.stringify(value), ContentType: 'application/json' };
        s3.putObject(params, function (err, data) {
            if (err) {
                console.log(err);
                return reject(err);
            }
            else {
                console.log(`Successfully uploaded data to ${key}`);
                console.log(data);
                return resolve(data);
            }
        });
    });

}
