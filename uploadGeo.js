'use strict';

const fs = require('fs');
const path = require('path');
const Papa = require('papaparse');

const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const myBucket = 's3db-acs1115';

const file = path.join(__dirname, 'acs_temp_cproj/ready/eseq106.csv');

const data_cache = {};

Papa.parse(fs.readFileSync(file, { encoding: 'binary' }), {
    header: true,
    complete: function () {

        // TODO batch calls, no more than X at a time.

        let insert_count = 0;
        // add AWS file for each aggregated level
        Object.keys(data_cache).forEach(sequence => {
            Object.keys(data_cache[sequence]).forEach(sumlev => {
                Object.keys(data_cache[sequence][sumlev]).forEach(aggregator => {
                    insert_count++;
                    console.log(`insert: ${sequence}/${sumlev}/${aggregator}.json`);
                    putObject(`${sequence}/${sumlev}/${aggregator}.json`, data_cache[sequence][sumlev][aggregator]);
                });
            });

        });

        console.log("Finished");
        console.log(`inserted ${insert_count} records into S3`);

    },
    step: function (results) {

        if (results.errors.length) {
            // TODO extra column error
            // console.log('E: ', results.errors);
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
        const county = results.data[0].COUNTY;
        const state = results.data[0].STATE;
        const sequence = '106';

        let aggregator;

        // aggregation level of each geography
        switch (sumlev) {
        case '140':
        case '150':
            aggregator = county;
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


function putObject(key, value) {
    const params = { Bucket: myBucket, Key: key, Body: JSON.stringify(value), ContentType: 'application/json' };
    s3.putObject(params, function (err, data) {
        if (err) {
            console.log(err);
        }
        else {
            console.log("Successfully uploaded data to myBucket/myKey");
            console.log(data);
        }
    });
}
