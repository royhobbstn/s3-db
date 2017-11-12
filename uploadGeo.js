const fs = require('fs');
const path = require('path');
const Papa = require('papaparse');

const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const myBucket = 's3db-acs1115';

const file = path.join(__dirname, 'g20155co.csv');

const header = ['FILEID', 'STUSAB', 'SUMLEVEL', 'COMPONENT', 'LOGRECNO', 'US', 'REGION', 'DIVISION', 'STATECE',
    'STATE', 'COUNTY', 'COUSUB', 'PLACE', 'TRACT', 'BLKGRP', 'CONCIT', 'AIANHH', 'AIANHHFP', 'AIHHTLI',
    'AITSCE', 'AITS', 'ANRC', 'CBSA', 'CSA', 'METDIV', 'MACC', 'MEMI', 'NECTA', 'CNECTA', 'NECTADIV', 'UA',
    'BLANK1', 'CDCURR', 'SLDU', 'SLDL', 'BLANK2', 'BLANK3', 'ZCTA5', 'SUBMCD', 'SDELM', 'SDSEC', 'SDUNI',
    'UR', 'PCI', 'BLANK4', 'BLANK5', 'PUMA5', 'BLANK6', 'GEOID', 'NAME', 'BTTR', 'BTBG', 'BLANK7'];

Papa.parse(fs.readFileSync(file, { encoding: 'binary' }), {
    preview: 100,
    complete: function () {
        console.log("Finished");
    },
    step: function (results) {
        if (results.errors.length) {
            console.log(results.errors);
        }
        const obj = {};
        results.data[0].forEach((d, i) => {
            obj[header[i]] = d;
        });
        putObject(`geo/${obj.GEOID}.json`, obj);
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


// TODO:  download all geo files, loop and upload all keys to bucket
// cost to put objects in bucket?  could be millions of requests
