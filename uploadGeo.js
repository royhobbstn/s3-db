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

const data_cache = {};

Papa.parse(fs.readFileSync(file, { encoding: 'binary' }), {
    preview: 10000,
    complete: function () {
        console.log("Finished");
        // console.log(data_cache);
        // TODO loop over OBJ here to upload to S3, rather than in the step function
        // putObject(`geo/${obj.GEOID}.json`, obj);
    },
    step: function (results) {
        if (results.errors.length) {
            console.log(results.errors);
        }

        // only tracts right now
        const sumlev = results.data[0][2];
        if (sumlev !== "140") {
            return;
        }

        // if county doesn't exist in data_cache, create it
        const county = results.data[0][10];
        if (!data_cache[county]) {
            data_cache[county] = [];
        }

        const obj = {};

        results.data[0].forEach((d, i) => {
            // console.log(d)
            obj[header[i]] = d;
        });

        data_cache[county].push(obj);

        // looks like: {"099":[{},{}], "101": [{},{}]}
        // needs to be: {"099": {"GEOID": {}, "GEOID": {} }, "101": {"GEOID": {}, "GEOID": {} } };
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

// TODO geofiles combined with each seq file.

// TODO:  download all geo files, loop and upload all keys to bucket
// cost to put objects in bucket?  could be millions of requests

// was i successfull in getting a block layer for all USA?
// no
// use tippecanoe with state level geojson to export tileset for zoomleve 8+?  
// - use tippecanoe multiple geojson option.
