#!/usr/bin/env node

const argv = require('yargs').argv;
const states = require('./modules/states');
const Promise = require('bluebird');
const request = require('request');
const fs = require('fs');
const unzip = require('unzip');
const csv = require('csvtojson');
const rimraf = require('rimraf');
const exec = require('child_process').exec;
const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const path = require('path');
const Papa = require('papaparse');


const geography_file_headers = ["FILEID", "STUSAB", "SUMLEVEL", "COMPONENT", "LOGRECNO", "US",
                "REGION", "DIVISION", "STATECE", "STATE", "COUNTY", "COUSUB", "PLACE", "TRACT", "BLKGRP", "CONCIT",
                "AIANHH", "AIANHHFP", "AIHHTLI", "AITSCE", "AITS", "ANRC", "CBSA", "CSA", "METDIV", "MACC", "MEMI",
                "NECTA", "CNECTA", "NECTADIV", "UA", "BLANK1", "CDCURR", "SLDU", "SLDL", "BLANK2", "BLANK3",
                "ZCTA5", "SUBMCD", "SDELM", "SDSEC", "SDUNI", "UR", "PCI", "BLANK4", "BLANK5", "PUMA5", "BLANK6",
                "GEOID", "NAME", "BTTR", "BTBG", "BLANK7"];

// if no states entered as parameters, use all states
const loop_states = argv._.length ? argv._ : Object.keys(states);

console.log(`Selected: ${loop_states}`);

// selected states array into array of objects to facilitate downloading multiple files
let selected_states = [];
loop_states.forEach(state => {
    selected_states.push({ state, isTractBGFile: true });
    selected_states.push({ state, isTractBGFile: false });
});


// main

readyWorkspace()
    .then(() => {
        return Promise.map(selected_states, state => {
            return requestAndSaveStateFileFromACS(state.state, state.isTractBGFile);
        }, { concurrency: 5 });
    })
    .then(() => {
        console.log('merging estimate files');
        return mergeDataFiles('estimate');
    })
    .then(() => {
        console.log('merging moe files');
        return mergeDataFiles('moe');
    })
    .then(() => {
        console.log('merging geo files');
        return mergeGeoFiles();
    })
    .then(() => {
        console.log('all file merging completed');
        return createSchemaFiles();
    })
    .then(schemas => {
        return parseGeofile(schemas);
    })
    .then(arr => {
        return loadDataToS3(arr);
    })
    .then(() => {
        console.log('program complete');
    })
    .catch(err => {
        console.log(err);
        process.exit(); // exit immediately upon error
    });


/**************************/



function loadDataToS3(arr) {
    const schemas = arr[0];
    const keyed_lookup = arr[1];

    const folder = path.join(__dirname, './CensusDL/ready/');

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
                const file_data = fs.readFileSync(path.join(__dirname, `./CensusDL/ready/${file}`), { encoding: 'binary' });
                console.log(`parsing: ${file}`);
                await parseFile(file_data, file, schemas, keyed_lookup);
                console.log(`done with: ${file}`);
            });
            console.log('Done');
        };

        start();

    });

}



function parseFile(file_data, file, schemas, keyed_lookup) {
    return new Promise((resolve, reject) => {
        const data_cache = {};

        Papa.parse(file_data, {
            header: false,
            skipEmptyLines: true,
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
                    console.log(results);
                    console.log('E: ', results.errors);
                    reject(results.errors);
                    process.exit();
                }


                const seq_string = file.split('.')[0].slice(-3);
                const seq_fields = schemas[seq_string];

                const keyed = {};
                results.data[0].forEach((d, i) => {
                    keyed[seq_fields[i]] = d;
                });

                // combine with geo on stustab+logrecno
                const unique_key = keyed.STUSAB + keyed.LOGRECNO;
                const geo_record = keyed_lookup[unique_key];
                const record = Object.assign({}, keyed, geo_record);

                // only tracts, bg, county, place, state right now
                const sumlev = record.SUMLEVEL;

                const component = record.COMPONENT;
                if (sumlev !== '140' && sumlev !== '150' && sumlev !== '050' && sumlev !== '160' && sumlev !== '040') {
                    return;
                }
                if (component !== '00') {
                    return;
                }

                // TODO states not joining

                const geoid = record.GEOID;
                const statecounty = `${record.STATE}${record.COUNTY}`;
                const state = record.STATE;
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
                data_cache[sequence][sumlev][aggregator][geoid] = record;

            }
        });
    });

}


function putObject(key, value) {
    const myBucket = 's3db-acs1115';

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


function parseGeofile(schemas) {

    const file = `./CensusDL/geofile/acs1115_geofile.csv`;
    const file_data = fs.readFileSync(file, 'utf8');

    return new Promise((resolve, reject) => {
        Papa.parse(file_data, {
            header: false,
            delimiter: ',',
            skipEmptyLines: true,
            complete: function (results, file) {
                console.log("Parsing complete:", file);
                const keyed_lookup = convertGeofile(results.data);
                resolve([schemas, keyed_lookup]);
            },
            error: function (error, file) {
                console.log("error:", error, file);
                reject('nope');
            }
        });
    });

}

function convertGeofile(data) {
    // convert geofile to json (with field names).  convert json to key-value
    // join key is stusab(lowercase) + logrecno
    const keyed_lookup = {};

    data.forEach(d => {
        const obj = {};
        d.forEach((item, index) => {
            obj[geography_file_headers[index]] = item;
        });
        const stusab_lc = obj.STUSAB.toLowerCase();
        keyed_lookup[stusab_lc + obj.LOGRECNO] = obj;
    });

    return keyed_lookup;
}


function mergeGeoFiles() {
    return new Promise((resolve, reject) => {
        const command = `cat ./CensusDL/group1/_unzipped/g20155**.csv > ./CensusDL/geofile/acs1115_geofile.csv;`;
        console.log(`running: ${command}`);
        exec(command, function (error, stdout, stderr) {
            if (error) {
                console.log(`error code: ${error.code}`);
                console.log(`stderr: ${stderr}`);
                reject(`error: ${error.code} ${stderr}`);
            }
            console.log('completed merging geofile.');
            resolve('completed merging geofile.');
        });
    });

}


function mergeDataFiles(file_type) {
    const typechar = (file_type === 'moe') ? 'm' : 'e';

    return new Promise((resolve, reject) => {
        const command = `for i in $(seq -f "%03g" 1 122); do cat ./CensusDL/group1/_unzipped/${typechar}20155**0"$i"000.txt ./CensusDL/group2/_unzipped/${typechar}20155**0"$i"000.txt > ./CensusDL/ready/${typechar}seq"$i".csv; done;`;
        console.log(`running: ${command}`);
        exec(command, function (error, stdout, stderr) {
            if (error) {
                console.log(`error code: ${error.code}`);
                console.log(`stderr: ${stderr}`);
                reject(`error: ${error.code} ${stderr}`);
            }
            console.log('completed merging files.');
            resolve('completed merging');
        });
    });
}



function createSchemaFiles() {
    return new Promise((resolve, reject) => {

        const url = 'https://www2.census.gov/programs-surveys/acs/summary_file/2015/documentation/user_tools/ACS_5yr_Seq_Table_Number_Lookup.txt';
        request(url, function (err, resp, body) {
            if (err) { return reject(err); }

            csv({ noheader: false })
                .fromString(body)
                .on('end_parsed', data => {

                    const fields = {};
                    // filter out line number if non-integer value
                    data.forEach(d => {
                        const line_number = Number(d['Line Number']);
                        if (Number.isInteger(line_number) && line_number > 0) {
                            const field_name = d['Table ID'] + String(d['Line Number']).padStart(3, "0");;
                            const seq_num = d['Sequence Number'].slice(1);
                            if (fields[seq_num]) {
                                fields[seq_num].push(field_name);
                            }
                            else {
                                fields[seq_num] = ["FILEID", "FILETYPE", "STUSAB", "CHARITER", "SEQUENCE", "LOGRECNO", field_name];
                            }
                        }
                    });

                    resolve(fields);
                })
                .on('done', () => {
                    //parsing finished
                    console.log('finished parsing schema file');
                });
        });
    });
}


function readyWorkspace() {
    return new Promise((resolve, reject) => {
        // delete ./CensusDL if exists
        rimraf('./CensusDL', function (err) {
            if (err) {
                return reject(err);
            }

            // logic to set up directories
            const directories_in_order = ['./CensusDL', './CensusDL/group1',
            './CensusDL/group2', './CensusDL/ready', './CensusDL/geofile'];

            directories_in_order.forEach(dir => {
                if (!fs.existsSync(dir)) {
                    fs.mkdirSync(dir);
                }
            });

            console.log('workspace ready');
            resolve('done');
        });
    });
}

function requestAndSaveStateFileFromACS(abbr, isTractBGFile) {
    return new Promise((resolve, reject) => {
        const fileName = states[abbr];
        const fileType = isTractBGFile ? '_Tracts_Block_Groups_Only' : '_All_Geographies_Not_Tracts_Block_Groups';
        const outputDir = isTractBGFile ? 'CensusDL/group1/' : 'CensusDL/group2/';
        const fileUrl = `https://www2.census.gov/programs-surveys/acs/summary_file/2015/data/5_year_by_state/${fileName}${fileType}.zip`;
        const outputFile = `${states[abbr]}${fileType}.zip`;

        console.log(`downloading ${fileName}${fileType}.zip`);
        request({ url: fileUrl, encoding: null }, function (err, resp, body) {
            if (err) { return reject(err); }
            fs.writeFile(`${outputDir}${outputFile}`, body, function (err) {
                if (err) { return reject(err); }
                console.log(`${outputFile} written!`);

                // unzip
                const stream = fs.createReadStream(`${outputDir}${outputFile}`);
                stream.pipe(unzip.Extract({ path: `${outputDir}_unzipped` })
                    .on('close', function () {
                        console.log(`${outputFile} unzipped!`);
                        resolve('done unzip');
                    })
                    .on('error', function (err) {
                        reject(err);
                    })
                );
            });
        });
    });
}
