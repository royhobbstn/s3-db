#!/usr/bin/env node

const argv = require('yargs').argv;
const states = require('./modules/states');
const Promise = require('bluebird');
const request = require('requestretry');
const fs = require('fs');
const unzip = require('unzip');
const csv = require('csvtojson');
const rimraf = require('rimraf');
const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const path = require('path');
const Papa = require('papaparse');


// const dataset = {
//     year: 2014,
//     text: 'acs1014',
//     seq_files: '121'
// };

// const dataset = {
//     year: 2015,
//     text: 'acs1115',
//     seq_files: '122'
// };

const dataset = {
    year: 2016,
    text: 'acs1216',
    seq_files: '122'
};


const geography_file_headers = ["FILEID", "STUSAB", "SUMLEVEL", "COMPONENT", "LOGRECNO", "US",
    "REGION", "DIVISION", "STATECE", "STATE", "COUNTY", "COUSUB", "PLACE", "TRACT", "BLKGRP", "CONCIT",
    "AIANHH", "AIANHHFP", "AIHHTLI", "AITSCE", "AITS", "ANRC", "CBSA", "CSA", "METDIV", "MACC", "MEMI",
    "NECTA", "CNECTA", "NECTADIV", "UA", "BLANK1", "CDCURR", "SLDU", "SLDL", "BLANK2", "BLANK3",
    "ZCTA5", "SUBMCD", "SDELM", "SDSEC", "SDUNI", "UR", "PCI", "BLANK4", "BLANK5", "PUMA5", "BLANK6",
    "GEOID", "NAME", "BTTR", "BTBG", "BLANK7"
];

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
        console.log('renaming and moving files');
        return renameAndMoveFiles();
    })
    .then(() => {
        console.log('creating schema files');
        return createSchemaFiles();
    })
    .then(() => {
        return parseGeofiles();
    })
    .then(() => {
        return loadDataToS3();
    })
    .then(() => {
        console.log('program complete');
    })
    .catch(err => {
        console.log(err);
        process.exit(); // exit immediately upon error
    });


/**************************/

function renameAndMoveFiles() {
    return new Promise((resolve, reject) => {

        // rename and move files in each group 1 and group 2
        const input_folder_1 = path.join(__dirname, './CensusDL/stage1/');
        const input_folder_2 = path.join(__dirname, './CensusDL/stage2/');

        const output_path = path.join(__dirname, './CensusDL/ready/');

        const files1 = fs.readdirSync(input_folder_1);
        files1
            .filter(file => {
                // exclude geo files
                return file.slice(0, 1) !== 'g';
            })
            .forEach(function(file) {
                const old_path = path.join(input_folder_1, file);
                const new_path = path.join(output_path, `1_${file}`);
                fs.renameSync(old_path, new_path);
            });

        const files2 = fs.readdirSync(input_folder_2);
        files2
            .filter(file => {
                // exclude geo files
                return file.slice(0, 1) !== 'g';
            })
            .forEach(function(file) {
                const old_path = path.join(input_folder_2, file);
                const new_path = path.join(output_path, `2_${file}`);
                fs.renameSync(old_path, new_path);
            });

        resolve(true);

    });
}


function loadDataToS3() {

    // load schemas file into memory
    const schemas = JSON.parse(fs.readFileSync('./CensusDL/geofile/schemas.json'));

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

                const state = file.slice(8, 10);
                const e_or_m = file.slice(2, 3);

                const keyed_lookup = JSON.parse(fs.readFileSync(`./CensusDL/geofile/g${dataset.year}5${state}.json`, 'utf8'));

                await parseFile(file_data, file, schemas, keyed_lookup, e_or_m);
                console.log(`done with: ${file}`);
            });
            console.log('Done');
        };

        start();

    });

}



function parseFile(file_data, file, schemas, keyed_lookup, e_or_m) {
    return new Promise((resolve, reject) => {
        const data_cache = {};

        Papa.parse(file_data, {
            header: false,
            skipEmptyLines: true,
            complete: function() {

                let put_object_array = [];

                Object.keys(data_cache).forEach(sequence => {
                    Object.keys(data_cache[sequence]).forEach(sumlev => {
                        Object.keys(data_cache[sequence][sumlev]).forEach(state => {
                            const filename = `${sequence}/${sumlev}/${state}.csv`;
                            const data = data_cache[sequence][sumlev][state];
                            put_object_array.push({ filename, data });
                        });
                    });
                });

                // run up to 5 AWS PutObject calls concurrently
                Promise.map(put_object_array, function(obj) {
                    return putObject(obj.filename, obj.data);
                }, { concurrency: 5 }).then(d => {
                    console.log(`inserted: ${d.length} objects into S3`);
                    console.log("Finished");
                    resolve(`finished: ${file}`);
                }).catch(err => {
                    reject(err);
                });

            },
            step: function(results) {

                if (results.errors.length) {
                    console.log(results);
                    console.log('E: ', results.errors);
                    reject(results.errors);
                    process.exit();
                }

                const seq_string = file.split('.')[0].slice(-6, -3);
                const seq_fields = schemas[seq_string];
                const keyed = {};

                results.data[0].forEach((d, i) => {
                    if (i > 5 && e_or_m === 'm') {
                        keyed[seq_fields[i] + '_moe'] = d;
                    }
                    else {
                        keyed[seq_fields[i]] = d;
                    }
                });

                // combine with geo on stustab+logrecno
                const unique_key = keyed.STUSAB + keyed.LOGRECNO;
                const geo_record = keyed_lookup[unique_key];

                // pick only GEOID and NAME (for now, TODO )from geography file
                const picked = (({ GEOID, NAME }) => ({ GEOID, NAME }))(geo_record);

                // combine geography with data
                const record = Object.assign({}, keyed, picked);

                // only tracts, bg, county, place, state right now
                const sumlev = geo_record.SUMLEVEL;

                const component = geo_record.COMPONENT;
                if (sumlev !== '140' && sumlev !== '150' && sumlev !== '050' && sumlev !== '160' && sumlev !== '040') {
                    return;
                }
                if (component !== '00') {
                    return;
                }

                const state = geo_record.STATE;
                const sequence = file.slice(2, 3) + file.split('.')[0].slice(-6, -3);


                if (!data_cache[sequence]) {
                    data_cache[sequence] = {};
                }

                if (!data_cache[sequence][sumlev]) {
                    data_cache[sequence][sumlev] = {};
                }

                if (!data_cache[sequence][sumlev][state]) {
                    data_cache[sequence][sumlev][state] = [];
                }

                // this is how the data will be modeled in S3		
                data_cache[sequence][sumlev][state].push(record);

            }
        });
    });

}


function putObject(key, value) {
    const myBucket = `s3db-v2-${dataset.text}`;

    return new Promise((resolve, reject) => {
        const params = { Bucket: myBucket, Key: key, Body: Papa.unparse(value, { header: false }), ContentType: 'text/csv' };
        s3.putObject(params, function(err, data) {
            if (err) {
                console.log(err);
                return reject(err);
            }
            else {
                console.log(`Successfully uploaded data to ${key}`);
                return resolve(data);
            }
        });
    });

}


function parseGeofiles() {

    // in sequence read all geofiles and write a geojson file to /geofile

    return Promise.map(loop_states, state => {
        const file = `./CensusDL/stage1/g${dataset.year}5${state}.csv`;
        const file_data = fs.readFileSync(file, 'utf8');

        return new Promise((resolve, reject) => {
            Papa.parse(file_data, {
                header: false,
                delimiter: ',',
                skipEmptyLines: true,
                complete: function(results, file) {
                    const keyed_lookup = convertGeofile(results.data);
                    // save keyed_lookup
                    fs.writeFileSync(`./CensusDL/geofile/g${dataset.year}5${state}.json`, JSON.stringify(keyed_lookup), 'utf8');
                    resolve(true);
                },
                error: function(error, file) {
                    console.log("error:", error, file);
                    reject('nope');
                }
            });
        });


    }, { concurrency: 1 });

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



function createSchemaFiles() {
    return new Promise((resolve, reject) => {

        const url = `https://www2.census.gov/programs-surveys/acs/summary_file/${dataset.year}/documentation/user_tools/ACS_5yr_Seq_Table_Number_Lookup.txt`;
        request(url, function(err, resp, body) {
            if (err) { return reject(err); }

            csv({ noheader: false })
                .fromString(body)
                .on('end_parsed', data => {

                    const fields = {};
                    // filter out line number if non-integer value
                    data.forEach(d => {
                        const line_number = Number(d['Line Number']);
                        if (Number.isInteger(line_number) && line_number > 0) {
                            const field_name = d['Table ID'] + String(d['Line Number']).padStart(3, "0");
                            const seq_num = d['Sequence Number'].slice(1);
                            if (fields[seq_num]) {
                                fields[seq_num].push(field_name);
                            }
                            else {
                                fields[seq_num] = ["FILEID", "FILETYPE", "STUSAB", "CHARITER", "SEQUENCE", "LOGRECNO", field_name];
                            }
                        }
                    });

                    fs.writeFileSync('./CensusDL/geofile/schemas.json', JSON.stringify(fields), 'utf8');
                    resolve(true);
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
        rimraf('./CensusDL', function(err) {
            if (err) {
                return reject(err);
            }

            // logic to set up directories
            const directories_in_order = ['./CensusDL', './CensusDL/group1',
                './CensusDL/group2', './CensusDL/stage1', './CensusDL/stage2',
                './CensusDL/ready', './CensusDL/geofile'
            ];

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
        const stageDir = isTractBGFile ? 'stage1' : 'stage2';
        const fileUrl = `https://www2.census.gov/programs-surveys/acs/summary_file/${dataset.year}/data/5_year_by_state/${fileName}${fileType}.zip`;
        const outputFile = `${states[abbr]}${fileType}.zip`;

        console.log(`downloading ${fileName}${fileType}.zip`);
        request({ url: fileUrl, encoding: null }, function(err, resp, body) {
            if (err) { return reject(err); }
            fs.writeFile(`${outputDir}${outputFile}`, body, function(err) {
                if (err) { return reject(err); }
                console.log(`${outputFile} written!`);

                // unzip
                const stream = fs.createReadStream(`${outputDir}${outputFile}`);
                stream.pipe(unzip.Extract({ path: `CensusDL/${stageDir}` })
                    .on('close', function() {
                        console.log(`${outputFile} unzipped!`);
                        resolve('done unzip');
                    })
                    .on('error', function(err) {
                        reject(err);
                    })
                );
            });
        });
    });
}
