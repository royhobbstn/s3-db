#!/usr/bin/env node

// TODO test multiple states
// TODO add error catchers to csv stream parsing
// TODO why not directly from file to S3?

const argv = require('yargs').argv;
const states = require('./modules/states');
const Promise = require('bluebird');
const request = require('request');
const fs = require('fs');
const unzip = require('unzip');
const csv = require('csvtojson');
const rimraf = require('rimraf');
const json2csv = require('json2csv');
const exec = require('child_process').exec;

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

// object with field names for each seq
let schemas;

readyWorkspace()
    .then(() => {
        return createSchemaFiles();
    })
    .then((schema_obj) => {
        schemas = schema_obj;
        return Promise.map(selected_states, state => {
            return requestAndSaveStateFileFromACS(state.state, state.isTractBGFile);
        }, { concurrency: 5 });
    })
    .then(d => {
        console.log(`downloaded: ${d.length} files from Census.`);
        console.log("converting tracts and block groups to csv");
        return combineGeoWithData('./CensusDL/group1/_unzipped', 'group1');
    }).then(() => {
        console.log('converting all other geographies to csv');
        return combineGeoWithData('./CensusDL/group2/_unzipped', 'group2');
    }).then(() => {
        mergeFiles('estimate');
        console.log('all estimate files de-normalized and converted to csv');
    }).then(() => {
        mergeFiles('moe');
        console.log('all moe files de-normalized and converted to csv');
        console.log('program complete');
    }).catch(err => {
        console.log(err);
        process.exit(); // exit immediately upon error
    });



function mergeFiles(file_type) {
    const typechar = (file_type === 'moe') ? 'm' : 'e';

    return new Promise((resolve, reject) => {
        const command = `for i in $(seq -f "%03g" 1 122); do awk 'FNR==1 && NR!=1{next;}{print}' ./CensusDL/group1ready/${typechar}20155**0"$i"000*.csv ./CensusDL/group2ready/${typechar}20155**0"$i"000*.csv > ./CensusDL/ready/${typechar}seq"$i".csv; done;`;
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
    })

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
            './CensusDL/group2', './CensusDL/group1ready', './CensusDL/group2ready', './CensusDL/ready'];

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

function combineGeoWithData(dirname, group) {
    return new Promise((resolve, reject) => {

        // read unzipped directory - find geog csv files
        fs.readdir(dirname, function (err, filenames) {
            if (err) {
                return reject(err);
            }

            const geo_files = filenames.filter(function (filename) {
                // if filename ends with .csv
                const extension = filename.split('.')[1];
                return (extension === 'csv');
            });

            const geo_done = Promise.map(geo_files, file => {
                return getStateGeofileJSON(dirname, file, group);
            }, { concurrency: 1 });

            Promise.all(geo_done).then(() => {
                console.log('diddy');
                resolve(true);
            });

        });

    });
}

function getStateGeofileJSON(dirname, geofile, group) {
    return new Promise((resolve, reject) => {
        fs.readFile(`${dirname}/${geofile}`, 'utf-8', function (err, content) {
            if (err) {
                return reject(err);
            }

            // convert content to JSON
            csv({
                    noheader: true,
                    headers: geography_file_headers
                })
                .fromString(content)
                .on('end_parsed', data => {

                    // convert to key lookup
                    const obj = {};
                    data.forEach(d => {
                        obj[d.STUSAB.toLowerCase() + d.LOGRECNO] = d;
                    });

                    const file_state_pattern = geofile.split('.')[0].slice(1);
                    const files = fs.readdirSync(dirname);

                    const txt_files = files.filter(function (name) {
                        const extension = name.split('.')[1];
                        const not_geog_file = (name.length === 19); // exclude geog file
                        return (extension === 'txt' && name.includes(file_state_pattern) && not_geog_file);
                    });

                    const txts = Promise.map(txt_files, file => {
                        const content = fs.readFileSync(`${dirname}/${file}`, 'utf-8');
                        const seq = file.split('.')[0].slice(9, -3); // e20155de0001000.txt

                        return parseTxtCSVs(obj, seq, content, file, group);

                    }, { concurrency: 1 });

                    Promise.all(txts).then(() => {
                        resolve(true);
                    }).catch(err => {
                        reject(err);
                    });

                })
                .on('done', () => {
                    // parsing finished
                });

        });
    });
}

function parseTxtCSVs(obj, seq, content, file, group) {
    //
    return new Promise((resolve, reject) => {
        csv({ noheader: true, headers: schemas[seq] })
            .fromString(content)
            .transf((jsonObj, csvRow, index) => {
                // mutate json obj
                const linked_record = obj[jsonObj.STUSAB + jsonObj.LOGRECNO];
                geography_file_headers.forEach(header => {
                    jsonObj[header] = linked_record[header];
                });
            })
            .on('end_parsed', data => {
                try {
                    const result = json2csv({ data });
                    const filename_csv = file.split('.')[0] + '.csv';
                    console.log(`saving ${filename_csv}`);
                    fs.writeFileSync(`./CensusDL/${group}ready/${filename_csv}`, result, 'utf8');
                }
                catch (err) {
                    console.error(err);
                }
            })
            .on('done', () => {
                resolve(true);
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
