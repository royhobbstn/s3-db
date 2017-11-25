#!/usr/bin/env node

// geography all merged together, saved as single CSV file
// geography loaded / immediately converted to key-val lookup on Lambda or Elasticache

// csv data files combined by seq/moe parsed to json and immediately uploaded to s3

// retrieval will go from tile geo -> geoLambda (single) -> dataLambda (multiple)


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

readyWorkspace()
    .then(() => {
        return Promise.map(selected_states, state => {
            return requestAndSaveStateFileFromACS(state.state, state.isTractBGFile);
        }, { concurrency: 5 });
    })
    .then(() => {
        console.log('merging estimate files');
        return mergeFiles('estimate');
    })
    .then(() => {
        console.log('merging moe files');
        return mergeFiles('moe');
    })
    .then(() => {
        console.log('all file merging completed');
        return createSchemaFiles();
    })
    .then(schemas => {
        return loadDataToS3(schemas);
    })
    .then(() => {
        console.log('program complete');
    })
    .catch(err => {
        console.log(err);
        process.exit(); // exit immediately upon error
    });


/**************************/


function loadDataToS3(schemas) {
    // TODO convert to JSON and write to S3

}


function mergeFiles(file_type) {
    const typechar = (file_type === 'moe') ? 'm' : 'e';

    return new Promise((resolve, reject) => {
        const command = `for i in $(seq -f "%03g" 1 122); do awk 'FNR==1 && NR!=1{next;}{print}' ./CensusDL/group1/_unzipped/${typechar}20155**0"$i"000.txt ./CensusDL/group2/_unzipped/${typechar}20155**0"$i"000.txt > ./CensusDL/ready/${typechar}seq"$i".csv; done;`;
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
