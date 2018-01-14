// https://www2.census.gov/programs-surveys/acs/summary_file/2016/data/5_year_entire_sf/


const request = require('requestretry');
const fs = require('fs');
const rimraf = require('rimraf');
const AWS = require('aws-sdk');
const tar = require('tar');
const { promisify } = require('util');
const writeFileAsync = promisify(fs.writeFile);


const dataset = {
    year: 2016,
    text: 'acs1216',
    seq_files: '122'
};


readyWorkspace()
    .then(() => {
        return Promise.all([requestAndSaveFileFromACS(true), requestAndSaveFileFromACS(false)]);
    })
    .then((result) => {
        console.log(result);
        console.log('renaming and moving files');
        process.exit();
        //return renameAndMoveFiles();
    })
    // .then(() => {
    //     console.log('creating schema files');
    //     return createSchemaFiles();
    // })
    // .then(() => {
    //     return parseGeofiles();
    // })
    // .then(() => {
    //     return loadDataToS3();
    // })
    // .then(() => {
    //     console.log('program complete');
    // })
    .catch(err => {
        console.log(err);
        process.exit(); // exit immediately upon error
    });


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




function requestAndSaveFileFromACS(isTractBGFile) {

    const fileType = isTractBGFile ? 'Tracts_Block_Groups_Only' : 'All_Geographies_Not_Tracts_Block_Groups';

    const outputDir = isTractBGFile ? 'CensusDL/group1/' : 'CensusDL/group2/';
    const stageDir = isTractBGFile ? 'stage1' : 'stage2';

    const fileUrl = `https://s3.amazonaws.com/acs-repository/${fileType}_${dataset.year}.tar.gz`;

    console.log(`downloading ${fileType}`);

    return request({ url: fileUrl, encoding: null, maxAttempts: 5, retryDelay: 5000 })
        .then((resp, body) => {
            return writeFileAsync(`${outputDir}${fileType}_${dataset.year}.tar.gz`, body);
        })
        .then((text) => {
            console.log(`${fileType} written!`);
            return tar.extract({
                file: `./${outputDir}${fileType}_${dataset.year}.tar.gz`,
                cwd: `./CensusDL/${stageDir}`
            });
        })
        .then(() => {
            return `${fileType} extracted to ./CensusDL/${stageDir}`;
        })
        .catch((err) => {
            console.log('ERROR:', err);
        });

}
