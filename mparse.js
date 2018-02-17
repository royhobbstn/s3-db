// split the s3db load process into tiny self-contained pieces

// also, script the entire cloud setup / deployment


/*************/

const argv = require('yargs').argv;
const states = require('./modules/states');
const Promise = require('bluebird');
const request = require('requestretry');
const rp = require('request-promise');
const fs = require('fs');
const unzip = require('unzip');
const csv = require('csvtojson');
const rimraf = require('rimraf');
const AWS = require('aws-sdk');
const path = require('path');
const Papa = require('papaparse');
const dataset = require('./modules/settings.js').dataset;
const geography_file_headers = require('./modules/settings.js').geography_file_headers;


if (argv._.length === 0) {
  console.log('fatal error.  Run like: node mparse.js 2015');
  process.exit();
}

const YEAR = argv._[0];



readyWorkspace()
  .then(() => {
    return downloadDataFromACS();
  })
  .then(ready => {
    console.log('done');
  });




/****************/

// for reference:  data is here:
// https://www2.census.gov/programs-surveys/acs/summary_file/2015/data/5_year_seq_by_state/Alabama/All_Geographies_Not_Tracts_Block_Groups/

function downloadDataFromACS() {
  const isTractBGFile = true;
  const fileType = isTractBGFile ? 'Tracts_Block_Groups_Only' : 'All_Geographies_Not_Tracts_Block_Groups';
  const outputDir = 'CensusDL/group/';
  const seq_num = '001';
  // todo ? 1 year files?

  const states_data_ready = Object.keys(states).slice(0, 2).map((state, index) => {
    const fileName = `${YEAR}5${state}0${seq_num}000.zip`;
    const url = `https://www2.census.gov/programs-surveys/acs/summary_file/${YEAR}/data/5_year_seq_by_state/${states[state]}/${fileType}/${fileName}`;

    return new Promise((resolve, reject) => {
      request({ url, encoding: null }, function(err, resp, body) {
        if (err) { return reject(err); }
        fs.writeFile(`${outputDir}${fileName}`, body, function(err) {
          if (err) { return reject(err); }
          console.log(`${fileName} written!`);

          // unzip
          const stream = fs.createReadStream(`${outputDir}${fileName}`);
          stream.pipe(unzip.Extract({ path: `CensusDL/stage` })
            .on('close', function() {
              console.log(`${fileName} unzipped!`);
              resolve('done unzip');
            })
            .on('error', function(err) {
              reject(err);
            })
          );
        });
      });

    });
  });

  return Promise.all(states_data_ready);
}


function readyWorkspace() {
  return new Promise((resolve, reject) => {
    // delete ./CensusDL if exists
    rimraf('./CensusDL', function(err) {
      if (err) {
        return reject(err);
      }

      // logic to set up directories
      const directories_in_order = ['./CensusDL', './CensusDL/group', './CensusDL/stage',
        './CensusDL/ready', './CensusDL/geofile', './CensusDL/output'
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
