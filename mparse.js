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
  .then(() => {
    console.log('downloading schemas, geoids, and cluster information');
    return Promise.all([createSchemaFiles(), getGeoKey(), getClusterInfo()]);
  })
  .then((setup_information) => {
    const schemas = setup_information[0];
    const keyed_lookup = setup_information[1];
    const clusters = setup_information[2];
    console.log('parsing ACS data');
    return parseData(schemas, keyed_lookup, clusters);
  })
  .then(() => {
    console.log('done');
  });




/****************/

function parseData(schemas, keyed_lookup, clusters) {

}



function getClusterInfo() {

  // Load cluster files for each geographic level;

  const promises = ['bg', 'tract', 'place', 'county', 'state'].map(geo => {
    return rp({
      method: 'get',
      uri: `https://s3-us-west-2.amazonaws.com/${dataset[YEAR].cluster_bucket}/clusters_${dataset[YEAR].year}_${geo}.json`,
      headers: {
        'Accept-Encoding': 'gzip',
      },
      gzip: true,
      json: true,
      fullResponse: false
    });
  });

  return Promise.all(promises)
    .then(data => {

      const arr = data.map(d => {
        return d[dataset[YEAR].clusters];
      });

      // parse into one master object with all geoids
      return Object.assign({}, ...arr);

    });

}


function getGeoKey() {

  return rp({
    method: 'get',
    uri: `https://s3-us-west-2.amazonaws.com/s3db-acs-${dataset[YEAR].text}/g${YEAR}.json`,
    json: true,
    fullResponse: false
  });

}



function createSchemaFiles() {
  return new Promise((resolve, reject) => {

    const url = `https://www2.census.gov/programs-surveys/acs/summary_file/${YEAR}/documentation/user_tools/ACS_5yr_Seq_Table_Number_Lookup.txt`;
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
