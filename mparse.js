const states = require('./modules/states');
const Promise = require('bluebird');
const request = require('requestretry');
const rp = require('request-promise');
const fs = require('fs');
const unzip = require('unzip');
const csv = require('csvtojson');
const rimraf = require('rimraf');
const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const Papa = require('papaparse');
const { dataset } = require('./modules/settings.js');
const zlib = require('zlib');

if (!process.argv[4]) {
  console.log('fatal error.  Run like: node --max_old_space_size=14192 mparse.js year seq a');
  console.log('where year: 2014, 2015, 2016');
  console.log('where seq: 001, 002, etc');
  console.log('where a: trbg, allgeo  (tracts and block groups or all other geographies');
  console.log('example: node mparse.js 2015 003 allgeo');
  process.exit();
}

const YEAR = process.argv[2];
const SEQ = process.argv[3];
const GRP = process.argv[4];


const data_cache = {};

readyWorkspace()
  .then(() => {
    return downloadDataFromACS();
  })
  .then(() => {
    console.log('downloading schemas and geoid information');
    return Promise.all([createSchemaFiles(), getGeoKey(), readDirectory()]);
  })
  .then((setup_information) => {
    const schemas = setup_information[0];
    const keyed_lookup = setup_information[1];
    const files = setup_information[2];
    console.log('parsing ACS data');
    return parseData(schemas, keyed_lookup, files);
  })
  .then(d => {
    console.log('combining ACS data & writing to S3');
    return combineData();
  })
  .then(d => {
    console.log(`saved: ${d.length} files.`);
  }).catch(err => {
    console.log(err);
  });




/****************/

function combineData() {

  let put_object_array = [];

  Object.keys(data_cache).forEach(attr => {
    Object.keys(data_cache[attr]).forEach(sumlev => {
      put_object_array.push(`${attr}/${sumlev}`);
    });
  });

  const write_files_total = put_object_array.length;
  console.log(`attempting to upload ${write_files_total} files.`);
  let running_count = 0;

  const mapped_promises = Promise.map(put_object_array, (obj) => {

    const split = obj.split('/');
    const attr = split[0];
    const sumlev = split[1];

    const key = `${attr}/${sumlev}.json`;
    const data = JSON.stringify(data_cache[attr][sumlev]);

    return new Promise((resolve, reject) => {

      zlib.gzip(data, function(error, result) {
        if (error) { return reject(error); }

        const params = { Bucket: `s3db-acs-${dataset[YEAR].text}`, Key: key, Body: result, ContentType: 'application/json', ContentEncoding: 'gzip' };
        s3.putObject(params, function(err, data) {

          running_count++;
          if (running_count % 10 === 0) {
            console.log(`processing: ${((running_count / write_files_total)*100).toFixed(2)} %`);
          }

          if (err) {
            console.log(err);
            return reject(err);
          }
          return resolve(key);
        });

      });

    });
  }, { concurrency: 10 });

  return Promise.all(mapped_promises);
}


function readDirectory() {
  return new Promise((resolve, reject) => {
    // for file in directory
    fs.readdir(`./CensusDL/stage`, (err, files) => {
      if (err) {
        reject(err);
      }

      resolve(files);
    });
  });
}

function parseData(schemas, keyed_lookup, files) {

  const parsed_files = Promise.map(files, (file) => {
    const e_or_m = file.slice(0, 1);

    return new Promise((resolve, reject) => {

      fs.readFile(`./CensusDL/stage/${file}`, 'utf8', (err, data) => {
        if (err) {
          return reject(err);
        }

        Papa.parse(data, {
          header: false,
          skipEmptyLines: true,
          complete: function() {
            console.log(`parsed ${file}`);
            resolve('done');
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

            // combine with geo on stustab(2)+logrecno(5)
            const unique_key = results.data[0][2] + results.data[0][5];
            const geo_record = keyed_lookup[unique_key];

            // only tracts, bg, county, place, state right now
            const sumlev = geo_record.slice(0, 3);

            const component = geo_record.slice(3, 5);
            if (sumlev !== '140' && sumlev !== '150' && sumlev !== '050' && sumlev !== '160' && sumlev !== '040') {
              return;
            }
            if (component !== '00') {
              return;
            }

            const geoid = geo_record.split('US')[1];

            let parsed_geoid = "";

            if (sumlev === '040') {
              parsed_geoid = geoid.slice(-2);
            }
            else if (sumlev === '050') {
              parsed_geoid = geoid.slice(-5);
            }
            else if (sumlev === '140') {
              parsed_geoid = geoid.slice(-11);
            }
            else if (sumlev === '150') {
              parsed_geoid = geoid.slice(-12);
            }
            else if (sumlev === '160') {
              parsed_geoid = geoid.slice(-7);
            }
            else {
              console.error('unknown geography');
              console.log(geoid);
              console.log(sumlev);
              process.exit();
            }

            results.data[0].forEach((d, i) => {

              if (i <= 5) {
                // index > 5 excludes: FILEID, FILETYPE, STUSAB, CHARITER, SEQUENCE, LOGRECNO
                return;
              }

              const attr = (e_or_m === 'm') ? seq_fields[i] + '_moe' : seq_fields[i];

              if (!data_cache[attr]) {
                data_cache[attr] = {};
              }

              if (!data_cache[attr][sumlev]) {
                data_cache[attr][sumlev] = {};
              }

              const num_key = (d === '' || d === '.') ? null : Number(d);

              // this is how the data will be modeled in S3
              data_cache[attr][sumlev][parsed_geoid] = num_key;

            });

          }

        });

      });

    });

  }, { concurrency: 1 });

  return Promise.all(parsed_files);

}



function getGeoKey() {

  // Load geoid lookup for all geographies in the dataset
  return rp({
    method: 'get',
    uri: `https://s3-us-west-2.amazonaws.com/s3db-acs-metadata-${dataset[YEAR].text}/g${dataset[YEAR].text}.json`,
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

          resolve(fields);
        })
        .on('done', () => {
          //parsing finished
          console.log('finished parsing schema file');
        });
    });
  });
}

function downloadDataFromACS() {
  const fileType = GRP === 'trbg' ? 'Tracts_Block_Groups_Only' : 'All_Geographies_Not_Tracts_Block_Groups';
  const outputDir = 'CensusDL/group/';

  const states_data_ready = Object.keys(states).map((state, index) => {
    const fileName = `${YEAR}5${state}0${SEQ}000.zip`;
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
      const directories_in_order = ['./CensusDL', './CensusDL/group', './CensusDL/stage', './CensusDL/output'];

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


// https://github.com/uxitten/polyfill/blob/master/string.polyfill.js
// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String/padStart
if (!String.prototype.padStart) {
  String.prototype.padStart = function padStart(targetLength, padString) {
    targetLength = targetLength >> 0; //truncate if number or convert non-number to 0;
    padString = String((typeof padString !== 'undefined' ? padString : ' '));
    if (this.length > targetLength) {
      return String(this);
    }
    else {
      targetLength = targetLength - this.length;
      if (targetLength > padString.length) {
        padString += padString.repeat(targetLength / padString.length); //append to original to ensure we are longer than needed
      }
      return padString.slice(0, targetLength) + String(this);
    }
  };
}
