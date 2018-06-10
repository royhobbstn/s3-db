const states = require('./modules/states');
const Promise = require('bluebird');
const request = require('requestretry');
const rp = require('request-promise');
const fs = require('fs');
const unzip = require('unzipper');
const rimraf = require('rimraf');
const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const Papa = require('papaparse');
const { dataset } = require('./modules/settings.js');
const zlib = require('zlib');

console.log('starting...');
console.time("runTime");

if (!process.argv[5]) {
  console.log('fatal error.  Run like: node --max_old_space_size=14192 mparse.js year seq a me');
  console.log('where year: 2014, 2015, 2016');
  console.log('where seq: 001, 002, etc');
  console.log('where a: trbg, allgeo  (tracts and block groups or all other geographies');
  console.log('where me: m, e (margin of error or estimate)');
  console.log('example: node mparse.js 2015 003 allgeo');
  process.exit();
}

const YEAR = process.argv[2];
const SEQ = process.argv[3];
const GRP = process.argv[4];
const M_or_E = process.argv[5];



readyWorkspace()
  .then(() => {
    return downloadDataFromACS();
  })
  .then(() => {
    // delete moe or est files
    return deleteUnused();
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
    console.log(`saved: ${d.length} files.`);
    return uniqueFiles();
  })
  .then(filesystem => {
    console.log(`found ${filesystem.uniques.length} unique keys to write to S3.`);
    console.log('combining ACS data & writing to S3');
    return combineData(filesystem);
  })
  .then(() => {
    console.log('all done');
    console.timeEnd("runTime");
  })
  .catch(err => {
    console.log(err);
  });




/****************/


function readyWorkspace() {
  return new Promise((resolve, reject) => {
    // delete ./CensusDL if exists
    rimraf('./CensusDL', function(err) {
      if (err) {
        return reject(err);
      }

      // logic to set up directories
      const directories_in_order = ['./CensusDL', './CensusDL/stage', './CensusDL/output'];

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

function downloadDataFromACS() {
  const fileType = GRP === 'trbg' ? 'Tracts_Block_Groups_Only' : 'All_Geographies_Not_Tracts_Block_Groups';

  const states_data_ready = Object.keys(states).map((state, index) => {
    const fileName = `${YEAR}5${state}0${SEQ}000.zip`;
    const url = `https://www2.census.gov/programs-surveys/acs/summary_file/${YEAR}/data/5_year_seq_by_state/${states[state]}/${fileType}/${fileName}`;

    return new Promise((resolve, reject) => {
      request({ url, encoding: null }).pipe(unzip.Extract({ path: `CensusDL/stage` })
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

  return Promise.all(states_data_ready);
}


function deleteUnused() {
  //
  return new Promise((resolve, reject) => {
    // for file in directory
    fs.readdir(`./CensusDL/stage`, (err, files) => {
      if (err) {
        return reject(err);
      }

      const delete_these_files = files.filter(file => {
        return file.slice(0, 1) !== M_or_E;
      });

      const deleted_files = delete_these_files.map(file => {

        return new Promise((resolve, reject) => {
          fs.unlink(`./CensusDL/stage/${file}`, function(err) {
            if (err) {
              // won't reject on error
              console.log(err);
              return reject(false);
            }
            resolve(true);
          });
        });

      });

      Promise.all(deleted_files).then(() => {
        return resolve(true);
      });

    });
  });
}


function createSchemaFiles() {

  // Load schema for the dataset
  return rp({
    method: 'get',
    uri: `https://s3-us-west-2.amazonaws.com/s3db-acs-metadata-${dataset[YEAR].text}/s${dataset[YEAR].text}.json`,
    json: true,
    fullResponse: false
  });

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

  // loop thrrough all state files
  const parsed_files = Promise.map(files, (file) => {
    const e_or_m = file.slice(0, 1);

    return new Promise((resolve, reject) => {

      fs.readFile(`./CensusDL/stage/${file}`, 'utf8', (err, data) => {
        if (err) {
          return reject(err);
        }

        const state = file.slice(6, 8);
        const data_cache = {};

        Papa.parse(data, {
          header: false,
          skipEmptyLines: true,
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

          },
          complete: function() {
            console.log(`parsed ${file}`);


            let put_object_array = [];

            Object.keys(data_cache).forEach(attr => {
              Object.keys(data_cache[attr]).forEach(sumlev => {
                put_object_array.push(`${attr}/${sumlev}`);
              });
            });

            const write_files_total = put_object_array.length;
            console.log(`saving ${write_files_total} intermediate files.`);

            const mapped_promises = Promise.map(put_object_array, (obj) => {

              const split = obj.split('/');
              const attr = split[0];
              const sumlev = split[1];

              const key = `${attr}-${sumlev}|${state}.json`;
              const data = JSON.stringify(data_cache[attr][sumlev]);

              return new Promise((resolve, reject) => {

                fs.writeFile(`./CensusDL/output/${key}`, data, 'utf8', (err) => {

                  if (err) {
                    console.log(err);
                    return reject(err);
                  }
                  return resolve(key);

                });

              });

            }, { concurrency: 10 });

            resolve(mapped_promises);
          }

        });

      });

    });

  }, { concurrency: 1 });

  return Promise.all(parsed_files);

}


function uniqueFiles() {
  //
  return new Promise((resolve, reject) => {
    // for file in directory
    fs.readdir(`./CensusDL/output`, (err, files) => {
      if (err) {
        reject(err);
      }

      // get unique filenames to write to S3 (basically get all file names and create unique set ignoring state)
      const uniques = Array.from(new Set(files.reduce((acc, current) => {
        acc.push(current.split('|')[0]);
        return acc;
      }, [])));

      console.log('uniques: ' + uniques.length);

      resolve({ uniques, files });
    });
  });
}


function combineData({ uniques, files }) {
  //

  const file_list_array = uniques.map(prefix => {
    // for each unique get a list of all files that match that pattern
    return files.filter(file => {
      return file.split('|')[0] === prefix;
    });
  });

  // each of these will be one saved S3 file
  const files_saved = Promise.map(file_list_array, (file_list) => {

    const file_data = file_list.map(file => {

      return new Promise((resolve, reject) => {
        fs.readFile(`./CensusDL/output/${file}`, (err, data) => {
          if (err) {
            console.log(err);
            return reject(err);
          }
          return resolve(JSON.parse(data));
        });
      });

    });

    return Promise.all(file_data).then(data => {

      const reduced = data.reduce((acc, current) => {
        return { ...acc, ...current };
      });

      return new Promise((resolve, reject) => {

        zlib.gzip(JSON.stringify(reduced), function(error, result) {
          if (error) { return reject(error); }

          // console.log(`s3db-acs-${dataset[YEAR].text}`);
          const key = file_list[0].split('|')[0].replace('-', '/');
          console.log(key);

          const params = { Bucket: `s3db-acs-${dataset[YEAR].text}`, Key: `${key}.json`, Body: result, ContentType: 'application/json', ContentEncoding: 'gzip' };
          s3.putObject(params, function(err, data) {

            if (err) {
              console.log(err);
              return reject(err);
            }
            return resolve(key);
          });

        });

      });
    });

  }, { concurrency: 10 });

  return Promise.all(files_saved);
}
