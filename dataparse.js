'use strict';


const states = require('./modules/states');
const Promise = require('bluebird');
const rp = require('request-promise');
const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const Papa = require('papaparse');
const { dataset } = require('./modules/settings.js');
const zlib = require('zlib');


module.exports.parse = (event, context, callback) => {


  console.log('starting...');
  console.time("runTime");

  const YEAR = event.year;
  const SEQ = event.seq;
  const GRP = event.geo;
  const M_or_E = event.type;
  const ATTRIBUTES = event.attributes;

  console.log(YEAR);
  console.log(SEQ);
  console.log(GRP);
  console.log(M_or_E);
  console.log(ATTRIBUTES);

  const data_cache = {};


  console.log('downloading schemas and geoid information');
  Promise.all([getSchemaFiles(), getGeoKey(), downloadDataFromS3()])
    .then((setup_information) => {
      const schemas = setup_information[0];
      const keyed_lookup = setup_information[1];
      const s3_data = setup_information[2];
      console.log('parsing ACS data');
      return parseData(schemas, keyed_lookup, s3_data);
    })
    .then(d => {
      console.log(`uploading to S3...`);
      return uploadToS3();
    })
    .then(files => {
      console.log(`uploaded ${files.length} files to S3`);
      console.timeEnd("runTime");
      return callback(null, { message: 'Success!', status: 200, event });
    })
    .catch(err => {
      console.log(err);
      return callback(null, { message: 'There was an error!', status: 500, event, err });
    });




  /****************/


  function downloadDataFromS3() {

    const states_data_ready = Object.keys(states).map((state, index) => {

      return new Promise((resolve, reject) => {
        const params = {
          Bucket: `s3db-acs-raw-${dataset[YEAR].text}`,
          Key: `${M_or_E}${YEAR}5${state}0${SEQ}000_${GRP}.csv`
        };

        s3.getObject(params, function(err, data) {
          if (err) {
            console.log('error in S3 getObject');
            console.log(err, err.stack);
            return reject(err);
          }
          else {
            zlib.gunzip(data.Body, function(err, dezipped) {
              if (err) {
                console.log('error in gunzip');
                console.log(err);
              }
              return resolve(dezipped.toString());
            });

          }
        });

      });
    });

    return Promise.all(states_data_ready);
  }


  function getSchemaFiles() {

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



  function parseData(schemas, keyed_lookup, s3data, keys) {

    // loop through all state files
    const parsed_files = s3data.map((data, index) => {

      return new Promise((resolve, reject) => {

        const state = Object.keys(states)[index];

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

            const seq_fields = schemas[SEQ];

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

              if (!ATTRIBUTES.includes(seq_fields[i])) {
                return;
              }

              const attr = (M_or_E === 'm') ? seq_fields[i] + '_moe' : seq_fields[i];

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
            console.log(`parsed ${state}`);
            return resolve(state);
          }

        });



      });

    });

    return Promise.all(parsed_files);

  }


  function uploadToS3() {
    let put_object_array = [];

    Object.keys(data_cache).forEach(attr => {
      Object.keys(data_cache[attr]).forEach(sumlev => {

        const key = `${attr}/${sumlev}.json`;
        const data = JSON.stringify(data_cache[attr][sumlev]);

        const promise = new Promise((resolve, reject) => {


          zlib.gzip(data, function(error, result) {
            if (error) {
              return reject(error);
            }

            // `s3db-acs-${dataset[YEAR].text}`

            const params = { Bucket: 'acs-test-bucket', Key: key, Body: result, ContentType: 'application/json', ContentEncoding: 'gzip' };
            s3.putObject(params, function(err, data) {
              if (err) {
                console.log(err);
                return reject(err);
              }
              return resolve(key);
            });

          });


        });

        put_object_array.push(promise);

      });
    });

    return Promise.all(put_object_array);

  }



};
