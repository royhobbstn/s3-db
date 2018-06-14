const states = require('./modules/states');
const Promise = require('bluebird');
const request = require('request');
const unzipper = require('unzipper');
const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const { dataset } = require('./modules/settings.js');
const zlib = require('zlib');


unzipper.Open.url(request, 'https://www2.census.gov/programs-surveys/acs/summary_file/2014/data/1_year_seq_by_state/Alaska/20141ak0001000.zip')
  .then(function(d) {

    const filenames = [];
    const text_promises = [];

    d.files.forEach(function(file) {
      console.log(file.path);
      filenames.push(file.path);
      text_promises.push(file.buffer());
    });

    return Promise.all(text_promises)
      .then(d => {
        return d.map((text, index) => {
          console.log(typeof d);
          return {
            filename: filenames[index],
            text: text.toString()
          };
        });
      });


  })
  .then(function(d) {
    console.log(d);
  });


/*

zlib.gzip(JSON.stringify(reduced), function(error, result) {
  if (error) { return reject(error); }

  // console.log(`s3db-acs-${dataset[YEAR].text}`);
  const key = file_list[0].split('|')[0].replace('-', '/');
  // console.log(key);

  const params = { Bucket: `s3db-acs-raw-${dataset[YEAR].text}`, Key: `${key}.csv`, Body: result, ContentType: 'text/csv', ContentEncoding: 'gzip' };
  s3.putObject(params, function(err, data) {

    if (err) {
      console.log(err);
      return reject(err);
    }
    return resolve(key);
  });

});


function downloadDataFromACS() {
  const fileType = GRP === 'trbg' ? 'Tracts_Block_Groups_Only' : 'All_Geographies_Not_Tracts_Block_Groups';

  const states_data_ready = Object.keys(states).map((state, index) => {
    const fileName = `${YEAR}5${state}0${SEQ}000.zip`;
    const url = `https://www2.census.gov/programs-surveys/acs/summary_file/${YEAR}/data/5_year_seq_by_state/${states[state]}/${fileType}/${fileName}`;

    console.log(url);

    return new Promise((resolve, reject) => {
      request({ url, encoding: null }).pipe(unzipper.Extract({ path: `/tmp/CensusDL/stage` })
        .on('close', function() {
          console.log(`${fileName} unzipped!`);
        })
        .on('error', function(err) {
          console.log('download problem');
          console.log(err);
          reject(err);
        })
        .on('finish', function() {
          resolve('done unzip');
        })
      );


    });
  });

  return Promise.all(states_data_ready);
}


*/
