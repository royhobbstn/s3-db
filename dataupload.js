const states = require('./modules/states');
const Promise = require('bluebird');
const request = require('request');
const unzipper = require('unzipper');
const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const { dataset } = require('./modules/settings.js');
const zlib = require('zlib');


module.exports.upload = (event, context, callback) => {


  const YEAR = event.year;
  const SEQ = event.seq;
  const STATE = event.state;
  const GEO = event.geo;

  const geo_alias = GEO === 'allgeo' ? 'All_Geographies_Not_Tracts_Block_Groups' : GEO === 'trbg' ? 'Tracts_Block_Groups_Only' : null;


  if (geo_alias === null) {
    console.log("Please specify 'Tracts_Block_Groups_Only', 'All_Geographies_Not_Tracts_Block_Groups' for the 'geo' parameter.");
    process.exit();
  }

  console.log(`starting...  ${YEAR} ${SEQ} ${STATE} ${GEO}`);
  console.time("runTime");

  const fileName = `${YEAR}5${STATE}0${SEQ}000.zip`;

  const url = `https://www2.census.gov/programs-surveys/acs/summary_file/${YEAR}/data/5_year_seq_by_state/${states[STATE]}/${geo_alias}/${fileName}`;

  const parsed_files = new Promise((map_resolve, map_reject) => {

    unzipper.Open.url(request, url)
      .then(function(d) {

        const filenames = [];
        const text_promises = [];

        d.files.forEach(function(file) {
          filenames.push(file.path.replace('.txt', `_${GEO}.csv`));
          text_promises.push(file.buffer());
        });

        return Promise.all(text_promises)
          .then(d => {
            return d.map((text, index) => {
              return {
                filename: filenames[index],
                text: text.toString()
              };
            });
          });

      })
      .then(function(data) {

        const written = data.map(d => {

          return new Promise((resolve, reject) => {

            zlib.gzip(d.text, function(error, result) {

              if (error) { return reject(error); }
              const key = d.filename;
              const params = { Bucket: `s3db-acs-raw-${dataset[YEAR].text}`, Key: key, Body: result, ContentType: 'text/csv', ContentEncoding: 'gzip' };

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
        return Promise.all(written);
      })
      .then(data => {
        map_resolve(data);
      })
      .catch(err => {
        map_reject(err);
      });

  });


  parsed_files
    .then(d => {
      console.timeEnd("runTime");
      console.log(`Completed! ${YEAR} ${SEQ} ${STATE} ${GEO}`);
      return callback(null, { message: `Completed! ${YEAR} ${SEQ} ${STATE} ${GEO}`, status: 200, event });
    })
    .catch(error => {
      console.timeEnd("runTime");
      console.log(`${YEAR} ${SEQ} ${STATE} ${GEO} Failed:`);
      console.log(error);
      return callback(null, { message: `${YEAR} ${SEQ} ${STATE} ${GEO} Failed:`, status: 500, event, error });
    });

};
