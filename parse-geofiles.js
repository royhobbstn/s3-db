// prep work before getting all census data

const Papa = require('papaparse');
const states = require('./modules/states');
const { dataset, geography_file_headers } = require('./modules/settings.js');
const argv = require('yargs').argv;
const rp = require('request-promise');
const AWS = require('aws-sdk');
const s3 = new AWS.S3();

if (argv._.length === 0) {
  console.log('fatal error.  Run like: node parse-geofiles.js 2015');
  process.exit();
}

const YEAR = argv._[0];


const all_states_parsed = Object.keys(states).map(state => {

  return rp({
    method: 'get',
    uri: `https://www2.census.gov/programs-surveys/acs/summary_file/${YEAR}/data/5_year_seq_by_state/${states[state]}/Tracts_Block_Groups_Only/g${YEAR}5${state}.csv`,
    fullResponse: false
  }).then(data => {
    return new Promise((resolve, reject) => {
      Papa.parse(data, {
        header: false,
        delimiter: ',',
        skipEmptyLines: true,
        complete: function(results) {
          console.log(`done: ${states[state]}`);
          resolve(convertGeofile(results.data));
        },
        error: function(error, file) {
          console.log("error:", error, file);
          reject('nope');
        }
      });
    });
  }).catch(err => {
    console.log(err);
  });

});


Promise.all(all_states_parsed).then(datas => {
  const parsed_dataset = JSON.stringify(Object.assign({}, ...datas));
  return putObject(`g${dataset[YEAR].text}.json`, parsed_dataset);
}).then(() => {
  console.log('done');
}).catch(error => {
  console.log('failed creating geofile.');
  console.log(error);
});

/*****************/


// combine cluster data into a single file;
// const promises = ['bg', 'tract', 'place', 'county', 'state'].map(geo => {
//   return rp({
//     method: 'get',
//     uri: `https://s3-us-west-2.amazonaws.com/${dataset[YEAR].cluster_bucket}/clusters_${dataset[YEAR].year}_${geo}.json`,
//     headers: {
//       'Accept-Encoding': 'gzip',
//     },
//     gzip: true,
//     json: true,
//     fullResponse: false
//   });
// });

// Promise.all(promises)
//   .then(data => {

//     const arr = data.map(d => {
//       return d[dataset[YEAR].clusters];
//     });

//     // parse into one master object with all geoids
//     const combined_clusters = Object.assign({}, ...arr);

//     putObject(`c${YEAR}.json`, JSON.stringify(combined_clusters))
//       .then(() => {
//         console.log('clusters written to metadata bucket');
//       });

//   });


/*****************/

function putObject(key, value) {
  const myBucket = `s3db-acs-metadata-${dataset[YEAR].text}`;

  return new Promise((resolve, reject) => {
    const params = { Bucket: myBucket, Key: key, Body: value, ContentType: 'application/json' };
    s3.putObject(params, function(err, data) {
      if (err) {
        console.log(err);
        return reject(err);
      }
      else {
        console.log(`Successfully uploaded data to ${key}`);
        return resolve(data);
      }
    });
  });

}


function convertGeofile(data) {
  // convert geofile to json (with field names).  convert json to key-value
  // join key is stusab(lowercase) + logrecno
  const keyed_lookup = {};

  data.forEach(d => {
    const obj = {};
    d.forEach((item, index) => {
      obj[geography_file_headers[index]] = item;
    });
    const stusab_lc = obj.STUSAB.toLowerCase();
    // GEOID layout is ${SUMLEVEL}${COMPONENT}US${GEOID} - that's all we need
    keyed_lookup[stusab_lc + obj.LOGRECNO] = obj.GEOID;
  });

  return keyed_lookup;
}
