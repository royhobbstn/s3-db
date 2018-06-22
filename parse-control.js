const argv = require('yargs').argv;
const Promise = require('bluebird');
const AWS = require('aws-sdk');
const rp = require('request-promise');
const { dataset } = require('./modules/settings.js');

/****/

AWS.config.update({
  region: 'us-west-2',
  maxRetries: 0,
  retryDelayOptions: {
    base: 10000
  }
});

/****/

setup();


// TODO below using dataset module

const CONFIG = {
  2014: {
    seq: 121,
    bucket: 's3db-acs-1014'
  },
  2015: {
    seq: 122,
    bucket: 's3db-acs-1115'
  },
  2016: {
    seq: 122,
    bucket: 's3db-acs-1216'
  }
};

if (argv._.length === 0) {
  console.log('fatal error.  Run like: node run-data.js 2014');
  process.exit();
}

const YEAR = argv._[0];
const config_obj = CONFIG[YEAR];

if (!config_obj) {
  console.log('unknown year.');
  process.exit();
}

const SEQ_COUNT = config_obj.seq;
const BUCKET_NAME = config_obj.bucket;

getSchemaFiles().then(schemas => {
    //

    const combinations = [];

    // TODO remove after testing
    const SEQ_COUNT = 104;

    // all possible combinations
    for (let i = 104; i <= SEQ_COUNT; i++) {
      combinations.push({ year: YEAR, seq: String(i).padStart(3, '0'), geo: 'allgeo', type: 'e' });
      combinations.push({ year: YEAR, seq: String(i).padStart(3, '0'), geo: 'trbg', type: 'e' });
      combinations.push({ year: YEAR, seq: String(i).padStart(3, '0'), geo: 'allgeo', type: 'm' });
      combinations.push({ year: YEAR, seq: String(i).padStart(3, '0'), geo: 'trbg', type: 'm' });
    }

    const field_combinations = [];

    combinations.forEach(d => {

      const fields = schemas[d.seq];

      const filtered_fields = fields.filter(field => {
        return !['FILEID', 'FILETYPE', 'STUSAB', 'CHARITER', 'SEQUENCE', 'LOGRECNO'].includes(field);
      });

      for (let i = 0; i < filtered_fields.length; i += 30) {
        const attributes = filtered_fields.slice(i, i + 30);
        field_combinations.push(Object.assign({}, d, { attributes }));
      }

    });


    const completed_lambdas = Promise.map(field_combinations, (c) => {

      console.log(c);

      return new Promise((resolve, reject) => {

        let lambda = new AWS.Lambda({ apiVersion: '2015-03-31' });

        const params = {
          FunctionName: "s3-db-dev-dataparse",
          InvocationType: "RequestResponse",
          LogType: "None",
          Payload: JSON.stringify({
            'year': c.year,
            'seq': c.seq,
            'geo': c.geo,
            'type': c.type,
            'attributes': c.attributes
          })
        };
        lambda.invoke(params, function(err, data) {
          if (err) {
            console.log(err, err.stack);
            return reject(err);
          }
          else {
            console.log(data);
            return resolve(data);
          }
        });

      });
    }, { concurrency: 1 });

    return Promise.all(completed_lambdas);


  }).then(() => {
    console.log(`done processing ACS ${YEAR}!`);
  })
  .catch(err => {
    console.log(err);
  });





/*******************/


function getSchemaFiles() {

  // TODO use AWS JS SDK
  // Load schema for the dataset
  return rp({
    method: 'get',
    uri: `https://s3-us-west-2.amazonaws.com/s3db-acs-metadata-${dataset[YEAR].text}/s${dataset[YEAR].text}.json`,
    json: true,
    fullResponse: false
  });

}

function setup() {

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

}
