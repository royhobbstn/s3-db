const argv = require('yargs').argv;
const Promise = require('bluebird');
const { url } = require('./secret-url.js');
const request = require('requestretry');
const rp = require('request-promise');

setup();

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

const combinations = [];

// all possible combinations
for (let i = 1; i <= SEQ_COUNT; i++) {
  combinations.push({ year: YEAR, seq: String(i).padStart(3, '0'), geo: 'allgeo', type: 'e' });
  combinations.push({ year: YEAR, seq: String(i).padStart(3, '0'), geo: 'trbg', type: 'e' });
  combinations.push({ year: YEAR, seq: String(i).padStart(3, '0'), geo: 'allgeo', type: 'm' });
  combinations.push({ year: YEAR, seq: String(i).padStart(3, '0'), geo: 'trbg', type: 'm' });
}

const completed_lambdas = Promise.map(combinations.slice(0, 3), (lambda) => {

  const uri = `${url}?year=${lambda.year}&seq=${lambda.seq}&geo=${lambda.geo}&type=${lambda.type}`;

  console.log(uri);

  return rp({
    method: 'get',
    uri,
    fullResponse: false
  });

}, { concurrency: 1 });

Promise.all(completed_lambdas)
  .then(() => {
    console.log(`done processing ACS ${YEAR}!`);
  }).catch(err => {
    console.log(err);
  });



/*******************/


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
