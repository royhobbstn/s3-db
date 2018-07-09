const argv = require('yargs').argv;
const Promise = require('bluebird');
const AWS = require('aws-sdk');
// **CREDENTIALS** const aws_credentials = require('./aws_key.json');
const rp = require('request-promise');
const { dataset } = require('./modules/settings.js');
const readline = require('readline');

AWS.config.update({
  region: 'us-west-2',
  maxRetries: 0,
  retryDelayOptions: {
    base: 10000
  }
});


if (argv._.length === 0) {
  console.log('fatal error.  Run like: node run-data.js 2014');
  process.exit();
}

setup();

const YEAR = argv._[0];
const config_obj = dataset[YEAR];

if (!config_obj) {
  console.log('unknown year.');
  process.exit();
}

const SEQ_COUNT = Number(config_obj.seq_files);
const BUCKET_NAME = `s3db-acs-${config_obj.text}`;

console.log({ YEAR, SEQ_COUNT, BUCKET_NAME });


getSchemaFiles()
  .then(schemas => {
    //
    const combinations = [];

    // all possible combinations
    for (let i = 1; i <= SEQ_COUNT; i++) {

      const seq = String(i).padStart(3, '0');

      const fields = schemas[seq];

      const filtered_fields = fields.filter(field => {
        return !['FILEID', 'FILETYPE', 'STUSAB', 'CHARITER', 'SEQUENCE', 'LOGRECNO'].includes(field);
      });

      filtered_fields.forEach(field => {
        ['040', '050', '140', '150', '160'].forEach(geo => {
          ['e', 'm'].forEach(type => {
            const ext = type === 'm' ? '_moe' : '';
            combinations.push({ year: YEAR, seq, field, geo, type, name: `${field}${ext}/${geo}` });
          });
        });
      });

    }

    console.log(combinations.length + ' total keys in dataset');

    const s3_bucket = `s3db-acs-${dataset[YEAR].text}`;

    console.log(`Reading keys from bucket: ${s3_bucket}`);

    // **CREDENTIALS if not on an Amazon Instance
    const listAll = require('s3-list-all')( /*{ accessKeyId: aws_credentials.accessKeyId, secretAccessKey: aws_credentials.secretAccessKey }*/ );

    listAll({ Bucket: s3_bucket, Prefix: '' }, function(err, results) {

      if (err) {
        console.log(err);
        process.exit();
      }

      console.log(`Found ${results.length} keys.`);

      const keys = {};

      results
        .map(d => d.Key.replace('.json', ''))
        .forEach(result => {
          keys[result] = true;
        });

      // filter out existing from possible
      const remaining = combinations.filter(opt => {
        return !(keys[opt.name]);
      });

      console.log(`Missing ${remaining.length} keys.`);

      // convert to list of lambdas to run
      const recreate = remaining.reduce((acc, obj) => {

        const geo_type = (obj.geo === '140' || obj.geo === '150') ? 'trbg' : 'allgeo';

        const key = `${obj.seq}|${geo_type}|${obj.type}`;
        if (!acc[key]) {
          acc[key] = [obj.field];
        }
        else {
          acc[key].push(obj.field);
        }
        return acc;
      }, {});

      const files_to_retrieve = [];

      Object.keys(recreate).forEach(key => {
        //
        const split = key.split('|');
        const seq = split[0];
        const geo = split[1];
        const type = split[2];

        // let lambda only parse up to 30 attributes at a time
        for (let i = 0; i < recreate[key].length; i += 30) {
          const attributes = recreate[key].slice(i, i + 30);
          files_to_retrieve.push({ year: YEAR, seq, geo, type, attributes });
        }
        //
      });

      console.log(`There are ${files_to_retrieve.length} files left to parse.`);



      const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
      });

      rl.question('Continue y/n ? ', (answer) => {
        rl.close();

        if (answer !== 'y') {
          process.exit();
        }

        const completed_lambdas = Promise.map(files_to_retrieve, (c) => {

          return new Promise((resolve, reject) => {

            let lambda = new AWS.Lambda({ apiVersion: '2015-03-31' });

            const params = {
              FunctionName: "s3-db-dev-dataparse",
              InvocationType: "Event",
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
                console.log(`status: ${data.StatusCode}, year: ${c.year}, seq: ${c.seq}, geo: ${c.geo}, type: ${c.type}, attributes: ${c.attributes.length}`);
                return resolve(data);
              }
            });

          });
        }, { concurrency: 1 });

        Promise.all(completed_lambdas).then(() => {
            console.log(`done processing ACS ${YEAR}!`);
          })
          .catch(err => {
            console.log(err);
          });

      });

    }); // end listAll

  }); // end getSchemaFiles


/*******************/


function getSchemaFiles() {

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
